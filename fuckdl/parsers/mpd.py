import xmltodict
import asyncio
import base64
import json
import math
import os
import re
import urllib.parse
import uuid
from copy import copy
from hashlib import md5

import requests
from langcodes import Language
from langcodes.tag_parser import LanguageTagError

from fuckdl import config
from fuckdl.objects import AudioTrack, TextTrack, Track, Tracks, VideoTrack
from fuckdl.utils import Cdm
from fuckdl.utils.io import aria2c
from fuckdl.utils.xml import load_xml
from fuckdl.vendor.pymp4.parser import Box

# Import VIX parser
from fuckdl.parsers.mpd_vix import parse_vix


def _calculate_fps_from_timeline(rep, period, timescale_multiplier=1):
    """
    Calculate FPS from segment timeline if available.
    
    :param rep: Representation element
    :param period: Period element  
    :param timescale_multiplier: Multiplier for timescale (default 1)
    :return: FPS as float or None if cannot calculate
    """
    import logging
    import statistics
    log = logging.getLogger("MPD")
    
    # Find SegmentTemplate in representation or adaptation set
    segment_template = rep.find("SegmentTemplate")
    if segment_template is None:
        # Try parent adaptation set
        parent = rep.getparent()
        if parent is not None:
            segment_template = parent.find("SegmentTemplate")
    
    if segment_template is None:
        return None
    
    # Get timescale
    timescale = int(segment_template.get("timescale", 24000))
    timescale = timescale * timescale_multiplier
    
    # Get segment timeline
    segment_timeline = segment_template.find("SegmentTimeline")
    if segment_timeline is None:
        return None
    
    # Collect segment durations
    durations = []
    for s in segment_timeline.findall("S"):
        d = int(s.get("d", 0))
        if d > 0:
            # Handle repeat count
            repeat = int(s.get("r", 0))
            if repeat > 0:
                durations.extend([d] * (repeat + 1))
            else:
                durations.append(d)
    
    if not durations:
        return None
    
    # Check if VFR (variable frame rate)
    if len(durations) > 1:
        try:
            variance = statistics.variance(durations) if len(durations) > 1 else 0
            if variance > 100:  # High variance indicates VFR
                log.debug(f" + Detected VFR content (variance={variance:.2f})")
                # Use median for VFR
                avg_duration_ticks = statistics.median(durations)
            else:
                avg_duration_ticks = sum(durations) / len(durations)
        except (statistics.StatisticsError, TypeError):
            avg_duration_ticks = sum(durations) / len(durations)
    else:
        avg_duration_ticks = durations[0]
    
    avg_duration_sec = avg_duration_ticks / timescale
    
    if avg_duration_sec <= 0:
        return None
    
    # Calculate FPS (frames per second = 1 / duration per frame)
    fps = 1.0 / avg_duration_sec
    
    # Round to common FPS values for cleaner output
    for common_fps in [23.976, 24.0, 25.0, 29.97, 30.0, 50.0, 59.94, 60.0]:
        if abs(fps - common_fps) < 0.01:
            fps = common_fps
            break
    
    log.debug(f" + Calculated FPS {fps:.3f} from {len(durations)} segments (timescale={timescale}, avg_duration_ticks={avg_duration_ticks:.2f})")
    return round(fps, 3)


def parse(*, url=None, data=None, source, session=None, downloader=None, multi_period=False):
    """
    Convert an MPEG-DASH MPD document to a Tracks object.
    
    :param url: URL of the MPD document.
    :param data: The MPD document as a string.
    :param source: Source tag for the returned tracks.
    :param session: Used for any remote calls, e.g. getting the MPD document from an URL.
    :param downloader: Downloader to use. Accepted values are None (use requests to download) and aria2c.
    :param multi_period: If True, enable multi-period manifest handling.
    """
    import logging
    log = logging.getLogger("MPD")
    
    # ===== VIX SPECIAL HANDLING =====
    if source == "VIX":
        log.info(" + Using VIX specialized parser")
        return parse_vix(url=url, data=data, source=source, session=session, downloader=downloader)
    # ===== END VIX SPECIAL =====
    
    if not data:
        if not url:
            raise ValueError("Neither a URL nor a document was provided to Tracks.from_mpd")
        
        if downloader is None:
            data = (session or requests).get(url).text
        elif downloader == "aria2c":
            out = os.path.join(config.directories.temp, url.split("/")[-1])
            asyncio.run(aria2c(url, out))

            with open(out, encoding="utf-8") as fd:
                data = fd.read()

            try:
                os.unlink(out)
            except FileNotFoundError:
                pass
        else:
            raise ValueError(f"Unsupported downloader: {downloader}")

    # Parse the document
    import xml.etree.ElementTree as ET
    root = load_xml(data)
    if root.tag != "MPD":
        raise ValueError("Non-MPD document provided to Tracks.from_mpd")

    # Check if multi-period is requested via flag
    import click
    try:
        ctx = click.get_current_context()
        if hasattr(ctx, 'params') and ctx.params.get('multi_period', False):
            multi_period = True
            log.info(f" + Multi-period mode ENABLED via --multi-period flag")
    except (RuntimeError, AttributeError):
        pass
    
    # Multi-period mode only if explicitly requested via flag
    if multi_period:
        log.info(f" + Using multi-period parser for {source}")
        return _parse_mpd(root, url, source, session)
    else:
        log.debug(f" + Using single-period parser for {source}")
        return _parse_mpd(root, url, source, session)


def _parse_mpd(root, url, source, session):
    """Parse MPD document with multi-period support and all existing fixes."""
    import logging
    import xml.etree.ElementTree as ET
    log = logging.getLogger("MPD")
    
    namespace_match = re.match(r'\{([^}]+)\}', root.tag)
    namespace_uri = namespace_match.group(1) if namespace_match else "urn:mpeg:dash:schema:mpd:2011"
    
    if namespace_match:
        periods = root.findall(f".//{{{namespace_uri}}}Period")
    else:
        periods = root.findall(".//Period")
    
    # Check if multi-period or single period
    is_multi_period = len(periods) > 1
    period_tracks_list = []
    
    # Get base URL from root if available
    root_base_url = root.findtext("BaseURL")
    
    # Determine which periods to process
    periods_to_process = periods if is_multi_period else [root]
    
    for period_idx, period_elem in enumerate(periods_to_process):
        if is_multi_period:
            log.debug(f" + Processing period {period_idx+1}/{len(periods)}")
            
            # Create virtual MPD for this period
            period_str = ET.tostring(period_elem, encoding='unicode', method='xml')
            virtual_mpd = f'''<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="{namespace_uri}"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xmlns:cenc="urn:mpeg:cenc:2013"
     xmlns:mspr="urn:microsoft:playready"
     profiles="urn:mpeg:dash:profile:isoff-live:2011"
     type="static"
     minBufferTime="PT2S"
     mediaPresentationDuration="{root.get('mediaPresentationDuration', 'PT0S')}">
  {period_str}
</MPD>'''
            
            virtual_mpd = virtual_mpd.replace('ns0:default_KID', 'cenc:default_KID')
            virtual_mpd = virtual_mpd.replace('ns0:pssh', 'cenc:pssh')
            virtual_mpd = virtual_mpd.replace('ns0:pro', 'mspr:pro')
            virtual_mpd = virtual_mpd.replace('xmlns:ns0', 'xmlns:cenc')
            virtual_mpd = re.sub(r'<!--.*?-->', '', virtual_mpd, flags=re.DOTALL)
            
            try:
                period_root = load_xml(virtual_mpd)
            except Exception as e:
                log.warning(f" + Failed to parse period {period_idx+1}: {e}")
                # Fallback: create simpler MPD
                new_root = ET.Element('MPD', attrib={
                    'xmlns': namespace_uri,
                    'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
                    'xmlns:cenc': 'urn:mpeg:cenc:2013',
                    'xmlns:mspr': 'urn:microsoft:playready',
                    'profiles': 'urn:mpeg:dash:profile:isoff-live:2011',
                    'type': 'static',
                    'minBufferTime': 'PT2S',
                    'mediaPresentationDuration': root.get('mediaPresentationDuration', 'PT0S')
                })
                new_root.append(period_elem)
                temp_mpd = ET.tostring(new_root, encoding='unicode', method='xml')
                temp_mpd = temp_mpd.replace('ns0:default_KID', 'cenc:default_KID')
                temp_mpd = temp_mpd.replace('ns0:pssh', 'cenc:pssh')
                temp_mpd = temp_mpd.replace('ns0:pro', 'mspr:pro')
                temp_mpd = temp_mpd.replace('xmlns:ns0', 'xmlns:cenc')
                period_root = load_xml(temp_mpd)
        else:
            # Single period - use original root
            period_root = root
            period_elem = root
        
        # Parse tracks for this period
        tracks = []
        
        for period in period_root.findall("Period") if not is_multi_period else [period_root.find(".//Period") if period_root.find(".//Period") is not None else period_root]:
            # Handle case where period might be None
            if period is None:
                period = period_root if period_root.tag == "Period" else period_root
            
            if source == "HULU" and next(iter(period.xpath("SegmentType/@value")), "content") != "content":
                continue

            period_base_url = period.findtext("BaseURL") or root_base_url
            if url and (not period_base_url or not re.match("^https?://", period_base_url.lower())):
                period_base_url = urllib.parse.urljoin(url, period_base_url) if period_base_url else url
                period_base_url = period_base_url.replace('fly.eu.prd.media.max.com', 'akm.eu.prd.media.max.com')
                period_base_url = period_base_url.replace('gcp.eu.prd.media.max.com', 'akm.eu.prd.media.max.com')
                period_base_url = period_base_url.replace('fly.latam.prd.media.max.com', 'akm.latam.prd.media.max.com')
                period_base_url = period_base_url.replace('gcp.latam.prd.media.max.com', 'akm.latam.prd.media.max.com')

            for adaptation_set in period.findall("AdaptationSet"):
                if any(x.get("schemeIdUri") == "http://dashif.org/guidelines/trickmode"
                       for x in adaptation_set.findall("EssentialProperty")
                       + adaptation_set.findall("SupplementalProperty")):
                    continue

                for rep in adaptation_set.findall("Representation"):
                    try:
                        content_type = next(x for x in [
                            rep.get("contentType"),
                            rep.get("mimeType"),
                            adaptation_set.get("contentType"),
                            adaptation_set.get("mimeType")
                        ] if bool(x))
                    except StopIteration:
                        raise ValueError("No content type value could be found")
                    else:
                        content_type = content_type.split("/")[0]
                    if content_type.startswith("image"):
                        continue
                    
                    codecs = rep.get("codecs") or adaptation_set.get("codecs")
                    supplementalcodecs = rep.get("{urn:scte:dash:scte214-extensions}supplementalCodecs") or adaptation_set.get("{urn:scte:dash:scte214-extensions}supplementalCodecs")
                    if content_type in ("text", "application"):
                        mime = adaptation_set.get("mimeType")
                        if mime and not mime.endswith("/mp4"):
                            codecs = mime.split("/")[1]
                    
                    track_lang = None
                    for lang in [rep.get("lang"), adaptation_set.get("lang")]:
                        lang = (lang or "").strip()
                        if not lang:
                            continue
                        try:
                            t = Language.get(lang.split("-")[0])
                            if t == Language.get("und") or not t.is_valid():
                                raise LanguageTagError()
                        except LanguageTagError:
                            continue
                        else:
                            track_lang = Language.get(lang)
                            break

                    protections = rep.findall("ContentProtection") + adaptation_set.findall("ContentProtection")
                    encrypted = bool(protections)
                    pssh = None
                    pr_pssh = None
                    
                    # ===== UNIFIED KID EXTRACTION - FIX FOR HBOMax/UNVP =====
                    kid = None
                    
                    for protection in adaptation_set.findall("ContentProtection"):
                        if protection.get("schemeIdUri") == "urn:mpeg:dash:mp4protection:2011":
                            kid_val = protection.get("{urn:mpeg:cenc:2013}default_KID")
                            if kid_val:
                                kid = uuid.UUID(kid_val).hex.lower()
                                log.debug(f" + KID from AdaptationSet {adaptation_set.get('id', '?')} for {content_type}: {kid}")
                                break
                    
                    if not kid:
                        for protection in rep.findall("ContentProtection"):
                            if protection.get("schemeIdUri") == "urn:mpeg:dash:mp4protection:2011":
                                kid_val = protection.get("{urn:mpeg:cenc:2013}default_KID")
                                if kid_val:
                                    kid = uuid.UUID(kid_val).hex.lower()
                                    log.debug(f" + KID from Representation for {content_type}: {kid}")
                                    break
                    
                    if not kid and content_type == "audio":
                        period_parent = adaptation_set.getparent()
                        if period_parent is not None:
                            for adap in period_parent.findall("AdaptationSet"):
                                if adap.get("contentType") == "video":
                                    for protection in adap.findall("ContentProtection"):
                                        if protection.get("schemeIdUri") == "urn:mpeg:dash:mp4protection:2011":
                                            kid_val = protection.get("{urn:mpeg:cenc:2013}default_KID")
                                            if kid_val:
                                                kid = uuid.UUID(kid_val).hex.lower()
                                                log.debug(f" + Audio inheriting KID from video: {kid}")
                                                break
                                    if kid:
                                        break
                    # ===== END UNIFIED KID EXTRACTION =====
                    
                    for protection in protections:
                        if "9a04f079-9840-4286-ab92-e65be0885f95" in protection.get("schemeIdUri", "").lower():
                            pr_pssh = protection.findtext("pro") if source in ["STAN", "RKTN", "CR"] else protection.findtext("pssh")
                        if (protection.get("schemeIdUri") or "").lower() != Cdm.urn:
                            continue
                        pssh = protection.findtext("pssh")
                        if pssh:
                            pssh = base64.b64decode(pssh)
                            try:
                                pssh = Box.parse(pssh)
                            except Exception:
                                pssh = Box.parse(Box.build(dict(
                                    type=b"pssh",
                                    version=0,
                                    flags=0,
                                    system_ID=Cdm.uuid,
                                    init_data=pssh
                                )))

                    rep_base_url = rep.findtext("BaseURL")
                    track_url = None
                    
                    if rep_base_url and (
                        (source in ["CR"] and content_type == "text")
                        or (source not in ["DSCP", "DSNY", "CR"])
                    ):
                        if not re.match("^https?://", rep_base_url.lower()):
                            rep_base_url = urllib.parse.urljoin(period_base_url, rep_base_url)
                        query = urllib.parse.urlparse(url).query
                        if query and not urllib.parse.urlparse(rep_base_url).query:
                            rep_base_url += "?" + query
                        track_url = rep_base_url

                    else:
                        segment_template = rep.find("SegmentTemplate")
                        if segment_template is None:
                            segment_template = adaptation_set.find("SegmentTemplate")
                        
                        if segment_template is None:
                            segment_list = rep.find("SegmentList")
                            if segment_list is None:
                                segment_list = adaptation_set.find("SegmentList")
                            
                            if segment_list is not None:
                                segment_urls = segment_list.findall("SegmentURL")
                                if segment_urls:
                                    initialization = segment_list.find("Initialization")
                                    track_url = []
                                    
                                    if initialization is not None and initialization.get("sourceURL"):
                                        init_url = initialization.get("sourceURL")
                                        if not re.match("^https?://", init_url.lower()):
                                            init_url = urllib.parse.urljoin(period_base_url, init_url)
                                        track_url.append(init_url)
                                    
                                    base_url = segment_list.findtext("BaseURL") or period_base_url
                                    for seg_url in segment_urls:
                                        media_url = seg_url.get("media")
                                        if media_url:
                                            if not re.match("^https?://", media_url.lower()):
                                                media_url = urllib.parse.urljoin(base_url, media_url)
                                            track_url.append(media_url)
                                    
                                    segment_template = None
                                else:
                                    raise ValueError("SegmentList has no SegmentURLs")
                            else:
                                rep_base_url = rep.findtext("BaseURL")
                                if rep_base_url:
                                    if not re.match("^https?://", rep_base_url.lower()):
                                        rep_base_url = urllib.parse.urljoin(period_base_url, rep_base_url)
                                    query = urllib.parse.urlparse(url).query
                                    if query and not urllib.parse.urlparse(rep_base_url).query:
                                        rep_base_url += "?" + query
                                    track_url = rep_base_url
                                    segment_template = None
                                else:
                                    raise ValueError("Couldn't find a SegmentTemplate, SegmentList, or BaseURL for a Representation.")
                        
                        if segment_template is not None:
                            segment_template = copy(segment_template)
                            
                            for item in ("initialization", "media"):
                                if not segment_template.get(item):
                                    continue
                                segment_template.set(
                                    item, segment_template.get(item).replace("$RepresentationID$", rep.get("id"))
                                )
                                query = urllib.parse.urlparse(url).query
                                if query and not urllib.parse.urlparse(segment_template.get(item)).query:
                                    segment_template.set(item, segment_template.get(item) + "?" + query)
                                if not re.match("^https?://", segment_template.get(item).lower()):
                                    segment_template.set(item, urllib.parse.urljoin(
                                        period_base_url if not rep_base_url else rep_base_url, segment_template.get(item)
                                    ))

                            period_duration = period.get("duration")
                            if period_duration:
                                period_duration = Track.pt_to_sec(period_duration)
                            mpd_duration = root.get("mediaPresentationDuration")
                            if mpd_duration:
                                mpd_duration = Track.pt_to_sec(mpd_duration)

                            track_url = []

                            def replace_fields(url_str, **kwargs):
                                for field, value in kwargs.items():
                                    url_str = url_str.replace(f"${field}$", str(value))
                                    m = re.search(fr"\${re.escape(field)}%([a-z0-9]+)\$", url_str, flags=re.I)
                                    if m:
                                        url_str = url_str.replace(m.group(), f"{value:{m.group(1)}}")
                                return url_str

                            initialization = segment_template.get("initialization")
                            if initialization:
                                track_url.append(replace_fields(
                                    initialization,
                                    Bandwidth=rep.get("bandwidth"),
                                    RepresentationID=rep.get("id")
                                ))

                            start_number = int(segment_template.get("startNumber") or 1)

                            segment_timeline = segment_template.find("SegmentTimeline")
                            if segment_timeline is not None:
                                seg_time_list = []
                                current_time = 0
                                for s in segment_timeline.findall("S"):
                                    if s.get("t"):
                                        current_time = int(s.get("t"))
                                    for _ in range(1 + (int(s.get("r") or 0))):
                                        seg_time_list.append(current_time)
                                        current_time += int(s.get("d"))
                                seg_num_list = list(range(start_number, len(seg_time_list) + start_number))
                                track_url += [
                                    replace_fields(
                                        segment_template.get("media"),
                                        Bandwidth=rep.get("bandwidth"),
                                        Number=n,
                                        RepresentationID=rep.get("id"),
                                        Time=t
                                    )
                                    for t, n in zip(seg_time_list, seg_num_list)
                                ]
                            else:
                                period_duration = period_duration or mpd_duration
                                segment_duration = (
                                    float(segment_template.get("duration")) / float(segment_template.get("timescale") or 1)
                                )
                                total_segments = math.ceil(period_duration / segment_duration)
                                track_url += [
                                    replace_fields(
                                        segment_template.get("media"),
                                        Bandwidth=rep.get("bandwidth"),
                                        Number=s,
                                        RepresentationID=rep.get("id"),
                                        Time=s
                                    )
                                    for s in range(start_number, start_number + total_segments)
                                ]

                    track_id = "{codec}-{lang}-{bitrate}-{extra}".format(
                        codec=codecs,
                        lang=track_lang,
                        bitrate=rep.get("bandwidth") or 0,
                        extra=(adaptation_set.get("audioTrackId") or "") + (rep.get("id") or ""),
                    )
                    track_id = md5(track_id.encode()).hexdigest()
                    
                    def get_track_size(track_repr):
                        segment_list = track_repr.findall('SegmentList')
                        if segment_list:
                            file_size = sorted(segment_list[0].findall('SegmentURL'), 
                                              key=lambda segment_url: int(segment_url.get('mediaRange').split('-')[1]), 
                                              reverse=True)
                            if file_size:
                                return int(file_size[0].get('mediaRange').split('-')[1])
                        return None

                    if content_type == "video":
                        fps = rep.get("frameRate") or adaptation_set.get("frameRate")
                        if not fps:
                            fps = _calculate_fps_from_timeline(rep, period)
                            if fps:
                                log.debug(f" + Calculated FPS {fps} for video track (codec: {codecs}, resolution: {rep.get('width')}x{rep.get('height')})")
                        
                        tracks.append(VideoTrack(
                            id_=track_id,
                            source=source,
                            url=track_url,
                            size=get_track_size(track_repr=rep),
                            codec=(codecs or "").split(".")[0],
                            language=track_lang,
                            bitrate=rep.get("bandwidth"),
                            width=int(rep.get("width") or 0) or adaptation_set.get("width"),
                            height=int(rep.get("height") or 0) or adaptation_set.get("height"),
                            fps=fps,
                            hdr10=any(
                                x.get("schemeIdUri") == "urn:mpeg:mpegB:cicp:TransferCharacteristics"
                                and x.get("value") == "16"
                                for x in adaptation_set.findall("SupplementalProperty")
                            ) or any(
                                x.get("schemeIdUri") == "http://dashif.org/metadata/hdr"
                                and x.get("value") == "SMPTE2094-40"
                                for x in adaptation_set.findall("SupplementalProperty")
                            ),
                            hlg=any(
                                x.get("schemeIdUri") == "urn:mpeg:mpegB:cicp:TransferCharacteristics"
                                and x.get("value") == "18"
                                for x in adaptation_set.findall("SupplementalProperty")
                            ),
                            dvhdr=(
                                (isinstance(codecs, str) and codecs.startswith(("dvhe.08", "dvh1.08"))) or
                                (isinstance(supplementalcodecs, str) and "dvh1.08" in supplementalcodecs)
                            ),
                            dv=codecs and codecs.startswith(("dvhe", "dvh1")),
                            descriptor=Track.Descriptor.MPD,
                            encrypted=encrypted,
                            pssh=pssh,
                            pr_pssh=pr_pssh,
                            kid=kid,
                            extra=(rep, adaptation_set)
                        ))
                    elif content_type == "audio":
                        audio_kid = kid
                        if not audio_kid:
                            for video_rep in adaptation_set.getparent().findall(".//Representation"):
                                if video_rep.get("mimeType", "").startswith("video/"):
                                    for prot in video_rep.findall("ContentProtection") + adaptation_set.getparent().findall(".//ContentProtection"):
                                        if prot.get("schemeIdUri") == "urn:mpeg:dash:mp4protection:2011":
                                            kid_val = prot.get("{urn:mpeg:cenc:2013}default_KID")
                                            if kid_val:
                                                audio_kid = uuid.UUID(kid_val).hex.lower()
                                                log.debug(f"Audio inheriting KID from video: {audio_kid}")
                                                break
                                    if audio_kid:
                                        break
                        
                        tracks.append(AudioTrack(
                            id_=track_id,
                            source=source,
                            url=track_url,
                            codec=(codecs or "").split(".")[0],
                            language=track_lang,
                            bitrate=rep.get("bandwidth"),
                            channels=next(iter(
                                rep.xpath("AudioChannelConfiguration/@value")
                                or adaptation_set.xpath("AudioChannelConfiguration/@value")
                            ), None),
                            descriptive=any(
                                (x.get("schemeIdUri") == "urn:mpeg:dash:role:2011" and x.get("value") == "description")
                                or (x.get("schemeIdUri") == "urn:tva:metadata:cs:AudioPurposeCS:2007" and x.get("value") == "1")
                                for x in adaptation_set.findall("Accessibility")
                            ),
                            atmos=any(
                                prop.get("schemeIdUri") == "tag:dolby.com,2018:dash:EC3_ExtensionType:2018" and prop.get("value") == "JOC"
                                for prop in rep.findall("SupplementalProperty")
                            ),
                            descriptor=Track.Descriptor.MPD,
                            encrypted=encrypted,
                            pssh=pssh,
                            pr_pssh=pr_pssh,
                            kid=audio_kid,
                            extra=(rep, adaptation_set)
                        ))
                    elif content_type in ("text", "application"):
                        role_elem = adaptation_set.find(".//{*}Role")
                        role = role_elem.get("value") if role_elem is not None else ""
                        
                        is_forced = (role == "forced-subtitle")
                        is_sdh = (role == "caption") or (role == "sdh")
                        is_normal = (role == "subtitle") or (role == "main") or (not role)
                        
                        adapt_set_id = adaptation_set.get("id", "")
                        if not is_forced and ("forced" in adapt_set_id.lower() or "fn" in adapt_set_id.lower()):
                            is_forced = True
                            log.debug(f" + Detected forced subtitle by adaptation set ID: {adapt_set_id}")
                        
                        rep_id = rep.get("id", "")
                        if not is_forced and ("forced" in rep_id.lower() or "fn" in rep_id.lower()):
                            is_forced = True
                            log.debug(f" + Detected forced subtitle by representation ID: {rep_id}")
                        
                        if not is_forced and codecs and ("forced" in codecs.lower() or "fn" in codecs.lower()):
                            is_forced = True
                            log.debug(f" + Detected forced subtitle by codec pattern: {codecs}")
                        
                        if source == 'HMAX':
                            segment_template = rep.find("SegmentTemplate")
                            sub_path_url = rep.findtext("BaseURL")
                            if not sub_path_url:
                                sub_path_url = segment_template.get('media') if segment_template else None
                            
                            if not sub_path_url:
                                continue
                            
                            try:
                                path = re.search(r'(t\/.+?\/)t', sub_path_url).group(1)
                            except AttributeError:
                                path = 't/sub/'
                            
                            # Build the complete URL - make sure period_base_url is properly set
                            if is_normal:
                                track_url = period_base_url + path + adaptation_set.get('lang') + '_sub.vtt'
                            elif is_sdh:
                                track_url = period_base_url + path + adaptation_set.get('lang') + '_sdh.vtt'
                            elif is_forced:
                                track_url = period_base_url + path + adaptation_set.get('lang') + '_forced.vtt'
                            else:
                                track_url = period_base_url + path + adaptation_set.get('lang') + '_sub.vtt'
                            
                            # Also store the SegmentTemplate for multi-segment subtitles
                            if segment_template is not None and segment_template.get('media') and '$Number$' in segment_template.get('media'):
                                # This is a segmented subtitle, store the pattern
                                media_pattern = segment_template.get('media')
                                if not re.match("^https?://", media_pattern):
                                    media_pattern = urllib.parse.urljoin(period_base_url, media_pattern)
                                track_url = media_pattern  # Store pattern, will be expanded during download
                            
                            tracks.append(TextTrack(
                                id_=track_id,
                                source=source,
                                url=track_url,
                                codec=(codecs or "").split(".")[0] if codecs else "vtt",
                                language=track_lang,
                                forced=is_forced,
                                sdh=is_sdh,
                                descriptor=Track.Descriptor.MPD,
                                extra=(rep, adaptation_set)  # Pass extra for segment info
                            ))
                        else:
                            extra_info = {
                                "role": role,
                                "adaptation_set_id": adapt_set_id,
                                "representation_id": rep_id,
                                "original_forced_detection": is_forced
                            }
                            
                            if track_url and isinstance(track_url, list):
                                for url_check in track_url:
                                    if isinstance(url_check, str) and ("forced" in url_check.lower() or "_fn" in url_check.lower() or "forced-subtitle" in url_check.lower()):
                                        is_forced = True
                                        extra_info["detected_by_url"] = True
                                        log.debug(f" + Detected forced subtitle by URL pattern: {url_check}")
                                        break
                            
                            tracks.append(TextTrack(
                                id_=track_id,
                                source=source,
                                url=track_url,
                                codec=(codecs or "").split(".")[0] if codecs else "vtt",
                                language=track_lang,
                                forced=is_forced,
                                sdh=is_sdh,
                                descriptor=Track.Descriptor.MPD,
                                extra=extra_info
                            ))
        
        period_tracks_obj = Tracks()
        period_tracks_obj.add(tracks, warn_only=True)
        
        if is_multi_period:
            for track in period_tracks_obj.videos + period_tracks_obj.audios + period_tracks_obj.subtitles:
                if not isinstance(track.url, list):
                    track.url = [track.url]
            period_tracks_list.append(period_tracks_obj)
        else:
            return period_tracks_obj
    
    if is_multi_period:
        return _merge_periods(period_tracks_list, source, log)
    
    return Tracks()  # fallback


def _merge_periods(period_tracks_list, source, log):
    """Merge tracks from multiple periods into a single set."""
    if not period_tracks_list:
        return Tracks()
    
    if len(period_tracks_list) == 1:
        return period_tracks_list[0]
    
    combined = Tracks()
    
    seen_videos = {}
    seen_audios = {}
    seen_subs = {}
    
    for period_tracks in period_tracks_list:
        for video in period_tracks.videos:
            key = (video.width, video.height, video.codec, video.hdr10, video.dv)
            if key not in seen_videos:
                new_video = video
                new_video.url = []
                seen_videos[key] = new_video
            
            if isinstance(video.url, list):
                seen_videos[key].url.extend(video.url)
            else:
                seen_videos[key].url.append(video.url)
        
        for audio in period_tracks.audios:
            lang = str(audio.language) if audio.language else "und"
            if lang not in seen_audios:
                new_audio = audio
                new_audio.url = []
                seen_audios[lang] = new_audio
            
            if isinstance(audio.url, list):
                seen_audios[lang].url.extend(audio.url)
            else:
                seen_audios[lang].url.append(audio.url)
        
        for sub in period_tracks.subtitles:
            lang = str(sub.language) if sub.language else "und"
            sub_type = "forced" if sub.forced else "sdh" if sub.sdh else "normal"
            key = f"{lang}_{sub_type}"
            
            if key not in seen_subs:
                seen_subs[key] = sub
                if not isinstance(seen_subs[key].url, list):
                    seen_subs[key].url = [seen_subs[key].url]
    
    combined.videos = list(seen_videos.values())
    combined.audios = list(seen_audios.values())
    combined.subtitles = list(seen_subs.values())
    
    log.debug(f" + Merged {len(period_tracks_list)} periods into: "
              f"{len(combined.videos)} video, {len(combined.audios)} audio, "
              f"{len(combined.subtitles)} subtitle tracks")
    
    return combined