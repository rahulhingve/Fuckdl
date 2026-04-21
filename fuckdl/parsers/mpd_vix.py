import asyncio
import base64
import logging
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


def _calculate_fps_from_segment_timeline(segment_template, timescale=None, segment_timeline=None):
    """
    Calculate FPS from segment timeline when frameRate is not present in MPD.
    Supports VFR (Variable Frame Rate) detection and proper FPS calculation.
    
    :param segment_template: The SegmentTemplate element
    :param timescale: Timescale value (optional, will be extracted from segment_template if not provided)
    :param segment_timeline: SegmentTimeline element (optional, will be extracted if not provided)
    :return: Float FPS value or None if cannot calculate
    """
    import logging
    import statistics
    log = logging.getLogger("VIX")
    
    if segment_template is None:
        return None
    
    # Get timescale
    if timescale is None:
        timescale = int(segment_template.get("timescale", 0))
    
    # Get segment timeline
    if segment_timeline is None:
        segment_timeline = segment_template.find("SegmentTimeline")
    
    if segment_timeline is None:
        return None
    
    # Collect all segment durations
    durations = []
    for s in segment_timeline.findall("S"):
        d = int(s.get("d", 0))
        if d > 0:
            r = int(s.get("r", 0))
            if r > 0:
                # Repeat count - add duration r+1 times
                durations.extend([d] * (r + 1))
            else:
                durations.append(d)
    
    if not durations:
        return None
    
    # Check for VFR (Variable Frame Rate) content
    is_vfr = False
    if len(durations) > 1:
        try:
            variance = statistics.variance(durations) if len(durations) > 1 else 0
            # If variance is high (> 1% of mean), consider it VFR
            mean_duration = sum(durations) / len(durations)
            if variance > (mean_duration * 0.01):
                is_vfr = True
                log.debug(f" + Detected VFR content (variance={variance:.2f}, mean={mean_duration:.2f})")
        except (statistics.StatisticsError, TypeError):
            pass
    
    # Calculate average duration
    if is_vfr:
        # Use median for VFR content (more robust)
        avg_duration_ticks = statistics.median(durations)
    else:
        avg_duration_ticks = sum(durations) / len(durations)
    
    # Calculate FPS
    if timescale > 0:
        fps = timescale / avg_duration_ticks
    elif avg_duration_ticks > 0:
        # Assume timescale of 1000 if not specified (milliseconds)
        fps = 1000 / avg_duration_ticks
    else:
        return None
    
    # Round to common FPS values for cleaner output
    for common_fps in [23.976, 24.0, 25.0, 29.97, 30.0, 50.0, 59.94, 60.0, 120.0]:
        if abs(fps - common_fps) < 0.01:
            fps = common_fps
            break
    
    log.debug(f" + Calculated FPS {fps:.3f} from {len(durations)} segments (timescale={timescale}, VFR={is_vfr})")
    return round(fps, 3)


def _calculate_fps_from_representation(rep, adaptation_set, period):
    """
    Calculate FPS from various sources in the representation.
    
    :param rep: Representation element
    :param adaptation_set: AdaptationSet element
    :param period: Period element
    :return: Float FPS value or None if cannot calculate
    """
    import logging
    log = logging.getLogger("VIX")
    
    # First try to get frameRate attribute
    fps = rep.get("frameRate") or adaptation_set.get("frameRate")
    if fps:
        try:
            # Handle fraction format like "24000/1001"
            if '/' in str(fps):
                num, den = str(fps).split('/')
                fps = float(num) / float(den)
            else:
                fps = float(fps)
            return round(fps, 3)
        except (ValueError, TypeError):
            pass
    
    # Try to get from segment template
    segment_template = rep.find("SegmentTemplate")
    if segment_template is None:
        segment_template = adaptation_set.find("SegmentTemplate")
    
    if segment_template is not None:
        calculated_fps = _calculate_fps_from_segment_timeline(segment_template)
        if calculated_fps:
            return calculated_fps
    
    return None


def _get_default_fps(width, height):
    """
    Return a reasonable default FPS based on resolution and content type.
    Most streaming content is 23.976 fps (film content) or 29.97 fps (NTSC).
    """
    # Default to 23.976 for most streaming content
    if width >= 3840:  # 4K
        return 23.976
    elif width >= 1920:  # 1080p
        return 23.976
    elif width >= 1280:  # 720p
        return 23.976
    elif width >= 854:   # 480p
        return 23.976
    elif width >= 640:   # 360p
        return 23.976
    else:
        return 23.976


def parse_vix(*, url=None, data=None, source, session=None, downloader=None):
    """
    Specialized parser for VIX multi-period MPD manifests.
    Handles VIX's ad periods and extracts proper FPS.
    """
    import logging
    log = logging.getLogger("VIX")
    
    tracks = []
    
    if not data:
        if not url:
            raise ValueError("Neither a URL nor a document was provided")
        
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

    root = load_xml(data)
    if root.tag != "MPD":
        raise ValueError("Non-MPD document provided")

    # Get base URL from original MPD URL
    base_url = url.rsplit('/', 1)[0] + '/'
    
    # Get overall MPD duration
    mpd_duration = Track.pt_to_sec(root.get("mediaPresentationDuration"))
    
    # Store segments by track type and representation
    video_segments = {}
    audio_segments = {}
    text_segments = {}
    
    def get_track_size(track_repr):
        segment_list = track_repr.findall('SegmentList')
        if segment_list:
            file_size = sorted(segment_list[0].findall('SegmentURL'), 
                              key=lambda segment_url: int(segment_url.get('mediaRange').split('-')[1]), 
                              reverse=True)
            if file_size:
                return int(file_size[0].get('mediaRange').split('-')[1])
        return None

    # Process all periods
    periods = root.findall("Period")
    log.info(f" + Processing {len(periods)} periods for VIX")
    
    for period_idx, period in enumerate(periods):
        period_duration = Track.pt_to_sec(period.get("duration"))
        
        # Get period base URL
        period_base_url = period.findtext("BaseURL") or root.findtext("BaseURL") or base_url
        
        if url and (not period_base_url or not re.match("^https?://", (period_base_url or "").lower())):
            period_base_url = urllib.parse.urljoin(url, period_base_url)

        for adaptation_set in period.findall("AdaptationSet"):
            # Skip trick mode streams
            if any(x.get("schemeIdUri") == "http://dashif.org/guidelines/trickmode"
                   for x in adaptation_set.findall("EssentialProperty")
                   + adaptation_set.findall("SupplementalProperty")):
                continue

            for rep in adaptation_set.findall("Representation"):
                # Get content type
                try:
                    content_type = next(x for x in [
                        rep.get("contentType"),
                        rep.get("mimeType"),
                        adaptation_set.get("contentType"),
                        adaptation_set.get("mimeType")
                    ] if bool(x))
                except StopIteration:
                    continue
                else:
                    content_type = content_type.split("/")[0]
                
                if content_type.startswith("image"):
                    continue
                
                # Get codec
                codecs = rep.get("codecs") or adaptation_set.get("codecs")
                supplementalcodecs = rep.get("{urn:scte:dash:scte214-extensions}supplementalCodecs") or adaptation_set.get("{urn:scte:dash:scte214-extensions}supplementalCodecs")
                
                if content_type in ("text", "application"):
                    mime = adaptation_set.get("mimeType")
                    if mime and not mime.endswith("/mp4"):
                        codecs = mime.split("/")[1]
                
                # Get language
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
                
                # Content protection
                protections = rep.findall("ContentProtection") + adaptation_set.findall("ContentProtection")
                encrypted = bool(protections)
                pssh = None
                kid = None
                pr_pssh = None
                
                for protection in protections:
                    kid = protection.get("default_KID")
                    if kid:
                        kid = uuid.UUID(kid).hex
                    else:
                        kid = protection.get("kid")
                        if kid:
                            kid = uuid.UUID(bytes_le=base64.b64decode(kid)).hex
                    
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
                
                # URL resolution
                rep_base_url = rep.findtext("BaseURL")
                aset_base_url = adaptation_set.findtext("BaseURL")
                effective_base_url = rep_base_url or aset_base_url or period_base_url
                
                # Get segment template
                segment_template = rep.find("SegmentTemplate")
                if segment_template is None:
                    segment_template = adaptation_set.find("SegmentTemplate")
                if segment_template is None:
                    continue
                
                media_template = segment_template.get("media")
                initialization_template = segment_template.get("initialization")
                start_number = int(segment_template.get("startNumber") or 1)
                timescale = float(segment_template.get("timescale") or 1)
                segment_duration = float(segment_template.get("duration") or 0)
                presentation_time_offset = int(segment_template.get("presentationTimeOffset") or 0)
                
                # Replace RepresentationID
                if media_template:
                    media_template = media_template.replace("$RepresentationID$", rep.get("id"))
                if initialization_template:
                    initialization_template = initialization_template.replace("$RepresentationID$", rep.get("id"))
                
                # Add query from original URL if present
                query = urllib.parse.urlparse(url).query
                if query:
                    if media_template and not urllib.parse.urlparse(media_template).query:
                        media_template += "?" + query
                    if initialization_template and not urllib.parse.urlparse(initialization_template).query:
                        initialization_template += "?" + query
                
                track_url = []
                
                def replace_fields(url_str, **kwargs):
                    if not url_str:
                        return url_str
                    result = url_str
                    for field, value in kwargs.items():
                        result = result.replace(f"${field}$", str(value))
                        m = re.search(fr"\${re.escape(field)}%([a-z0-9]+)\$", result, flags=re.I)
                        if m:
                            result = result.replace(m.group(), f"{value:{m.group(1)}}")
                    return result
                
                # Add initialization segment (only from first period)
                if initialization_template and period_idx == 0:
                    init_url = replace_fields(
                        initialization_template,
                        Bandwidth=rep.get("bandwidth"),
                        RepresentationID=rep.get("id")
                    )
                    if not re.match("^https?://", init_url.lower()):
                        init_url = urllib.parse.urljoin(effective_base_url, init_url)
                    track_url.append(init_url)
                
                # Get segments from timeline
                segment_timeline = segment_template.find("SegmentTimeline")
                if segment_timeline is not None:
                    seg_time_list = []
                    current_time = 0
                    for s in segment_timeline.findall("S"):
                        if s.get("t"):
                            current_time = int(s.get("t"))
                        repeat = int(s.get("r") or 0) + 1
                        segment_duration_val = int(s.get("d"))
                        for _ in range(repeat):
                            seg_time_list.append(current_time)
                            current_time += segment_duration_val
                    
                    seg_num_list = list(range(start_number, start_number + len(seg_time_list)))
                    
                    for time_val, num in zip(seg_time_list, seg_num_list):
                        media_url = replace_fields(
                            media_template,
                            Bandwidth=rep.get("bandwidth"),
                            Number=num,
                            RepresentationID=rep.get("id"),
                            Time=time_val
                        )
                        if not re.match("^https?://", media_url.lower()):
                            media_url = urllib.parse.urljoin(effective_base_url, media_url)
                        track_url.append(media_url)
                else:
                    # Duration-based segments
                    period_duration_sec = Track.pt_to_sec(period.get("duration")) or mpd_duration
                    
                    if period_duration_sec and segment_duration and timescale:
                        segment_duration_sec = segment_duration / timescale
                        total_segments = int(math.ceil(period_duration_sec / segment_duration_sec))
                        
                        for segment_num in range(start_number, start_number + total_segments):
                            time_value = int(presentation_time_offset + (segment_duration * (segment_num - start_number)))
                            media_url = replace_fields(
                                media_template,
                                Bandwidth=rep.get("bandwidth"),
                                Number=segment_num,
                                RepresentationID=rep.get("id"),
                                Time=time_value
                            )
                            if not re.match("^https?://", media_url.lower()):
                                media_url = urllib.parse.urljoin(effective_base_url, media_url)
                            track_url.append(media_url)
                    else:
                        continue
                
                # Create unique key
                rep_key = f"{codecs}-{track_lang}-{rep.get('bandwidth')}-{rep.get('id')}"
                
                # Store segments
                if content_type == "video":
                    if rep_key not in video_segments:
                        # Calculate FPS for this representation
                        fps = _calculate_fps_from_representation(rep, adaptation_set, period)
                        
                        # If still no FPS, use default based on resolution
                        if not fps:
                            width = int(rep.get("width") or adaptation_set.get("width") or 0)
                            height = int(rep.get("height") or adaptation_set.get("height") or 0)
                            fps = _get_default_fps(width, height)
                            log.debug(f" + Using default FPS {fps} for {width}x{height} video track")
                        
                        video_segments[rep_key] = {
                            'urls': [],
                            'rep': rep,
                            'adaptation_set': adaptation_set,
                            'segment_template': segment_template,
                            'segment_timeline': segment_timeline,
                            'codecs': codecs,
                            'supplementalcodecs': supplementalcodecs,
                            'track_lang': track_lang,
                            'encrypted': encrypted,
                            'pssh': pssh,
                            'pr_pssh': pr_pssh,
                            'kid': kid,
                            'bitrate': rep.get("bandwidth"),
                            'width': rep.get("width") or adaptation_set.get("width"),
                            'height': rep.get("height") or adaptation_set.get("height"),
                            'fps': fps,
                            'size': get_track_size(rep),
                        }
                    if isinstance(track_url, list):
                        video_segments[rep_key]['urls'].extend(track_url)
                    else:
                        video_segments[rep_key]['urls'] = [track_url]
                        
                elif content_type == "audio":
                    if rep_key not in audio_segments:
                        audio_segments[rep_key] = {
                            'urls': [],
                            'rep': rep,
                            'adaptation_set': adaptation_set,
                            'codecs': codecs,
                            'track_lang': track_lang,
                            'encrypted': encrypted,
                            'pssh': pssh,
                            'pr_pssh': pr_pssh,
                            'kid': kid,
                            'bitrate': rep.get("bandwidth"),
                            'channels': next(iter(
                                rep.xpath("AudioChannelConfiguration/@value")
                                or adaptation_set.xpath("AudioChannelConfiguration/@value")
                            ), None),
                        }
                    if isinstance(track_url, list):
                        audio_segments[rep_key]['urls'].extend(track_url)
                    else:
                        audio_segments[rep_key]['urls'] = [track_url]
                        
                elif content_type in ("text", "application"):
                    if rep_key not in text_segments:
                        is_normal = any(x.get("value") == "subtitle" for x in adaptation_set.findall("Role"))
                        is_sdh = any(x.get("value") == "caption" for x in adaptation_set.findall("Role"))
                        is_forced = any(x.get("value") == "forced-subtitle" for x in adaptation_set.findall("Role"))
                        
                        text_segments[rep_key] = {
                            'urls': [],
                            'rep': rep,
                            'adaptation_set': adaptation_set,
                            'codecs': codecs,
                            'track_lang': track_lang,
                            'encrypted': encrypted,
                            'pssh': pssh,
                            'pr_pssh': pr_pssh,
                            'kid': kid,
                            'is_normal': is_normal,
                            'is_sdh': is_sdh,
                            'is_forced': is_forced,
                        }
                    if isinstance(track_url, list):
                        text_segments[rep_key]['urls'].extend(track_url)
                    else:
                        text_segments[rep_key]['urls'] = [track_url]
    
    # Create video tracks with FPS calculation
    for rep_key, data in video_segments.items():
        track_id = md5(rep_key.encode()).hexdigest()
        
        # Use pre-calculated FPS
        fps = data['fps']
        
        # Double-check FPS from segment timeline if available and FPS is still None
        if not fps and data.get('segment_template') is not None:
            segment_template = data['segment_template']
            segment_timeline = data.get('segment_timeline')
            timescale = int(segment_template.get("timescale", 0)) if segment_template is not None else 0
            calculated_fps = _calculate_fps_from_segment_timeline(segment_template, timescale, segment_timeline)
            if calculated_fps:
                fps = calculated_fps
                log.debug(f" + Re-calculated FPS from segment timeline: {fps}")
        
        # If still no FPS, use default based on resolution
        if not fps:
            width = int(data['width'] or 0)
            height = int(data['height'] or 0)
            fps = _get_default_fps(width, height)
            log.debug(f" + Using default FPS for {width}x{height}: {fps}")
        
        # Log FPS information
        if fps:
            log.debug(f" + Video track FPS: {fps} (resolution: {data['width']}x{data['height']})")
        else:
            log.debug(f" + Video track FPS: Unknown (resolution: {data['width']}x{data['height']})")
        
        tracks.append(VideoTrack(
            id_=track_id,
            source=source,
            url=data['urls'],
            size=data['size'],
            codec=(data['codecs'] or "").split(".")[0],
            language=data['track_lang'],
            bitrate=data['bitrate'],
            width=int(data['width'] or 0),
            height=int(data['height'] or 0),
            fps=fps,
            hdr10=any(
                x.get("schemeIdUri") == "urn:mpeg:mpegB:cicp:TransferCharacteristics"
                and x.get("value") == "16"
                for x in data['adaptation_set'].findall("SupplementalProperty")
            ) or any(
                x.get("schemeIdUri") == "http://dashif.org/metadata/hdr"
                and x.get("value") == "SMPTE2094-40"
                for x in data['adaptation_set'].findall("SupplementalProperty")
            ),
            hlg=any(
                x.get("schemeIdUri") == "urn:mpeg:mpegB:cicp:TransferCharacteristics"
                and x.get("value") == "18"
                for x in data['adaptation_set'].findall("SupplementalProperty")
            ),
            dvhdr=(
                (isinstance(data['codecs'], str) and data['codecs'].startswith(("dvhe.08", "dvh1.08"))) or
                (isinstance(data.get('supplementalcodecs', ''), str) and "dvh1.08" in data['supplementalcodecs'])
            ),
            dv=data['codecs'] and data['codecs'].startswith(("dvhe", "dvh1")),
            descriptor=Track.Descriptor.MPD,
            encrypted=data['encrypted'],
            pssh=data['pssh'],
            pr_pssh=data['pr_pssh'],
            kid=data['kid'],
            extra=(data['rep'], data['adaptation_set'])
        ))
    
    # Create audio tracks
    for rep_key, data in audio_segments.items():
        track_id = md5(rep_key.encode()).hexdigest()
        
        tracks.append(AudioTrack(
            id_=track_id,
            source=source,
            url=data['urls'],
            codec=(data['codecs'] or "").split(".")[0],
            language=data['track_lang'],
            bitrate=data['bitrate'],
            channels=data['channels'],
            descriptive=any(
                (x.get("schemeIdUri") == "urn:mpeg:dash:role:2011" and x.get("value") == "description")
                or (x.get("schemeIdUri") == "urn:tva:metadata:cs:AudioPurposeCS:2007" and x.get("value") == "1")
                for x in data['adaptation_set'].findall("Accessibility")
            ),
            atmos=any(
                prop.get("schemeIdUri") == "tag:dolby.com,2018:dash:EC3_ExtensionType:2018" and prop.get("value") == "JOC"
                for prop in data['rep'].findall("SupplementalProperty")
            ),
            descriptor=Track.Descriptor.MPD,
            encrypted=data['encrypted'],
            pssh=data['pssh'],
            pr_pssh=data['pr_pssh'],
            kid=data['kid'],
            extra=(data['rep'], data['adaptation_set'])
        ))
    
    # Create text tracks - REMOVED 'external' parameter
    for rep_key, data in text_segments.items():
        track_id = md5(rep_key.encode()).hexdigest()
        
        tracks.append(TextTrack(
            id_=track_id,
            source=source,
            url=data['urls'],
            codec=(data['codecs'] or "").split(".")[0] if data['codecs'] else "vtt",
            language=data['track_lang'],
            forced=data['is_forced'],
            sdh=data['is_sdh'],
            descriptor=Track.Descriptor.URL,
            encrypted=data['encrypted'],
            pssh=data['pssh'],
            pr_pssh=data['pr_pssh'],
            kid=data['kid'],
            extra=(data['rep'], data['adaptation_set'])
        ))
    
    log.info(f" + Created {len(video_segments)} video, {len(audio_segments)} audio, {len(text_segments)} text tracks")
    
    tracks_obj = Tracks()
    tracks_obj.add(tracks, warn_only=True)
    return tracks_obj