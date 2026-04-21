from __future__ import annotations

import base64
import hashlib
import json
import uuid
import os
import re
import time
import secrets
import string
from pathlib import Path
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Union
from urllib.parse import urlencode, quote, urlparse

import click
import jsonpickle
import requests
import random
from langcodes import Language
from tldextract import tldextract
from click.core import ParameterSource

from fuckdl.objects import TextTrack, Title, Tracks, Track
from fuckdl.objects.tracks import MenuTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.Logger import Logger
from fuckdl.utils.widevine.device import BaseDevice, LocalDevice, RemoteDevice


class Amazon(BaseService):
    """
    Service code for Amazon VOD (https://amazon.com) and Amazon Prime Video (https://primevideo.com).
    Updated by @AnotherBigUserHere
    V5 - Enhanced with Unshackle improvements (HYBRID, AV1, Atmos, Language Map)

    \b
    Authorization: Cookies
    Security: UHD@L1/SL3000 FHD@L3(ChromeCDM) FHD@L3, Maintains their own license server like Netflix, be cautious.

    \b
    Region is chosen automatically based on domain extension found in cookies.
    Prime Video specific code will be run if the ASIN is detected to be a prime video variant.
    Use 'Amazon Video ASIN Display' for Tampermonkey addon for ASIN
    https://greasyfork.org/en/scripts/381997-amazon-video-asin-display

    V5 Improvements (Unshackle merge):
    + HYBRID mode (DV + HDR10) with separate manifest fetching
    + AV1 codec support with automatic fallback
    + Language map for audio tracks (BCP-47 full tags)
    + Subtitle language resolution (short codes â†’ full regional tags)
    + Descriptive and Boosted audio track detection
    + Atmos 576kb/s audio from DV/UHD manifest
    + Fixed 4K/UHD manifest retrieval
    + Fixed device registration for UHD content
    """

    ALIASES = ["AMZN", "amazon"]
    TITLE_RE = [
        r"^(?:https?://(?:www\.)?(?P<domain>amazon\.(?P<region>com|co\.uk|de|co\.jp)|primevideo\.com)(?:/.+)?/)?(?P<id>[A-Z0-9]{10,}|amzn1\.dv\.gti\.[a-f0-9-]+)", 
        r"^(?:https?://(?:www\.)?(?P<domain>amazon\.(?P<region>com|co\.uk|de|co\.jp)|primevideo\.com)(?:/[^?]*)?(?:\?gti=)?)(?P<id>[A-Z0-9]{10,}|amzn1\.dv\.gti\.[a-f0-9-]+)"
    ]

    REGION_TLD_MAP = {
        "au": "com.au",
        "br": "com.br",
        "jp": "co.jp",
        "mx": "com.mx",
        "tr": "com.tr",
        "gb": "co.uk",
        "us": "com",
    }
    VIDEO_RANGE_MAP = {
        "SDR": "None",
        "HDR10": "Hdr10",
        "DV": "DolbyVision",
    }

    @staticmethod
    @click.command(name="Amazon", short_help="https://amazon.com, https://primevideo.com", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.option("-b", "--bitrate", default="CBR",
                  type=click.Choice(["CVBR", "CBR", "CVBR+CBR"], case_sensitive=False),
                  help="Video Bitrate Mode to download in. CVBR=Constrained Variable Bitrate, CBR=Constant Bitrate.")
    @click.option("-p", "--player", default="html5",
                  type=click.Choice(["html5", "xp"], case_sensitive=False),
                  help="Video playerType to download in. html5, xp.")
    @click.option("-c", "--cdn", default="Akamai", type=str,
                  help="CDN to download from, defaults to the CDN with the highest weight set by Amazon.")
    @click.option("-vq", "--vquality", default="HD",
                  type=click.Choice(["SD", "HD", "UHD"], case_sensitive=False),
                  help="Manifest quality to request.")
    @click.option("-s", "--single", is_flag=True, default=False,
                  help="Force single episode/season instead of getting series ASIN.")
    @click.option("-am", "--amanifest", default="CVBR",
                  type=click.Choice(["CVBR", "CBR", "H265"], case_sensitive=False),
                  help="Manifest to use for audio. Defaults to H265 if the video manifest is missing 640k audio.")
    @click.option("-aq", "--aquality", default="SD",
                  type=click.Choice(["SD", "HD", "UHD"], case_sensitive=False),
                  help="Manifest quality to request for audio. Defaults to the same as --quality.")
    @click.option("-nr", "--no_true_region", is_flag=True, default=False,
                  help="Skip checking true current region.")
    @click.option("-drm", "--drm-system", type=click.Choice(["widevine", "playready"], case_sensitive=False),
                  default="playready", help="Which DRM system to use (widevine or playready)")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Amazon(ctx, **kwargs)

    def __init__(self, ctx, title, bitrate: str, player: str, cdn: str, vquality: str, single: bool,
                 amanifest: str, aquality: str, no_true_region: bool, drm_system: str):
        m = self.parse_title(ctx, title)
        self.bitrate = bitrate
        self.player = player
        self.bitrate_source = ctx.get_parameter_source("bitrate")
        self.cdn = cdn
        self.vquality = vquality
        self.vquality_source = ctx.get_parameter_source("vquality")
        self.single = single
        self.amanifest = amanifest
        self.aquality = aquality
        
        self.no_true_region = no_true_region
        self.drm_system = drm_system
        
        super().__init__(ctx)

        assert ctx.parent is not None

        self.vcodec = ctx.parent.params["vcodec"] or "H264"
        self.range = ctx.parent.params["range_"] or "SDR"
        self.chapters_only = ctx.parent.params["chapters_only"]
        self.atmos = ctx.parent.params["atmos"]
        self.quality = ctx.parent.params.get("quality") or 1080

        self.cdm = ctx.obj.cdm
        self.profile = ctx.obj.profile
        self.playready = self.drm_system == "playready"

        self.region: dict[str, str] = {}
        self.endpoints: dict[str, str] = {}
        self.device: dict[str, str] = {}

        self.pv = False
        self.rpv = False
        self.event = False
        self.device_token = None
        self.device_id = None
        self.customer_id = None
        self.client_id = "f22dbddb-ef2c-48c5-8876-bed0d47594fd"
        
        # Episode counter for future rotation
        self.episode_counter = 0

        # Quality logic
        if self.vquality_source != ParameterSource.COMMANDLINE:
            if 0 < self.quality <= 576 and self.range == "SDR":
                self.log.info(" + Setting manifest quality to SD")
                self.vquality = "SD"

            if self.quality > 1080:
                self.log.info(" + Setting manifest quality to UHD to be able to get 2160p video track")
                self.vquality = "UHD"

        self.vquality = self.vquality or "HD"

        if self.vquality == "UHD":
            self.vcodec = "H265"

        if self.bitrate_source != ParameterSource.COMMANDLINE:
            if self.vcodec == "H265" and self.range == "SDR" and self.bitrate != "CVBR+CBR":
                self.bitrate = "CVBR+CBR"
                self.log.info(" + Changed bitrate mode to CVBR+CBR to be able to get H.265 SDR video track")

            if self.vquality == "UHD" and self.range != "SDR" and self.bitrate != "CBR":
                self.bitrate = "CBR"
                self.log.info(f" + Changed bitrate mode to CBR to be able to get highest quality UHD {self.range} video track")

        self.orig_bitrate = self.bitrate

        # AV1 fast path configuration
        if self.vcodec.upper() == "AV1":
            self.vcodec = "AV1"
            self.bitrate = "CVBR"
            self.orig_bitrate = "CVBR"
            self.amanifest = "CVBR"
            self.atmos = False
            self.log.info(" + AV1 codec selected, using CVBR bitrate mode (fast path)")

        self.configure()

    # ========== HELPER METHODS ==========
    
    def _build_ordered_lang_map_from_mpd(self, mpd_text: str) -> dict:
        """Extract language ordering from MPD AdaptationSet lang attributes and Representation IDs."""
        import xml.etree.ElementTree as ET
        ns_strip = re.compile(r'\{[^}]*\}')
        rid_lang_re = re.compile(r'^audio_([a-zA-Z]{2,3}-[a-zA-Z0-9]{2,5})')
        result = {}
        try:
            root = ET.fromstring(mpd_text)
            for elem in root.iter():
                if ns_strip.sub('', elem.tag) != 'AdaptationSet':
                    continue
                content_type = elem.get('contentType', '') or elem.get('mimeType', '')
                if 'audio' not in content_type.lower():
                    if not any('audio' in (r.get('mimeType', '')).lower() for r in elem):
                        continue
                base_lang = elem.get('lang') or elem.get('language') or ''
                if not base_lang:
                    continue
                for r in elem:
                    if ns_strip.sub('', r.tag) != 'Representation':
                        continue
                    rid = r.get('id') or ''
                    m = rid_lang_re.match(rid)
                    precise = m.group(1) if m else base_lang
                    result.setdefault(base_lang, []).append(precise)
        except Exception as e:
            self.log.debug(f"Language map parsing failed: {e}")
        return result

    def _apply_ordered_lang_map(self, audio_tracks, lang_map: dict) -> None:
        """Apply language ordering to audio tracks."""
        counters = {}
        for track in audio_tracks:
            base = str(track.language)
            if base not in lang_map:
                continue
            ordered = lang_map[base]
            if not any('-' in p for p in ordered):
                continue
            idx = counters.get(base, 0)
            if idx < len(ordered):
                track.language = Language.get(ordered[idx])
            counters[base] = idx + 1

    def _resolve_subtitle_language(self, language_code: str, url: str) -> str:
        """
        Amazon sometimes returns short codes (e.g. 'es') in the subtitle API.
        The subtitle URL itself usually contains the full BCP-47 tag (es-ES, es_419, etc).
        """
        if '-' in language_code:
            return language_code
        # Match patterns like /es-ES/, /es_419/, es-ES_default.ttml, etc.
        m = re.search(
            r'(?:^|[/_-])(' + re.escape(language_code) + r'[-_][A-Za-z0-9]{2,5})(?:[/_.-]|$)',
            url, re.IGNORECASE
        )
        if m:
            return m.group(1).replace('_', '-')
        return language_code

    def _filter_videos_by_codec(self, tracks: Tracks) -> Tracks:
        """Filter video tracks by selected codec (AV1/H265/H264)."""
        if not tracks.videos:
            return tracks
        
        filtered_videos = []
        for video in tracks.videos:
            if not video.codec:
                continue
            
            codec = video.codec.lower()
            
            if self.vcodec == "AV1":
                if "av01" in codec:
                    filtered_videos.append(video)
                else:
                    self.log.debug(f"Skipping non-AV1 track: {codec}")
            elif self.vcodec == "H265":
                if any(x in codec for x in ["hev1", "hvc1", "h265"]):
                    filtered_videos.append(video)
                else:
                    self.log.debug(f"Skipping non-H265 track: {codec}")
            else:  # H264
                if "avc" in codec:
                    filtered_videos.append(video)
                else:
                    self.log.debug(f"Skipping non-H264 track: {codec}")
        
        tracks.videos = filtered_videos
        
        # Fallback for AV1 when no tracks found
        if not tracks.videos and self.vcodec == "AV1":
            self.log.warning(" + No AV1 tracks found in manifest, falling back to H265")
            self.vcodec = "H265"
            return self.get_tracks(self.current_title) if hasattr(self, 'current_title') else tracks
        
        return tracks
    
    # ========== MAIN METHODS ==========

    def get_titles(self):
        if self.domain == "primevideo" and not self.pv:
            raise self.log.exit("Wrong titleID for primevideo cookies")
        res = self.session.get(
            url=self.endpoints["details"],
            params={
                "titleID": self.title,
                "isElcano": "1",
                "sections": ["Atf", "Btf"]
            },
            headers={
                "Accept": "application/json"
            }
        )

        if not res.ok:
            raise self.log.exit(f"Unable to get title: {res.text} [{res.status_code}]")

        data = res.json()["widgets"]
        product_details = data.get("productDetails", {}).get("detail")

        if not product_details:
            error = res.json()["degradations"][0]
            raise self.log.exit(f"Unable to get title: {error['message']} [{error['code']}]")

        titles = []
        titles_ = []

        if data["pageContext"]["subPageType"] == "Event":
           self.event = True

        if data["pageContext"]["subPageType"] == "Movie" or data["pageContext"]["subPageType"] == "Event":
            card = data["productDetails"]["detail"]
            titles.append(Title(
                id_=card["catalogId"],
                type_=Title.Types.MOVIE,
                name=product_details["title"],
                year=card.get("releaseYear", ""),
                original_lang=None,
                source=self.ALIASES[0],
                service_data=card
            ))
            playbackEnvelope_info = self.playbackEnvelope_data([card["catalogId"]])
            for title in titles:
                for playbackInfo in playbackEnvelope_info:
                    if title.id == playbackInfo["titleID"]:
                        title.service_data.update({"playbackInfo": playbackInfo})
                        titles_.append(title)
        else:
            if not self.single:
                headers = {
                    "accept": "application/json",
                    "device-memory": "8",
                    "downlink": "10",
                    "dpr": "2",
                    "ect": "4g",
                    "rtt": "50",
                    "viewport-width": "604",
                    "x-amzn-client-ttl-seconds": "58.999",
                    "x-purpose": "navigation",
                    "x-requested-with": "WebSPA",
                }

                if self.pv:
                    res = self.session.get(f"https://{self.region['base']}/detail/{self.title}", headers=headers)
                    if "redirect" in res.text:
                        url = f"https://{self.region['base']}{res.json()['redirect']}"
                    else:
                        url = res.url
                else:
                    url = f"https://{self.region['base']}/dp/{self.title}"
                
                headers.update({"referer": url})

                response = self.session.get(
                    url=url,
                    params={"dvWebSPAClientVersion": "1.0.106799.0"},
                    headers=headers,
                )
                if not response.status_code == 200:
                    raise self.log.exit("Unable to get seasons")
                data = response.json()
                
                seasons_data = None
                
                if "seasons" in data:
                    seasons_data = {"seasons": data.get("seasons", [])}
                elif "page" in data and isinstance(data["page"], list):
                    for page in data["page"]:
                        if page:
                            try:
                                if "assembly" in page and "body" in page["assembly"]:
                                    for body in page["assembly"]["body"]:
                                        if body and "props" in body:
                                            props = body["props"]
                                            if "atf" in props and "state" in props["atf"]:
                                                if "seasons" in props["atf"]["state"]:
                                                    seasons_data = props["atf"]["state"]
                                                    break
                            except (KeyError, TypeError, AttributeError):
                                continue
                elif "widgets" in data:
                    widgets = data.get("widgets", {})
                    if "seasonSelector" in widgets:
                        seasons = []
                        for season_item in widgets.get("seasonSelector", []):
                            if "titleID" in season_item:
                                seasons.append({"id": season_item["titleID"]})
                        if seasons:
                            seasons_data = {"seasons": seasons}
                
                if not seasons_data:
                    self.log.info("Could not find seasons data, trying to get episodes directly")
                    episodes_titles = self.get_episodes(self.title)
                    titles_.extend(episodes_titles)
                    seasons_data = {"seasons": []}
                
                if seasons_data and "seasons" in seasons_data:
                    seasons = list(seasons_data["seasons"].values()) if isinstance(seasons_data["seasons"], dict) else seasons_data["seasons"]
                    
                    for season in seasons:
                        if isinstance(season, dict):
                            if "seasonLink" in season:
                                seasonLink = season["seasonLink"]
                            elif "id" in season:
                                titleID = season["id"]
                                episodes_titles = self.get_episodes(titleID)
                                titles_.extend(episodes_titles)
                                continue
                            else:
                                continue
                        else:
                            continue
                        
                        match = re.search(r"/detail/([A-Z0-9]{10,})/", seasonLink)
                        if match:
                            titleID = match.group(1)
                        else:
                            self.log.warning(f"Unable to get season id from {seasonLink}, trying direct")
                            import urllib.parse
                            parsed = urllib.parse.urlparse(seasonLink)
                            path_parts = parsed.path.split('/')
                            for part in path_parts:
                                if len(part) == 10 and part.isalnum() and part.isupper():
                                    titleID = part
                                    break
                            else:
                                continue
                        
                        episodes_titles = self.get_episodes(titleID)
                        titles_.extend(episodes_titles)
                else:
                    episodes_titles = self.get_episodes(self.title)
                    titles_.extend(episodes_titles)

            else:
                episodes_titles = self.get_episodes(self.title)
                titles_.extend(episodes_titles)
            
        if titles_ == []:
            raise self.log.exit(" - The profile used does not have the rights to this title.")

        if titles_:
            original_lang = self.get_original_language(self.get_manifest(
                next((x for x in titles_ if x.type == Title.Types.MOVIE or x.episode > 0), titles_[0]),
                video_codec=self.vcodec,
                bitrate_mode=self.bitrate,
                quality=self.vquality,
                ignore_errors=True
            ))
            if original_lang:
                for title in titles_:
                    title.original_lang = Language.get(original_lang)
            else:
                for title in titles_:
                    title.original_lang = Language.get("en")

        filtered_titles = []
        season_episode_count = defaultdict(int)
        for title in titles_:
            key = (title.season, title.episode) 
            if season_episode_count[key] < 1:
                filtered_titles.append(title)
                season_episode_count[key] += 1
        
        titles = filtered_titles

        return titles

    def get_tracks(self, title: Title) -> Tracks:
        """Modified get_tracks to support HYBRID mode, AV1, descriptive/boosted audio, and language maps."""
        if self.chapters_only:
            return []

        # Store current title for fallback
        self.current_title = title
        
        # Increment episode counter
        self.episode_counter += 1

        hybrid_mode = self.range and self.range.upper() in ("DVHDR", "HDRDV", "HYBRID", "DV_HYBRID", "HybridLog")
        
        if hybrid_mode:
            self.log.info(" + HYBRID mode detected - getting both HDR10 and DV tracks")
            
            # First, get the HDR10 manifest (base layer)
            manifest_hdr = self.get_manifest(
                title,
                video_codec=self.vcodec,
                bitrate_mode=self.bitrate,
                quality=self.vquality,
                hdr="HDR10",
                ignore_errors=False
            )
            
            if "rightsException" in manifest_hdr:
                self.log.error(" - The profile used does not have the rights to this title.")
                return Tracks()
            
            # Then get the DV manifest (enhancement layer)
            manifest_dv = self.get_manifest(
                title,
                video_codec="H265",
                bitrate_mode=self.bitrate,
                quality=self.vquality,
                hdr="DV",
                ignore_errors=True
            )
            
            if not manifest_dv or not manifest_dv.get("vodPlaybackUrls"):
                self.log.warning(" - No DV manifest available for HYBRID mode, falling back to HDR10 only")
                self.range = "HDR10"
                return self.get_tracks(title)
            
            chosen_manifest_hdr = self.choose_manifest(manifest_hdr, self.cdn)
            if not chosen_manifest_hdr:
                raise self.log.exit(f"No HDR10 manifests available")
            
            manifest_url_hdr = self.clean_mpd_url(chosen_manifest_hdr["url"], False)
            
            self.log.info(" + Downloading HDR10 Manifest")
            self.log.info(f" + HDR10 Manifest URL: {manifest_url_hdr}")
            
            streamingProtocol_hdr = manifest_hdr["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
            sessionHandoffToken_hdr = manifest_hdr["sessionization"]["sessionHandoffToken"]
            
            # Parse HDR10 manifest
            hdr10_mpd_raw = self.session.get(manifest_url_hdr).text
            hdr10_lang_map = self._build_ordered_lang_map_from_mpd(hdr10_mpd_raw)
            
            tracks = Tracks()
            
            if streamingProtocol_hdr == "DASH":
                mpd_tracks = Tracks.from_mpd(
                    url=manifest_url_hdr,
                    session=self.session,
                    source=self.ALIASES[0],
                )
                
                # Apply language map to HDR10 audio tracks
                if hdr10_lang_map:
                    self._apply_ordered_lang_map(mpd_tracks.audios, hdr10_lang_map)
                
                for track in mpd_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken_hdr, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken_hdr,)
                    else:
                        track.extra = (sessionHandoffToken_hdr,)
                
                tracks.add(mpd_tracks)
                
            elif streamingProtocol_hdr == "SmoothStreaming":
                ism_tracks = Tracks.from_ism(
                    url=manifest_url_hdr,
                    source=self.ALIASES[0],
                )
                
                for track in ism_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken_hdr, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken_hdr,)
                    else:
                        track.extra = (sessionHandoffToken_hdr,)
                
                tracks.add(ism_tracks)
            
            # Filter videos by codec
            tracks = self._filter_videos_by_codec(tracks)
            
            if not tracks.videos:
                raise self.log.exit(f"No {self.vcodec} tracks found, stopping.")
            
            # Mark HDR10 tracks
            for video in tracks.videos:
                video.hdr10 = True
                video.dv = False
            
            # Parse DV manifest for enhancement layer metadata
            chosen_manifest_dv = self.choose_manifest(manifest_dv, self.cdn)
            if chosen_manifest_dv:
                manifest_url_dv = self.clean_mpd_url(chosen_manifest_dv["url"], False)
                self.log.info(" + Downloading DV Manifest (for enhancement layer)")
                self.log.info(f" + Dolby Vision Manifest URL: {manifest_url_dv}")
                
                dv_mpd_raw = self.session.get(manifest_url_dv).text
                dv_lang_map = self._build_ordered_lang_map_from_mpd(dv_mpd_raw)
                
                streamingProtocol_dv = manifest_dv["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
                sessionHandoffToken_dv = manifest_dv["sessionization"]["sessionHandoffToken"]
                
                if streamingProtocol_dv == "DASH":
                    dv_tracks = Tracks.from_mpd(
                        url=manifest_url_dv,
                        session=self.session,
                        source=self.ALIASES[0],
                    )
                    
                    # Apply language map to DV audio tracks
                    if dv_lang_map:
                        self._apply_ordered_lang_map(dv_tracks.audios, dv_lang_map)
                    
                    for track in dv_tracks:
                        if isinstance(track.extra, dict):
                            track.extra = {"session_handoff": sessionHandoffToken_dv, **track.extra}
                        elif isinstance(track.extra, tuple):
                            track.extra = track.extra + (sessionHandoffToken_dv,)
                        else:
                            track.extra = (sessionHandoffToken_dv,)
                            
                elif streamingProtocol_dv == "SmoothStreaming":
                    dv_tracks = Tracks.from_ism(
                        url=manifest_url_dv,
                        source=self.ALIASES[0],
                    )
                    for track in dv_tracks:
                        if isinstance(track.extra, dict):
                            track.extra = {"session_handoff": sessionHandoffToken_dv, **track.extra}
                        elif isinstance(track.extra, tuple):
                            track.extra = track.extra + (sessionHandoffToken_dv,)
                        else:
                            track.extra = (sessionHandoffToken_dv,)
                
                # Mark DV tracks
                for video in dv_tracks.videos:
                    video.dv = True
                    video.hdr10 = False
                
                # Add the best DV track
                dv_tracks.videos = sorted(dv_tracks.videos, key=lambda x: float(x.bitrate or 0.0))
                if dv_tracks.videos:
                    tracks.add([dv_tracks.videos[0]], warn_only=True)
                    self.log.info(f" + Added DV track for enhancement layer: {dv_tracks.videos[0].bitrate // 1000 if dv_tracks.videos[0].bitrate else '?'} kb/s")
                
                # Also add DV audio tracks (may include 576kbps Atmos)
                tracks.add(dv_tracks.audios, warn_only=True)
        else:
            # Normal mode (non-HYBRID)
            tracks = self.get_best_quality(title)
            
            # Filter videos by codec (CRITICAL for AV1)
            tracks = self._filter_videos_by_codec(tracks)
            
            if not tracks.videos:
                raise self.log.exit(f"No {self.vcodec} tracks found, stopping.")
            
            effective_vcodec = "DolbyVision" if self.range == "DV" and self.vcodec == "H265" else self.vcodec
            effective_range = self.range
            manifest = self.get_manifest(
                title,
                video_codec=effective_vcodec,
                bitrate_mode=self.bitrate,
                quality=self.vquality,
                hdr=effective_range,
                ignore_errors=self.range == "DV"
            )
            
            if self.range == "DV" and not manifest.get("vodPlaybackUrls"):
                self.log.warning(" - Dolby Vision request rejected by server, retrying with HDR10...")
                effective_vcodec = self.vcodec
                effective_range = "HDR10"
                manifest = self.get_manifest(
                    title,
                    video_codec=effective_vcodec,
                    bitrate_mode=self.bitrate,
                    quality=self.vquality,
                    hdr=effective_range,
                    ignore_errors=False
                )
            
            if "rightsException" in manifest:
                self.log.error(" - The profile used does not have the rights to this title.")
                return Tracks()
            
            chosen_manifest = self.choose_manifest(manifest, self.cdn)
            if not chosen_manifest:
                raise self.log.exit(f"No manifests available")
            
            manifest_url = self.clean_mpd_url(chosen_manifest["url"], False)

            self.log.info(" + Downloading Manifest")
                      
            if self.event:
                devicetype = self.device["device_type"]
                manifest_url = chosen_manifest["url"]
                manifest_url = f"{manifest_url}?amznDtid={devicetype}&encoding=segmentBase"
            
            self.log.info(f" + Manifest URL: {manifest_url}")
            
            mpd_raw = self.session.get(manifest_url).text
            lang_order_map = self._build_ordered_lang_map_from_mpd(mpd_raw)
            self.log.info(f" + MPD language map: {sum(len(v) for v in lang_order_map.values())} representations indexed")
            
            streamingProtocol = manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
            sessionHandoffToken = manifest["sessionization"]["sessionHandoffToken"]
            
            if streamingProtocol == "DASH":
                mpd_tracks = Tracks.from_mpd(
                    url=manifest_url,
                    session=self.session,
                    source=self.ALIASES[0],
                )
                
                for track in mpd_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken,)
                    else:
                        track.extra = (sessionHandoffToken,)
                
                tracks.add(mpd_tracks)
                
                if lang_order_map:
                    self._apply_ordered_lang_map(tracks.audios, lang_order_map)

            elif streamingProtocol == "SmoothStreaming":
                ism_tracks = Tracks.from_ism(
                    url=manifest_url,
                    source=self.ALIASES[0],
                )
                
                for track in ism_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken,)
                    else:
                        track.extra = (sessionHandoffToken,)
                
                tracks.add(ism_tracks)
            else:
                raise self.log.exit(f"Unsupported manifest type: {streamingProtocol}")            
            
            for video in tracks.videos:
                video.hdr10 = manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["dynamicRange"] == "Hdr10"
                video.dv = manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["dynamicRange"] == "DolbyVision"

        # Detect descriptive and boosted audio tracks
        for audio in tracks.audios:
            if audio.descriptor == audio.descriptor.MPD:
                if audio.extra and len(audio.extra) > 1:
                    if isinstance(audio.extra, dict):
                        subtype = audio.extra.get("audioTrackSubtype", "")
                        audio.descriptive = (subtype == "descriptive")
                        if "boosteddialog" in subtype:
                            audio.boosted = True
                            audio.bitrate = 1
                    else:
                        try:
                            subtype = audio.extra[1].get("audioTrackSubtype", "") if len(audio.extra) > 1 else ""
                            audio.descriptive = (subtype == "descriptive")
                            if "boosteddialog" in subtype:
                                audio.boosted = True
                                audio.bitrate = 1
                        except (IndexError, AttributeError):
                            pass
            elif audio.descriptor == audio.descriptor.ISM:
                if audio.extra and len(audio.extra) > 0:
                    if isinstance(audio.extra, dict):
                        subtype = audio.extra.get("audioTrackSubtype", "")
                        audio.descriptive = (subtype == "descriptive")
                        if "boosteddialog" in subtype:
                            audio.boosted = True
                            audio.bitrate = 1
                    else:
                        try:
                            subtype = audio.extra[0].get("audioTrackSubtype", "") if len(audio.extra) > 0 else ""
                            audio.descriptive = (subtype == "descriptive")
                            if "boosteddialog" in subtype:
                                audio.boosted = True
                                audio.bitrate = 1
                        except (IndexError, AttributeError):
                            pass

        # Skip separate audio for AV1 (fast path)
        if self.vcodec == "AV1":
            self.log.debug(" + AV1 mode: skipping separate audio manifest")
            need_separate_audio = False
        else:
            need_separate_audio = ((self.aquality or self.vquality) != self.vquality
                                   or self.amanifest == "CVBR" and (self.vcodec, self.bitrate) != ("H264", "CVBR")
                                   or self.amanifest == "CBR" and (self.vcodec, self.bitrate) != ("H264", "CBR")
                                   or self.amanifest == "H265" and self.vcodec != "H265"
                                   or self.amanifest != "H265" and self.vcodec == "H265")

            if not need_separate_audio:
                audios = defaultdict(list)
                for audio in tracks.audios:
                    audios[audio.language].append(audio)

                for lang in audios:
                    if not any((x.bitrate or 0) >= 640000 for x in audios[lang]):
                        need_separate_audio = True
                        break

        if need_separate_audio and not self.atmos:
            manifest_type = self.amanifest or "CVBR"
            self.log.info(f"Getting audio from {manifest_type} manifest for potential higher bitrate or better codec")
            audio_manifest = self.get_manifest(
                title=title,
                video_codec="H265" if manifest_type == "H265" else "H264",
                bitrate_mode="CVBR",
                quality=self.aquality or self.vquality,
                hdr=None,
                ignore_errors=True
            )
            if not audio_manifest:
                self.log.warning(f" - Unable to get {manifest_type} audio manifests, skipping")
            elif not (chosen_audio_manifest := self.choose_manifest(audio_manifest, self.cdn)):
                self.log.warning(f" - No {manifest_type} audio manifests available, skipping")
            else:
                audio_mpd_url = self.clean_mpd_url(chosen_audio_manifest["url"], optimise=False)

                self.log.info(" + Downloading CVBR manifest")
                
                self.log.debug(audio_mpd_url)
                if self.event:
                    devicetype = self.device["device_type"]
                    audio_mpd_url = chosen_audio_manifest["url"]
                    audio_mpd_url = f"{audio_mpd_url}?amznDtid={devicetype}&encoding=segmentBase"
                self.log.info(f" + {manifest_type} Audio Manifest URL: {audio_mpd_url}")

                streamingProtocol = audio_manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
                sessionHandoffToken = audio_manifest["sessionization"]["sessionHandoffToken"]

                try:
                    if streamingProtocol == "DASH":
                        audio_tracks = Tracks.from_mpd(
                            url=audio_mpd_url,
                            session=self.session,
                            source=self.ALIASES[0],
                        )
                        # Apply language map to audio tracks
                        audio_mpd_raw = self.session.get(audio_mpd_url).text
                        audio_lang_map = self._build_ordered_lang_map_from_mpd(audio_mpd_raw)
                        if audio_lang_map:
                            self._apply_ordered_lang_map(audio_tracks.audios, audio_lang_map)
                        
                        for track in audio_tracks:
                            if isinstance(track.extra, dict):
                                track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                            elif isinstance(track.extra, tuple):
                                track.extra = track.extra + (sessionHandoffToken,)
                            else:
                                track.extra = (sessionHandoffToken,)
                        tracks.add(audio_tracks.audios, warn_only=True)
                        
                    elif streamingProtocol == "SmoothStreaming":
                        audio_tracks = Tracks.from_ism(
                            url=audio_mpd_url,
                            source=self.ALIASES[0],
                        )
                        for track in audio_tracks:
                            if isinstance(track.extra, dict):
                                track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                            elif isinstance(track.extra, tuple):
                                track.extra = track.extra + (sessionHandoffToken,)
                            else:
                                track.extra = (sessionHandoffToken,)
                        tracks.add(audio_tracks.audios, warn_only=True)
                        
                except KeyError:
                    self.log.warning(f" - Title has no {self.amanifest} stream, cannot get higher quality audio")

        # Fetch Atmos audio from DV/UHD manifest (576kbps DD+)
        need_uhd_audio = self.atmos

        if not self.amanifest and ((self.aquality == "UHD" and self.vquality != "UHD") or not self.aquality):
            audios = defaultdict(list)
            for audio in tracks.audios:
                audios[audio.language].append(audio)
            for lang in audios:
                if not any((x.bitrate or 0) >= 640000 for x in audios[lang]):
                    need_uhd_audio = True
                    break

        if need_uhd_audio and (self.config.get("device") or {}).get(self.profile, None):
            self.log.info("Getting audio from UHD manifest for potential higher bitrate or better codec")
            temp_device = self.device
            temp_device_token = self.device_token
            temp_device_id = self.device_id
            uhd_audio_manifest = None

            try:
                # Ensure we have a valid device token for UHD manifest
                if self.cdm.device.type in [LocalDevice.Types.CHROME, LocalDevice.Types.PLAYREADY] and self.quality < 2160:
                    self.log.info(f" + Switching to device to get UHD manifest")
                    self.register_device()

                uhd_audio_manifest = self.get_manifest(
                    title=title,
                    video_codec="H265",
                    bitrate_mode="CVBR+CBR",
                    quality="UHD",
                    hdr="DV",  # DV manifest required for 576kbps Atmos track
                    ignore_errors=True
                )
            except:
                pass

            self.device = temp_device
            self.device_token = temp_device_token
            self.device_id = temp_device_id

            if not uhd_audio_manifest or not uhd_audio_manifest.get("vodPlaybackUrls"):
                self.log.warning(f" - Unable to get UHD manifests, skipping")
            elif not (chosen_uhd_audio_manifest := self.choose_manifest(uhd_audio_manifest, self.cdn)):
                self.log.warning(f" - No UHD manifests available, skipping")
            else:
                uhd_audio_mpd_url = self.clean_mpd_url(chosen_uhd_audio_manifest["url"], optimise=False)
                
                self.log.info(f" + UHD Audio Manifest URL: {uhd_audio_mpd_url}")
                
                self.log.debug(uhd_audio_mpd_url)
                if self.event:
                    devicetype = self.device["device_type"]
                    uhd_audio_mpd_url = chosen_uhd_audio_manifest["url"]
                    uhd_audio_mpd_url = f"{uhd_audio_mpd_url}?amznDtid={devicetype}&encoding=segmentBase"
                self.log.info(" + Downloading UHD manifest")

                streamingProtocol = uhd_audio_manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
                sessionHandoffToken = uhd_audio_manifest["sessionization"]["sessionHandoffToken"]

                try:
                    if streamingProtocol == "DASH":
                        uhd_audio_tracks = Tracks.from_mpd(
                            url=uhd_audio_mpd_url,
                            session=self.session,
                            source=self.ALIASES[0],
                        )
                        # Apply language map to UHD audio tracks
                        uhd_mpd_raw = self.session.get(uhd_audio_mpd_url).text
                        uhd_lang_map = self._build_ordered_lang_map_from_mpd(uhd_mpd_raw)
                        if uhd_lang_map:
                            self._apply_ordered_lang_map(uhd_audio_tracks.audios, uhd_lang_map)
                            self.log.info(f" + UHD audio lang map: {sum(len(v) for v in uhd_lang_map.values())} entries")
                        
                        for track in uhd_audio_tracks:
                            if isinstance(track.extra, dict):
                                track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                            elif isinstance(track.extra, tuple):
                                track.extra = track.extra + (sessionHandoffToken,)
                            else:
                                track.extra = (sessionHandoffToken,)
                                
                    elif streamingProtocol == "SmoothStreaming":
                        uhd_audio_tracks = Tracks.from_ism(
                            url=uhd_audio_mpd_url,
                            source=self.ALIASES[0],
                        )
                        for track in uhd_audio_tracks:
                            if isinstance(track.extra, dict):
                                track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                            elif isinstance(track.extra, tuple):
                                track.extra = track.extra + (sessionHandoffToken,)
                            else:
                                track.extra = (sessionHandoffToken,)
                                
                except KeyError:
                    self.log.warning(f" - Title has no UHD stream, cannot get higher quality audio")
                else:
                    # Report Atmos/high-bitrate tracks
                    atmos_tracks = [x for x in uhd_audio_tracks.audios if (x.bitrate or 0) >= 448000 and (x.channels or 0) >= 6]
                    if atmos_tracks:
                        best_kbps = max((x.bitrate or 0) for x in atmos_tracks) // 1000
                        self.log.info(f" + Added {len(atmos_tracks)} Atmos/high-bitrate audio track(s) from UHD manifest (best: {best_kbps} kb/s)")
                        tracks.add(uhd_audio_tracks.audios, warn_only=True)
                    else:
                        self.log.info(" + UHD audio manifest fetched (no Atmos tracks found)")

        # Post-process audio tracks
        for audio in tracks.audios:
            if audio.descriptor == audio.descriptor.MPD:
                if audio.extra and len(audio.extra) > 1:
                    if isinstance(audio.extra, dict):
                        audio_track_id = audio.extra.get("audioTrackId")
                    else:
                        try:
                            audio_track_id = audio.extra[1].get("audioTrackId") if len(audio.extra) > 1 else None
                        except (IndexError, AttributeError):
                            audio_track_id = None
                    
                    if audio_track_id:
                        audio.language = Language.get(audio_track_id.split("_")[0])
                        
            elif audio.descriptor == audio.descriptor.ISM:
                if audio.extra and len(audio.extra) > 0:
                    if isinstance(audio.extra, dict):
                        audio_track_id = audio.extra.get("audioTrackId")
                    else:
                        try:
                            audio_track_id = audio.extra[0].get("audioTrackId") if len(audio.extra) > 0 else None
                        except (IndexError, AttributeError):
                            audio_track_id = None
                    
                    if audio_track_id:
                        audio.language = Language.get(audio_track_id.split("_")[0])
        
        # Deduplicate audio tracks
        unique_audio_tracks = {}
        for audio in tracks.audios:
            key = (audio.language, audio.bitrate, audio.descriptive, getattr(audio, 'boosted', False))
            if key not in unique_audio_tracks:
                unique_audio_tracks[key] = audio
        tracks.audios = list(unique_audio_tracks.values())   
        
        # Subtitles with language resolution
        if not hybrid_mode:
            manifest_for_subs = manifest if not hybrid_mode else manifest_hdr
            for sub in manifest_for_subs.get("timedTextUrls", {}).get("result", {}).get("subtitleUrls", []) + \
                       manifest_for_subs.get("timedTextUrls", {}).get("result", {}).get("forcedNarrativeUrls", []):
                
                url = sub["url"]
                url_path = url.split("?")[0]
                url_ext = os.path.splitext(url_path)[1].lstrip(".").lower()
                
                codec_map = {
                    "ttml": "ttml",
                    "dfxp": "ttml",
                    "vtt": "vtt",
                    "srt": "srt",
                }
                
                tracks.add(TextTrack(
                    id_=f"{sub['trackGroupId']}_{sub['languageCode']}_{sub['type']}_{sub['subtype']}",
                    source=self.ALIASES[0],
                    url=url,
                    codec=codec_map.get(url_ext, "ttml"),
                    language=self._resolve_subtitle_language(sub["languageCode"], url),
                    forced="ForcedNarrative" in sub["type"],
                    sdh=sub["type"].lower() == "sdh"
                ), warn_only=True)
        
        for track in tracks:
            track.needs_proxy = False

        if self.vquality != "UHD" and not self.no_true_region and tracks.videos:
            self.manage_session(tracks.videos[0])

        return tracks

    def get_chapters(self, title: Title) -> list[MenuTrack]:
        """Get chapters from Amazon's XRay Scenes API."""
        return []

    def certificate(self, **_):
        return self.config["certificate"]
        
    def license(self, challenge: bytes, title: Title, track: Track, *_, **__) -> Union[bytes, str, dict, None]:
        if self.playready:
            return self.get_playready_license(challenge, title, track)
        else:
            return self.get_widevine_license(challenge, title, track)

    def get_widevine_license(self, challenge: bytes, title: Title, track: Track, **_) -> bytes:
        return self._get_license(challenge, title, track, widevine=True)

    def get_playready_license(self, challenge: bytes, title: Title, track: Track, **_) -> bytes:
        return self._get_license(challenge, title, track, widevine=False)

    def _get_license(self, challenge: bytes, title: Title, track: Track, widevine: bool) -> bytes:
        if (isinstance(self.cdm, (RemoteDevice, LocalDevice))
            and challenge != self.cdm.service_certificate_challenge):
            self.register_device(quality=track.quality or getattr(track, "height", None))

        if widevine:
            license_type_key = "widevineLicense"
            license_endpoint = "license_wv"
            other_params = {"includeHdcpTestKey": True}
        else:
            license_type_key = "playReadyLicense"
            license_endpoint = "license_pr"
            other_params = {}

        request_json = {
            **other_params,
            "licenseChallenge": base64.b64encode(challenge).decode(),
            "playbackEnvelope": self.playbackInfo["playbackExperienceMetadata"]["playbackEnvelope"],
        }

        if self.device_token:
            request_json.update({
                "capabilityDiscriminators": {
                    "discriminators": {
                        "hardware": {
                            "chipset": self.device.get("device_chipset", ""),
                            "manufacturer": self.device.get("manufacturer", ""),
                            "modelName": self.device.get("device_model", ""),
                        },
                        "software": {
                            "application": {
                                "name": self.device.get("app_name", ""),
                                "version": self.device.get("firmware", ""),
                            },
                            "client": {"id": None},
                            **( {"firmware": {"version": str(self.device.get("firmware_version", ""))}} if self.device.get("firmware_version") else {} ),
                            "operatingSystem": {"name": "Android", "version": self.device.get("os_version", "")},
                            "player": {"name": "Android UIPlayer SDK", "version": "4.1.18"},
                            "renderer": {"drmScheme": "WIDEVINE" if widevine else "PLAYREADY", "name": "MCMD"},
                        },
                    },
                    "version": 1,
                },
                "deviceCapabilityFamily": "AndroidPlayer",
                "keyId": str(uuid.UUID(track.kid)).upper(),
                "packagingFormat": "SMOOTH_STREAMING" if track.descriptor == Track.Descriptor.ISM else "MPEG_DASH",
            })
        else:
            try:
                if isinstance(track.extra, dict):
                    session_handoff = track.extra.get("session_handoff")
                else:
                    session_handoff = track.extra[2] if len(track.extra) > 2 else None
            except (IndexError, AttributeError, TypeError):
                session_handoff = None
            
            if not session_handoff:
                self.log.exit("No sessionHandoff found in track data. Web licensing requires sessionHandoff.")
            
            request_json.update({
                "sessionHandoff": session_handoff,
                "deviceCapabilityFamily": "WebPlayer",
            })

        try:
            res = self.session.post(
                url=self.endpoints[license_endpoint],
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json; charset=utf-8",
                    "Authorization": f"Bearer {self.device_token}" if self.device_token else None,
                    "connection": "Keep-Alive",
                    "x-gasc-enabled": "true",
                    "x-request-priority": "CRITICAL",
                    "x-retry-count": "0",
                    "nerid": self.generate_nerid(),
                },
                params={
                    "deviceID": self.device_id,
                    "deviceTypeID": self.device.get("device_type", self.config["device_types"]["browser"]),
                    "gascEnabled": str(self.pv).lower(),
                    "marketplaceID": self.region["marketplace_id"],
                    "uxLocale": "en_EN",
                    "firmware": "1",
                    "titleId": title.id,
                    "nerid": self.generate_nerid(),
                },
                json=request_json,
            )
            res.raise_for_status()
            response_data = res.json()
        except requests.exceptions.HTTPError as e:
            msg = "Failed to license"
            if e.response is not None:
                try:
                    res_json = e.response.json()
                    msg += f": {res_json}"
                except Exception:
                    msg += f": {e.response.text}"
            else:
                msg += f": {str(e)}"
            self.log.exit(msg)
        except Exception as e:
            self.log.exit(f"Failed to license: {str(e)}")

        if "errorsByResource" in response_data:
            error = response_data["errorsByResource"]
            error_code = error.get("errorCode", error.get("type", "Unknown"))
            if error_code == "PRS.NoRights.AnonymizerIP":
                self.log.error(" - Amazon detected a Proxy/VPN and refused to return a license!")
                raise SystemExit(1)
            self.log.error(f" - Amazon reported an error during the License request: [{error_code}]")
            raise SystemExit(1)
        
        if "error" in response_data:
            self.log.error(f" - License Error: {response_data['error'].get('message', 'Unknown error')}")
            raise SystemExit(1)

        license_b64 = response_data.get(license_type_key, {}).get("license")
        if not license_b64:
            self.log.error(f" - No license found in response: {response_data}")
            raise SystemExit(1)
            
        if isinstance(license_b64, str):
            return base64.b64decode(license_b64)
        return license_b64

    def configure(self) -> None:
        if len(self.title) > 10:
            self.pv = True

        self.log.info("Getting Account Region")
        self.region = self.get_region()
        if not self.region:
            raise self.log.exit(" - Failed to get Amazon Account region")
        self.GEOFENCE.append(self.region["code"])
        
        if self.no_true_region:
            self.log.info(f" + Region: {self.region['code']}")

        self.endpoints = self.prepare_endpoints(self.config["endpoints"], self.region)

        self.session.headers.update({
            "Origin": f"https://{self.region['base']}",
            "Referer": f"https://{self.region['base']}/"
        })

        self.device = (self.config.get("device") or {}).get(self.profile, {})
        
        # Check dtid_dict safety - prevents account bans
        dtid_dict = self.config.get("dtid_dict", [])
        if self.device and dtid_dict:
            if self.device.get("device_type") not in set(dtid_dict):
                raise self.log.exit(f"Device type '{self.device.get('device_type')}' is NOT in approved dtid_dict. Using it could result in an Amazon account ban. Update your config.")
        
        # Determine if we need device registration for UHD
        need_device = False
        if (isinstance(self.quality, list) and self.quality[0] > 1080) or self.vquality == "UHD" or self.range != "SDR":
            need_device = True
        if self.vcodec == "H265" and self.vquality == "UHD":
            need_device = True
        
        if need_device and self.device:
            self.log.info(f"Using device profile: {self.profile}")
            # Call configuration endpoint to verify actual marketplace_id
            res_cfg = self.session.get(
                url=self.endpoints["configuration"],
                params={"deviceTypeID": self.device["device_type"], "deviceID": "Tv"}
            )
            if res_cfg.status_code == 200:
                cfg_data = res_cfg.json()
                territory = cfg_data.get("requestContext", {}).get("currentTerritory")
                if territory:
                    self.log.info(f" + Current Region (verified): {territory}")
                marketplace = cfg_data.get("requestContext", {}).get("marketplaceID")
                if marketplace and not self.no_true_region:
                    self.region["marketplace_id"] = marketplace
            else:
                self.log.warning(f" - Configuration endpoint returned {res_cfg.status_code}, using config values")
            self.register_device()
        else:
            if not self.device:
                self.log.warning("No Device information was provided for %s, using browser device...", self.profile)
            self.device_id = hashlib.sha224(("CustomerID" + self.session.headers["User-Agent"]).encode("utf-8")).hexdigest()
            self.device = {"device_type": self.config["device_types"]["browser"]}
            # Also call configuration for browser to get real marketplace_id
            res_cfg = self.session.get(
                url=self.endpoints["configuration"],
                params={"deviceTypeID": self.device["device_type"], "deviceID": "Web"}
            )
            if res_cfg.status_code == 200:
                cfg_data = res_cfg.json()
                marketplace = cfg_data.get("requestContext", {}).get("marketplaceID")
                if marketplace and not self.no_true_region:
                    self.region["marketplace_id"] = marketplace

    def register_device(self) -> None:
        self.device = (self.config.get("device") or {}).get(self.profile, {})
        
        # Unique device identity per installation
        import hashlib
        from pathlib import Path
        import jsonpickle
        
        identity_cache_dir = Path(self.get_cache(""))
        identity_cache_file = identity_cache_dir / f"device_identity_{self.profile}.json"
        identity_cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        identity = None
        if identity_cache_file.exists():
            try:
                with open(identity_cache_file, "r", encoding="utf-8") as f:
                    identity = jsonpickle.decode(f.read())
                    self.log.debug(" + Using cached device identity")
            except Exception:
                pass
        
        if not identity:
            unique_serial = secrets.token_hex(8)
            base_name = self.device.get("device_name", "%FIRST_NAME%'s Shield TV")
            clean_name = re.sub(r"%DUPE_STRATEGY[^%]*%", "", base_name).rstrip()
            suffix = secrets.token_hex(2).upper()
            unique_name = f"{clean_name}-{suffix}"
            identity = {"device_serial": unique_serial, "device_name": unique_name}
            with open(identity_cache_file, "w", encoding="utf-8") as f:
                f.write(jsonpickle.encode(identity))
            self.log.info(f" + Generated unique device identity: serial={unique_serial}, name={unique_name!r}")
        
        self.device["device_serial"] = identity["device_serial"]
        self.device["device_name"] = identity["device_name"]
        
        device_cache_path = f"device_tokens_{self.profile}_{hashlib.md5(json.dumps(self.device, sort_keys=True).encode()).hexdigest()[0:6]}.json"
        
        registration = self.DeviceRegistration(
            device=self.device,
            endpoints=self.endpoints,
            log=self.log,
            cache_path=device_cache_path,
            session=self.session,
            base_service=self
        )
        self.device_token = registration.bearer
        self.device_id = self.device.get("device_serial")
        if not self.device_id:
            raise self.log.exit(f" - A device serial is required in the config, perhaps use: {os.urandom(8).hex()}")

    def get_region(self) -> dict:
        domain_region = self.get_domain_region()
        if not domain_region:
            return {}

        region = self.config["regions"].get(domain_region)
        if not region:
            raise self.log.exit(f" - There's no region configuration data for the region: {domain_region}")

        region["code"] = domain_region

        if self.pv:
            res = self.session.get("https://www.primevideo.com").text
            match = re.search(r'ue_furl *= *([\'"])fls-(na|eu|fe)\.amazon\.[a-z.]+\1', res)
            if match:
                pv_region = match.group(2).lower()
            else:
                self.log.error(" - Failed to get PrimeVideo region")
                raise SystemExit(1)
            pv_region = {"na": "atv-ps"}.get(pv_region, f"atv-ps-{pv_region}")
            region["base_manifest"] = f"{pv_region}.primevideo.com"
            region["base"] = "www.primevideo.com"

        return region

    def get_domain_region(self):
        tlds = [tldextract.extract(x.domain) for x in self.cookies if x.domain_specified]
        tld = next((x.suffix for x in tlds if x.domain.lower() in ("amazon", "primevideo")), None)
        self.domain = next((x.domain for x in tlds if x.domain.lower() in ("amazon", "primevideo")), None).lower()
        if tld:
            tld = tld.split(".")[-1]
        return {"com": "us", "uk": "gb"}.get(tld, tld)

    def prepare_endpoint(self, name: str, uri: str, region: dict) -> str:
        # Playback, license, session and configuration endpoints go to the manifest CDN host
        if name in ("configuration", "refreshplayback", "playback", "license_wv", "license_pr", "xray", "opensession", "updatesession", "closesession"):
            return f"https://{region['base_manifest']}{uri}"
        # UI / metadata endpoints go to primevideo.com (or regional base)
        if name in ("ontv", "devicelink", "details", "getDetailWidgets", "metadata"):
            if self.pv:
                host = "www.primevideo.com"
            else:
                if name in ("metadata"):
                    host = f"{region['base']}/gp/video"
                else:
                    host = region["base"]
            return f"https://{host}{uri}"
        # Auth endpoints go to the regional API host
        if name in ("codepair", "register", "token"):
            base_api = region.get("base_api") or self.config["regions"]["us"]["base_api"]
            return f"https://{base_api}{uri}"
        raise ValueError(f"Unknown endpoint: {name}")

    def prepare_endpoints(self, endpoints: dict, region: dict) -> dict:
        return {k: self.prepare_endpoint(k, v, region) for k, v in endpoints.items()}

    def choose_manifest(self, manifest: dict, cdn=None):
        """Get manifest URL for the title based on CDN weight (or specified CDN)."""
        if not manifest:
            self.log.warning("Empty manifest provided to choose_manifest")
            return {}
        
        if "vodPlaybackUrls" not in manifest:
            self.log.warning("Manifest missing vodPlaybackUrls key")
            return {}
        
        if "result" not in manifest["vodPlaybackUrls"]:
            self.log.warning("Manifest missing result in vodPlaybackUrls")
            return {}
        
        if cdn:
            cdn = cdn.lower()
            try:
                manifest = next((x for x in manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlSets"] if x["cdn"].lower() == cdn), {})
            except (KeyError, TypeError):
                self.log.warning(f"Unable to parse urlSets for CDN {cdn}")
                return {}
            if not manifest:
                raise self.log.exit(f" - There isn't any DASH manifests available on the CDN \"{cdn}\" for this title")
        else:
            url_sets = manifest["vodPlaybackUrls"]["result"]["playbackUrls"].get("urlSets", [])
            manifest = random.choice(url_sets) if url_sets else {}

        return manifest
    
    def manage_session(self, track: Tracks):
        try:
            current_progress_time = round(random.uniform(0, 10), 6)
            time_ = 3

            stream_update_time = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
            
            if isinstance(track.extra, dict):
                session_handoff = track.extra.get("session_handoff")
            else:
                session_handoff = track.extra[2] if len(track.extra) > 2 else None
                
            if not session_handoff:
                self.log.warning("No session handoff found, skipping session management")
                return
                
            res = self.session.post(
                url=self.endpoints["opensession"],
                params={
                    "deviceID": self.device_id,
                    "deviceTypeID": self.device["device_type"],
                    "gascEnabled": str(self.pv).lower(),
                    "marketplaceID": self.region["marketplace_id"],
                    "uxLocale": "en_EN",
                    "firmware": "1",
                    "version": "1",
                    "nerid": self.generate_nerid(),
                },
                headers={
                    "Content-Type": "application/json",
                    "accept": "application/json",
                    "x-request-priority": "CRITICAL",
                    "x-retry-count": "0"
                },
                json={
                    "sessionHandoff": session_handoff,
                    "playbackEnvelope": self.playbackEnvelope_update(self.playbackInfo)["playbackExperienceMetadata"]["playbackEnvelope"],
                    "streamInfo": {
                        "eventType": "START",
                        "streamUpdateTime": current_progress_time,
                        "vodProgressInfo": {
                            "currentProgressTime": f"PT{current_progress_time:.6f}S",
                            "timeFormat": "ISO8601DURATION",
                        },
                    },
                    "userWatchSessionId": str(uuid.uuid4())
                }
            )
            if res.status_code == 200:
                try:
                    data = res.json()
                    sessionToken = data["sessionToken"]
                except Exception as e:
                    raise self.log.exit(f"Unable to open session: {e}")
            else:
                raise self.log.exit(f"Unable to open session: {res.text}")
            
            time.sleep(time_)
            stream_update_time = (datetime.fromisoformat(stream_update_time[:-1]) + timedelta(seconds=time_)).isoformat(timespec="milliseconds") + "Z"
            res = self.session.post(
                url=self.endpoints["updatesession"],
                params={
                    "deviceID": self.device_id,
                    "deviceTypeID": self.device["device_type"],
                    "gascEnabled": str(self.pv).lower(),
                    "marketplaceID": self.region["marketplace_id"],
                    "uxLocale": "en_EN",
                    "firmware": "1",
                    "version": "1",
                    "nerid": self.generate_nerid()
                },
                headers={
                    "Content-Type": "application/json",
                    "accept": "application/json",
                    "x-request-priority": "CRITICAL",
                    "x-retry-count": "0"
                },
                json={
                    "sessionToken": sessionToken,
                    "streamInfo": {
                        "eventType": "PAUSE",
                        "streamUpdateTime": stream_update_time,
                        "vodProgressInfo": {
                            "currentProgressTime": f"PT{current_progress_time + time_:.6f}S",
                            "timeFormat": "ISO8601DURATION",
                        }
                    }
                }
            )
            if res.status_code == 200:
                try:
                    data = res.json()
                    sessionToken = data["sessionToken"]
                except Exception as e:
                    raise self.log.exit(f"Unable to update session: {e}")
            else:
                raise self.log.exit(f"Unable to update session: {res.text}")
            
            res = self.session.post(
                url=self.endpoints["closesession"],
                params={
                    "deviceID": self.device_id,
                    "deviceTypeID": self.device["device_type"],
                    "gascEnabled": str(self.pv).lower(),
                    "marketplaceID": self.region["marketplace_id"],
                    "uxLocale": "en_EN",
                    "firmware": "1",
                    "version": "1",
                    "nerid": self.generate_nerid()
                },
                headers={
                    "Content-Type": "application/json",
                    "accept": "application/json",
                    "x-request-priority": "CRITICAL",
                    "x-retry-count": "0"
                },
                json={
                    "sessionToken": sessionToken,
                    "streamInfo": {
                        "eventType": "STOP",
                        "streamUpdateTime": stream_update_time,
                        "vodProgressInfo": {
                            "currentProgressTime": f"PT{current_progress_time + time_:.6f}S",
                            "timeFormat": "ISO8601DURATION",
                        }
                    }
                }
            )
            if res.status_code == 200:
                self.log.info("Session completed successfully!")
                return None
            else:
                raise self.log.exit(f"Unable to close session: {res.text}")
        except Exception as e:
            raise self.log.exit(f"Unable to get session: {e}")

    def playbackEnvelope_data(self, titles):
        try:
            res = self.session.get(
                url=self.endpoints["metadata"],
                params={
                    "metadataToEnrich": json.dumps({"placement": "HOVER", "playback": "true", "preroll": "true", "trailer": "true", "watchlist": "true"}),
                    "titleIDsToEnrich": json.dumps(titles),
                    "currentUrl":  f"https://{self.region['base']}/"
                },
                headers={
                    "device-memory": "8",
                    "downlink": "10",
                    "dpr": "2",
                    "ect": "4g",
                    "rtt": "50",
                    "viewport-width": "671",
                    "x-amzn-client-ttl-seconds": "15",
                    "x-amzn-requestid": "".join(random.choices(string.ascii_uppercase + string.digits, k=20)).upper(),
                    "x-requested-with": "XMLHttpRequest"
                }
            )
            
            if res.status_code == 200:
                try:
                    data = res.json()
                    playbackEnvelope_info = []
                    enrichments = data["enrichments"]
                    
                    for titleid_, enrichment in list(enrichments.items()):
                        playbackActions = enrichment["playbackActions"]
                        if enrichment["entitlementCues"]['focusMessage'].get('message') == "Watch with a 30 day free Prime trial, auto renews at â‚¬4.99/month":
                            raise self.log.exit("Cookies Expired")
                        if playbackActions == []:
                            continue
                        for playbackAction in playbackActions:
                            if playbackAction.get("titleID") or playbackAction.get("legacyOfferASIN"):
                                title_id = titleid_
                                playbackExperienceMetadata = playbackAction.get("playbackExperienceMetadata")
                                if not title_id or not playbackExperienceMetadata:
                                    continue
                                playbackEnvelope_info.append({"titleID": title_id, "playbackExperienceMetadata": playbackExperienceMetadata})
                    return playbackEnvelope_info
                except Exception as e:             
                    raise self.log.exit(f"Unable to get playbackEnvelope: {e}")
            else:
                return []
        except Exception as e:
            return []
        
    def playbackEnvelope_update(self, playbackInfo):
        if not playbackInfo:
            self.log.warning("No playbackInfo available to update")
            return playbackInfo
    
        try:
            expiry = playbackInfo.get("playbackExperienceMetadata", {}).get("expiryTime", 0)
            if not expiry or (int(expiry) / 1000) < time.time():
                self.log.warning("Updating playbackEnvelope")
                correlationId = playbackInfo["playbackExperienceMetadata"].get("correlationId")
                titleID = playbackInfo.get("titleID")
    
                if not correlationId or not titleID:
                    return playbackInfo
    
                res = self.session.post(
                    url=self.endpoints["refreshplayback"],
                    params={
                        "deviceID": self.device_id,
                        "deviceTypeID": self.device["device_type"],
                        "gascEnabled": str(self.pv).lower(),
                        "marketplaceID": self.region["marketplace_id"],
                        "uxLocale": "en_EN",
                        "firmware": "1",
                        "version": "1",
                        "nerid": self.generate_nerid()
                    },
                    data=json.dumps({
                        "deviceId": self.device_id,
                        "deviceTypeId": self.device["device_type"],
                        "identifiers": {titleID: correlationId},
                        "geoToken": "null",
                        "identityContext": "null"
                    })
                )
    
                if res.status_code == 200:
                    try:
                        data = res.json()
                        response_data = data.get("response", {})
                        if not isinstance(response_data, dict):
                            raise ValueError("Invalid response structure")
    
                        title_data = response_data.get(titleID, {})
                        playbackExperience = title_data.get("playbackExperience")
    
                        if playbackExperience:
                            expiry_val = playbackExperience.get("expiryTime")
                            if expiry_val:
                                playbackExperience["expiryTime"] = int(expiry_val * 1000)
                            return {"titleID": titleID, "playbackExperienceMetadata": playbackExperience}
                        else:
                            self.log.warning("Refresh returned no playbackExperience, falling back to existing envelope")
                            return playbackInfo
                    except (ValueError, KeyError, TypeError) as e:
                        self.log.warning(f"Failed to parse refresh response: {e}, falling back to existing envelope")
                        return playbackInfo
                else:
                    self.log.warning(f"Refresh request failed ({res.status_code}), falling back to existing envelope")
                    return playbackInfo
            else:
                return playbackInfo
        except Exception as e:
            self.log.warning(f"Unexpected error during envelope update: {e}, falling back to existing envelope")
            return playbackInfo

    def get_manifest(self, title: Title, video_codec: str, bitrate_mode: str, quality: str, hdr=None, ignore_errors: bool = False, retries: int = 3) -> dict:
        for attempt in range(retries):
            try:
                # Force refresh playbackInfo on each retry attempt
                if attempt > 0:
                    self.log.info(f"Retry attempt {attempt + 1}/{retries} - refreshing playback envelope...")
                    if title.service_data.get("playbackInfo"):
                        title.service_data["playbackInfo"]["playbackExperienceMetadata"]["expiryTime"] = 0
                
                self.playbackInfo = self.playbackEnvelope_update(title.service_data.get("playbackInfo"))
                title.service_data["playbackInfo"] = self.playbackInfo
                
                data_dict = {
                    "globalParameters": {
                        "deviceCapabilityFamily": "WebPlayer" if not self.device_token else "AndroidPlayer",
                        "playbackEnvelope": self.playbackInfo["playbackExperienceMetadata"]["playbackEnvelope"],
                        "capabilityDiscriminators": {
                            "operatingSystem": {"name": "Windows", "version": "10.0"},
                            "middleware": {"name": "EdgeNext", "version": "136.0.0.0"},
                            "nativeApplication": {"name": "EdgeNext", "version": "136.0.0.0"},
                            "hfrControlMode": "Legacy",
                            "displayResolution": {"height": 2304, "width": 4096}
                        } if not self.device_token else {
                            "discriminators": {"software": {}, "version": 1}
                        }
                    },
                    "auditPingsRequest": {
                        **({"device": {"category": "Tv", "platform": "Android"}} if self.device_token else {})
                    },
                    "playbackDataRequest": {},
                    "timedTextUrlsRequest": {"supportedTimedTextFormats": ["TTMLv2", "DFXP"]},
                    "trickplayUrlsRequest": {},
                    "transitionTimecodesRequest": {},
                    "vodPlaybackUrlsRequest": {
                        "device": {
                            "hdcpLevel": "2.2" if quality == "UHD" else "1.4",
                            "maxVideoResolution": "1080p" if quality == "HD" else "480p" if quality == "SD" else "2160p",
                            "supportedStreamingTechnologies": ["DASH"],
                            "streamingTechnologies": {
                                "DASH": {
                                    "bitrateAdaptations": ["CVBR", "CBR"] if bitrate_mode in ("CVBR+CBR", "CVBR,CBR") else [bitrate_mode],
                                    "codecs": ["AV1", "H265", "H264"] if video_codec == "AV1" else [video_codec],
                                    "drmKeyScheme": "SingleKey" if self.playready else "DualKey",
                                    "drmType": "PlayReady" if self.playready else "Widevine",
                                    "dynamicRangeFormats": self.VIDEO_RANGE_MAP.get(hdr, "None"),
                                    "fragmentRepresentations": ["ByteOffsetRange", "SeparateFile"],
                                    "frameRates": ["Standard"],
                                    "segmentInfoType": "Base",
                                    "timedTextRepresentations": ["NotInManifestNorStream", "SeparateStreamInManifest"],
                                    "trickplayRepresentations": ["NotInManifestNorStream"],
                                    "variableAspectRatio": "supported"
                                }
                            },
                            "displayWidth": 4096,
                            "displayHeight": 2304
                        },
                        "ads": {"sitePageUrl": "", "gdpr": {"enabled": "false", "consentMap": {}}},
                        "playbackCustomizations": {},
                        "playbackSettingsRequest": {"firmware": "UNKNOWN", "playerType": self.player, "responseFormatVersion": "1.0.0", "titleId": title.id}
                    } if not self.device_token else {
                        "ads": {},
                        "device": {
                            "displayBasedVending": "supported",
                            "displayHeight": 2304,
                            "displayWidth": 4096,
                            "streamingTechnologies": {
                                "DASH": {
                                    "fragmentRepresentations": ["ByteOffsetRange", "SeparateFile"],
                                    "manifestThinningToSupportedResolution": "Forbidden",
                                    "segmentInfoType": "List",
                                    "timedTextRepresentations": ["BurnedIn", "NotInManifestNorStream", "SeparateStreamInManifest"],
                                    "trickplayRepresentations": ["NotInManifestNorStream"],
                                    "variableAspectRatio": "supported",
                                    "vastTimelineType": "Absolute",
                                    "bitrateAdaptations": ["CVBR", "CBR"] if bitrate_mode in ("CVBR+CBR", "CVBR,CBR") else [bitrate_mode],
                                    "codecs": ["AV1", "H265", "H264"] if video_codec == "AV1" else [video_codec],
                                    "drmKeyScheme": "SingleKey",
                                    "drmStrength": "L40",
                                    "drmType": "PlayReady" if self.playready else "Widevine",
                                    "dynamicRangeFormats": [self.VIDEO_RANGE_MAP.get(hdr, "None")],
                                    "frameRates": ["Standard"]
                                },
                                "SmoothStreaming": {
                                    "fragmentRepresentations": ["ByteOffsetRange", "SeparateFile"],
                                    "manifestThinningToSupportedResolution": "Forbidden",
                                    "segmentInfoType": "List",
                                    "timedTextRepresentations": ["BurnedIn", "NotInManifestNorStream", "SeparateStreamInManifest"],
                                    "trickplayRepresentations": ["NotInManifestNorStream"],
                                    "variableAspectRatio": "supported",
                                    "vastTimelineType": "Absolute",
                                    "bitrateAdaptations": ["CVBR", "CBR"] if bitrate_mode in ("CVBR+CBR", "CVBR,CBR") else [bitrate_mode],
                                    "codecs": ["AV1", "H265", "H264"] if video_codec == "AV1" else [video_codec],
                                    "drmKeyScheme": "SingleKey",
                                    "drmStrength": "L40",
                                    "drmType": "PlayReady",
                                    "dynamicRangeFormats": [self.VIDEO_RANGE_MAP.get(hdr, "None")],
                                    "frameRates": ["Standard"]
                                }
                            },
                            "acceptedCreativeApis": [],
                            "category": "Tv",
                            "hdcpLevel": "2.2",
                            "maxVideoResolution": "2160p",
                            "platform": "Android",
                            "supportedStreamingTechnologies": ["DASH", "SmoothStreaming"]
                        },
                        "playbackCustomizations": {},
                        "playbackSettingsRequest": {"firmware": "UNKNOWN", "playerType": self.player, "responseFormatVersion": "1.0.0", "titleId": title.id}
                    },
                    "vodXrayMetadataRequest": {"xrayDeviceClass": "normal", "xrayPlaybackMode": "playback", "xrayToken": "XRAY_WEB_2023_V2"}
                }

                json_data = json.dumps(data_dict)

                res = self.session.post(
                    url=self.endpoints["playback"],
                    params={
                        "deviceID": self.device_id,
                        "deviceTypeID": self.device["device_type"],
                        "gascEnabled": str(self.pv).lower(),
                        "marketplaceID": self.region["marketplace_id"],
                        "uxLocale": "en_EN",
                        "firmware": "1",
                        "titleId": title.id,
                        "nerid": self.generate_nerid(),
                    },
                    data=json_data,
                    headers={
                        "Authorization": f"Bearer {self.device_token}" if self.device_token else None,
                    },
                )
                
                try:
                    manifest = res.json()
                except json.JSONDecodeError:
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        self.log.warning(f"JSON decode error, retrying in {wait_time}s... (attempt {attempt + 1}/{retries})")
                        time.sleep(wait_time)
                        continue
                    if ignore_errors:
                        return {}
                    raise self.log.exit(f" - Amazon reported an error when obtaining the Playback Manifest\n{res.text}")

                # Check for AV1 availability
                if video_codec == "AV1" and "error" in manifest.get("vodPlaybackUrls", {}):
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        self.log.warning(f"AV1 manifest not available, retrying...")
                        time.sleep(wait_time)
                        continue
                    if ignore_errors:
                        return {}
                    self.log.warning(f" - AV1 manifest not available: {manifest['vodPlaybackUrls']['error'].get('message', 'unknown error')}")
                    return {}

                if "error" in manifest.get("vodPlaybackUrls", {}):
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        self.log.warning(f"Manifest error: {manifest['vodPlaybackUrls']['error'].get('message', 'Unknown')}, retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    if ignore_errors:
                        return {}
                    message = manifest["vodPlaybackUrls"]["error"]["message"]
                    raise self.log.exit(f" - Amazon reported an error when obtaining the Playback Manifest: {message}")

                if (manifest.get("errorsByResource", {}).get("PlaybackUrls") and
                    manifest["errorsByResource"]["PlaybackUrls"].get("errorCode") != "PRS.NoRights.NotOwned"):
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        error = manifest["errorsByResource"]["PlaybackUrls"]
                        self.log.warning(f"PlaybackUrls error: {error.get('message', 'Unknown')}, retrying...")
                        time.sleep(wait_time)
                        continue
                    if ignore_errors:
                        return {}
                    error = manifest["errorsByResource"]["PlaybackUrls"]
                    raise self.log.exit(f" - Amazon had an error with the Playback Urls: {error['message']} [{error['errorCode']}]")

                if (manifest.get("errorsByResource", {}).get("AudioVideoUrls") and
                    manifest["errorsByResource"]["AudioVideoUrls"].get("errorCode") != "PRS.NoRights.NotOwned"):
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt
                        error = manifest["errorsByResource"]["AudioVideoUrls"]
                        self.log.warning(f"AudioVideoUrls error: {error.get('message', 'Unknown')}, retrying...")
                        time.sleep(wait_time)
                        continue
                    if ignore_errors:
                        return {}
                    error = manifest["errorsByResource"]["AudioVideoUrls"]
                    raise self.log.exit(f" - Amazon had an error with the A/V Urls: {error['message']} [{error['errorCode']}]")

                return manifest
                
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    self.log.warning(f"Unexpected error: {e}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise
        
        return {}
    
    def get_episodes(self, titleID):
        titles = []
        titles_ = []
        res = self.session.get(
            url=self.endpoints["details"],
            params={
                "titleID": titleID,
                "isElcano": "1",
                "sections": ["Atf", "Btf"]
            },
            headers={"Accept": "application/json"}
        )

        if not res.ok:
            raise self.log.exit(f"Unable to get title: {res.text} [{res.status_code}]")

        data = res.json()["widgets"]
        seasons = [x.get("titleID") for x in data["seasonSelector"]]

        for season in seasons:
            res = self.session.get(
                url=self.endpoints["details"],
                params={"titleID": season, "isElcano": "1", "sections": "Btf"},
                headers={"Accept": "application/json"},
            ).json()["widgets"]

            try:
                episode_list_data = res.get("episodeList", {})
                episodes = episode_list_data.get("episodes", [])
            except:
                continue

            product_details = res.get("productDetails", {}).get("detail", {})
            season_number = product_details.get("seasonNumber", 1)

            batch = []
            episodes_titles = []
            for episode in episodes:
                details = episode["detail"]
                episodes_titles.append(details["catalogId"])
                batch.append(Title(
                    id_=details["catalogId"],
                    type_=Title.Types.TV,
                    name=product_details["parentTitle"],
                    season=season_number,
                    episode=episode["self"]["sequenceNumber"],
                    episode_name=details["title"],
                    original_lang=None,
                    source=self.ALIASES[0],
                    service_data=details,
                ))

            playbackEnvelope_info = self.playbackEnvelope_data(episodes_titles)
            for title in batch:
                for playbackInfo in playbackEnvelope_info:
                    if title.id == playbackInfo["titleID"]:
                        title.service_data.update({"playbackInfo": playbackInfo})
                    titles_.append(title)

            pagination_data = episode_list_data.get('actions', {}).get('pagination', [])
            token = next((item.get('token') for item in pagination_data if item.get('tokenType') == 'NextPage'), None)
            
            page_count = 1
            while token:
                page_count += 1
                self.log.info(f" + Loading page {page_count} for season {season_number}...")
                
                res = self.session.get(
                    url=self.endpoints["getDetailWidgets"],
                    params={
                        "titleID": season,
                        "isTvodOnRow": "1",
                        "widgets": f'[{{"widgetType":"EpisodeList","widgetToken":"{quote(token)}"}}]'
                    },
                    headers={"Accept": "application/json"}
                )
                
                if not res.ok:
                    self.log.warning(f"Failed to get page {page_count}: {res.status_code}")
                    break
                    
                page_data = res.json()
                episodeList = page_data.get('widgets', {}).get('episodeList', {})
                page_episodes = episodeList.get('episodes', [])
                
                if not page_episodes:
                    break
                
                episodes_titles = []
                for item in page_episodes:
                    details = item["detail"]
                    episode_num = int(item.get('self', {}).get('sequenceNumber', 0))
                    episodes_titles.append(details["catalogId"])
                    titles.append(Title(
                        id_=details["catalogId"],
                        type_=Title.Types.TV,
                        name=product_details["parentTitle"],
                        season=season_number,
                        episode=episode_num,
                        episode_name=details["title"],
                        original_lang=None,
                        source=self.ALIASES[0],
                        service_data=item
                    ))
                
                playbackEnvelope_info = self.playbackEnvelope_data(episodes_titles)
                for title in titles:
                    for playbackInfo in playbackEnvelope_info:
                        if title.id == playbackInfo["titleID"]:
                            title.service_data.update({"playbackInfo": playbackInfo})
                            if title not in titles_:
                                titles_.append(title)
                
                pagination_data = episodeList.get('actions', {}).get('pagination', [])
                token = next((item.get('token') for item in pagination_data if item.get('tokenType') == 'NextPage'), None)

        return titles_

    @staticmethod
    def get_original_language(manifest):
        try:
            return next(x["language"].replace("_", "-") for x in manifest["catalogMetadata"]["playback"]["audioTracks"] if x["isOriginalLanguage"])
        except (KeyError, StopIteration):
            pass

        if "defaultAudioTrackId" in manifest.get("playbackUrls", {}):
            try:
                return manifest["playbackUrls"]["defaultAudioTrackId"].split("_")[0]
            except IndexError:
                pass

        try:
            return sorted(manifest["audioVideoUrls"]["audioTrackMetadata"], key=lambda x: x["index"])[0]["languageCode"]
        except (KeyError, IndexError):
            pass

        return None
    
    @staticmethod
    def generate_nerid(e=0):
        BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        timestamp = int(time.time() * 1000)
        ts_chars = []
        for _ in range(7):
            ts_chars.append(BASE64_CHARS[timestamp % 64])
            timestamp //= 64
        ts_part = ''.join(reversed(ts_chars))
        rand_part = ''.join(secrets.choice(BASE64_CHARS) for _ in range(15))
        suffix = f"{e % 100:02d}"
        return ts_part + rand_part + suffix

    @staticmethod
    def clean_mpd_url(mpd_url, optimise=False):
        if '@' in mpd_url:
            mpd_url = re.sub(r'/\d+@[^/]+', '', mpd_url, count=1)
        if optimise:
            return mpd_url.replace("~", "") + "?encoding=segmentBase"
        if match := re.match(r"(https?://.*/)d.?/.*~/(.*)", mpd_url):
            mpd_url = "".join(match.groups())
        else:
            try:
                mpd_url = "".join(re.split(r"(?i)(/)", mpd_url)[:5] + re.split(r"(?i)(/)", mpd_url)[9:])
            except IndexError:
                raise IndexError("Unable to parse MPD URL")
        return mpd_url
        
    def get_best_quality(self, title):
        """Choose the best quality manifest from CBR / CVBR"""
        tracks = Tracks()
        
        # AV1 fast path: only CVBR
        if self.vcodec == "AV1":
            bitrates = ["CVBR"]
            self.log.info(" + AV1 mode: using CVBR only")
        else:
            bitrates = [self.orig_bitrate]
            if self.vcodec != "H265":
                bitrates = self.orig_bitrate.split('+')
        
        for bitrate in bitrates:
            manifest = self.get_manifest(
                title,
                video_codec=self.vcodec,
                bitrate_mode=bitrate,
                quality=self.vquality,
                hdr=self.range,
                ignore_errors=True
            )

            if not manifest:
                self.log.warning(f"Skipping {bitrate} manifest due to empty response")
                continue
            
            if "vodPlaybackUrls" not in manifest:
                self.log.warning(f"Skipping {bitrate} manifest: missing vodPlaybackUrls")
                continue
            
            if "error" in manifest.get("vodPlaybackUrls", {}):
                error_msg = manifest["vodPlaybackUrls"]["error"].get("message", "Unknown error")
                self.log.warning(f"Skipping {bitrate} manifest: {error_msg}")
                continue
            
            try:
                bitrate_name = manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["bitrateAdaptation"]
            except (KeyError, TypeError) as e:
                self.log.warning(f"Skipping {bitrate} manifest: failed to parse metadata - {e}")
                continue

            chosen_manifest = self.choose_manifest(manifest, self.cdn)
            
            if not chosen_manifest:
                self.log.warning(f"No {bitrate_name} DASH manifests available")
                continue
            
            mpd_url = self.clean_mpd_url(chosen_manifest["url"], optimise=False)

            self.log.info(f" + Downloading {bitrate_name} MPD")
                        
            if self.event:
                devicetype = self.device["device_type"]
                mpd_url = chosen_manifest["url"]
                mpd_url = f"{mpd_url}?amznDtid={devicetype}&encoding=segmentBase"
            self.log.info(f" + {bitrate_name} Manifest URL: {mpd_url}")

            try:
                streamingProtocol = manifest["vodPlaybackUrls"]["result"]["playbackUrls"]["urlMetadata"]["streamingProtocol"]
                sessionHandoffToken = manifest["sessionization"]["sessionHandoffToken"]
            except (KeyError, TypeError) as e:
                self.log.warning(f"Skipping {bitrate_name} manifest: missing protocol info - {e}")
                continue

            if streamingProtocol == "DASH":
                mpd_tracks = Tracks.from_mpd(
                    url=mpd_url,
                    session=self.session,
                    source=self.ALIASES[0],
                )
                
                for track in mpd_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken,)
                    else:
                        track.extra = (sessionHandoffToken,)
                
                tracks.add(mpd_tracks)
                
            elif streamingProtocol == "SmoothStreaming":
                ism_tracks = Tracks.from_ism(
                    url=mpd_url,
                    source=self.ALIASES[0],
                )
                
                for track in ism_tracks:
                    if isinstance(track.extra, dict):
                        track.extra = {"session_handoff": sessionHandoffToken, **track.extra}
                    elif isinstance(track.extra, tuple):
                        track.extra = track.extra + (sessionHandoffToken,)
                    else:
                        track.extra = (sessionHandoffToken,)
                
                tracks.add(ism_tracks)
            else:
                self.log.warning(f"Unsupported manifest type: {streamingProtocol}")
                continue

            for video in tracks.videos:
                video.note = bitrate_name
            
        if len(self.bitrate.split('+')) > 1:
            self.bitrate = "CVBR,CBR"
            self.log.info("Selected video manifest bitrate: %s", self.bitrate)

        return tracks

    # Service specific classes

    class DeviceRegistration:
        def __init__(self, device: dict, endpoints: dict, cache_path: str, session: requests.Session, log: Logger, base_service):
            self.device = device
            self.endpoints = endpoints
            self.cache_path = cache_path
            self.log = log
            self.base_service = base_service
            
            self.clean_session = requests.Session()
            self.clean_session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9"
            })
    
            self.device = {k: str(v) if not isinstance(v, str) else v for k, v in self.device.items()}
            self.bearer = None
    
            cache_file = Path(self.base_service.get_cache(cache_path))
            cached_data = None
            if cache_file.exists():
                try:
                    with open(cache_file, encoding="utf-8") as fd:
                        cached_data = jsonpickle.decode(fd.read())
                except Exception as e:
                    log.debug(f"Failed to read cache: {e}")
    
            if cached_data and cached_data.get("expires_in", 0) > int(time.time()):
                self.log.info(" + Using cached device bearer")
                self.bearer = cached_data["access_token"]
            else:
                self.log.info(" + Registering new device bearer")
                self.bearer = self.register()
    
        def register(self) -> str:
            """Register device with polling loop using clean session (NO cookies)."""
            
            code_pair = self.get_code_pair()
            public_code = code_pair["public_code"]
    
            self.log.info(f" + Visit https://www.primevideo.com/mytv and enter the code: {public_code}")
            self.log.info(f"   Waiting for authorisation (up to 5 minutes)...")
    
            interval = 5
            deadline = int(time.time()) + 300
            last_log_time = 0
    
            while int(time.time()) < deadline:
                try:
                    response = self.clean_session.post(
                        url=self.endpoints["register"],
                        headers={
                            "Content-Type": "application/json", 
                            "Accept-Language": "en-US",
                            "Accept": "application/json"
                        },
                        json={
                            "auth_data": {"code_pair": code_pair},
                            "registration_data": self.device,
                            "requested_token_type": ["bearer"],
                            "requested_extensions": ["device_info", "customer_info"]
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "response" in data and "success" in data["response"]:
                            break
                        elif "error" in data.get("response", {}):
                            error_code = data["response"]["error"].get("code", "")
                            if error_code == "Unauthorized":
                                pass
                            else:
                                raise self.log.exit(f"Registration error: {error_code}")
                    elif response.status_code == 401:
                        # 401 = Unauthorized 
                        pass
                    else:
                        self.log.debug(f"Unexpected status {response.status_code}")
                        
                except requests.exceptions.RequestException as e:
                    self.log.debug(f"Request error: {e}")
                
                if int(time.time()) - last_log_time >= 30:
                    remaining = int((deadline - time.time()) / 60)
                    self.log.info(f"   Still waiting... ({remaining} minute(s) remaining)")
                    last_log_time = int(time.time())
                
                time.sleep(interval)
                continue
            
            else:
                raise self.log.exit("Device registration timed out â€” code was not approved in time.")
    
            bearer = data["response"]["success"]["tokens"]["bearer"]
            expires_val = bearer.get("expires_in", 3600)
            if isinstance(expires_val, dict):
                expires_val = expires_val.get("value", 3600)
            
            bearer_data = {
                "access_token": bearer["access_token"],
                "refresh_token": bearer.get("refresh_token", ""),
                "expires_in": int(time.time()) + int(expires_val),
            }
            
            cache_file = Path(self.base_service.get_cache(self.cache_path))
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as fd:
                fd.write(jsonpickle.encode(bearer_data))
    
            self.log.info(" + Device registered and token cached successfully")
            return bearer_data["access_token"]

        def refresh(self, device: dict, refresh_token: str) -> dict:
            response = self.session.post(
                url=self.endpoints["token"],
                json={
                    "app_name": device["app_name"],
                    "app_version": device["app_version"],
                    "source_token_type": "refresh_token",
                    "source_token": refresh_token,
                    "requested_token_type": "access_token"
                }
            ).json()
            if "error" in response:
                cache_file = Path(self.base_service.get_cache(self.cache_path))
                if cache_file.exists():
                    cache_file.unlink()
                raise self.log.exit(f"Failed to refresh device token: {response['error_description']} [{response['error']}]")
            if response["token_type"] != "bearer":
                raise self.log.exit("Unexpected returned refreshed token type")
            return response

        def get_csrf_token(self) -> str:
            res = self.session.get(self.endpoints["ontv"])
            response = res.text
            if 'input type="hidden" name="appAction" value="SIGNIN"' in response:
                raise self.log.exit("Cookies are signed out, cannot get ontv CSRF token. Expecting profile to have cookies for: {self.endpoints['ontv']}")
            for match in re.finditer(r"<script type=\"text/template\">(.+)</script>", response):
                prop = json.loads(match.group(1))
                prop = prop.get("props", {}).get("codeEntry", {}).get("token")
                if prop:
                    return prop
            raise self.log.exit("Unable to get ontv CSRF token")

        def get_code_pair(self) -> dict:
            """Get code pair using clean session (NO cookies)."""
            url = self.endpoints["codepair"]
            
            self.log.debug(f"Getting code pair from: {url}")
            
            response = self.clean_session.post(
                url=url,
                headers={"Content-Type": "application/json", "Accept-Language": "en-US"},
                json={"code_data": self.device}
            )
            
            if response.status_code != 200:
                raise self.log.exit(f"Unable to get code pair: HTTP {response.status_code}\n{response.text}")
            
            data = response.json()
            if "error" in data:
                raise self.log.exit(f"Unable to get code pair: {data.get('error_description', data['error'])}")
            
            return data