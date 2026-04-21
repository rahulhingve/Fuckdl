from __future__ import annotations

import hashlib
import json
import base64
import random
import re
import string
import time
import os
from hashlib import md5
from urllib.parse import parse_qs, unquote, urlparse
from typing import Optional, Any
from datetime import datetime, timedelta

import click
import requests
from bs4 import BeautifulSoup
from langcodes import Language

from fuckdl.objects import AudioTrack, TextTrack, Title, Tracks, Track, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.widevine.device import LocalDevice


class YouTube(BaseService):
    """
    Service code for YouTube Platform Service (https://youtube.com).

    Original Author: CodeName393

    Structure system: Huvog

    Ported by @AnotherBigUserHere to VT
    
    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    \b
    Authorization: OAuth Token (Device Flow) with caching
    Security:
        Widevine: L1/L3 for UHD/SD content
        PlayReady: SL3/SL2 for UHD/FHD content
    """
    
    ALIASES = ["YT", "YTTV", "YouTube"]

    TITLE_RE = (
        r"^(?:https?://)?(?:www\.|m\.)?(?:youtube\.com/watch\?.*v=|youtu\.be/|youtube\.com/shorts/)(?P<id>[a-zA-Z0-9_-]{11})(?:[&?].*)?$",
        r"^(?:https?://)?(?:www\.)?youtube\.com/show/(?P<id>SC[a-zA-Z0-9_-]+)",
        r"^(?P<id>[a-zA-Z0-9_-]{11})$",
        r"^(?P<id>SC[a-zA-Z0-9_-]+)$",
    )

    VIDEO_CODEC_MAP = {
        "H264": ["avc1", "h264"],
        "H265": ["hvc1", "hev1", "dvh1"],
        "AV1": ["av1", "av01"],
        "VP9": ["vp9", "vp09"],
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "AC3": "ac-3",
        "EC3": "ec-3",
        "DTS": "dtse",
        "OPUS": "opus",
    }

    @staticmethod
    @click.command(name="YouTube", short_help="https://youtube.com")
    @click.argument("title", type=str, required=False)
    @click.option("-ag", "--auto-generated", is_flag=True, default=False, help="Get auto generated subtitles too.")
    @click.option("-b", "--bitrate", default="CBR", 
                type=click.Choice(["VBR", "CBR", "VBR+CBR"], case_sensitive=False),
                help="Video bitrate mode for VP9, defaults to CBR.",
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return YouTube(ctx, **kwargs)

    def __init__(self, ctx, title, bitrate, auto_generated):
        super().__init__(ctx)

        self.title = title
        self.bitrate = bitrate
        self.auto_generated = auto_generated

        self.verbose = False
        self.debug = ctx.parent.params["debug"]
        self.list = ctx.parent.params["list_"]
        self.vcodec = ctx.parent.params["vcodec"] or "H264"
        self.acodec = ctx.parent.params["acodec"]
        self.scodec = "VTT"
        self.vquality = ctx.parent.params.get("vquality") or 1080
        self.range = ctx.parent.params.get("range_") or "SDR"
        
        # Store content keys
        self._content_keys = {}
        
        # Authentication tokens
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self.ytcfg_tokens: dict[str, Any] = {}
        
        # DRM type detection
        if hasattr(ctx.obj.cdm, 'device') and hasattr(ctx.obj.cdm.device, 'type'):
            self.playready = ctx.obj.cdm.device.type == LocalDevice.Types.PLAYREADY
        else:
            try:
                self.playready = getattr(ctx.obj.cdm, 'type', '') == LocalDevice.Types.PLAYREADY
            except:
                self.playready = False

        # Parse title ID
        self.title_id = self.title
        for pattern in self.TITLE_RE:
            match = re.match(pattern, self.title)
            if match:
                self.title_id = match.group("id")
                break

        self.is_show = self.title_id.startswith("SC")
        self.is_playlist = False
        
        if "list" in self.title:
            self.is_playlist = True
            up = urlparse(self.title)
            query_parsed = parse_qs(up.query)
            if "list" in query_parsed:
                self.playlist_id = query_parsed["list"][0]

        # Authenticate
        self._authenticate()
        self.configure()

    def _get_cache_path(self) -> str:
        """Get path for token cache file"""
        cache_dir = self.get_cache("")
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, "tokens.json")

    def _load_cached_tokens(self) -> Optional[dict]:
        """Load tokens from cache if valid"""
        cache_path = self._get_cache_path()
        
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check if token is still valid (with 5 min buffer)
            if data.get("expires_at", 0) > time.time() + 300:
                self.log.info(" + Using cached tokens...")
                return data
            else:
                self.log.debug("Cached token expired")
                return None
        except Exception as e:
            self.log.debug(f"Failed to load cached tokens: {e}")
            return None

    def _save_tokens_to_cache(self, token_data: dict, expires_in: int) -> None:
        """Save tokens to cache"""
        cache_path = self._get_cache_path()
        
        token_data["expires_at"] = time.time() + expires_in - 300  # 5 min buffer
        
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(token_data, f, indent=2)
            self.log.debug("Tokens saved to cache")
        except Exception as e:
            self.log.debug(f"Failed to save tokens: {e}")

    def _authenticate(self) -> None:
        """Authenticate using OAuth device flow with caching"""
        self.log.info("Logging into YouTube...")
        
        # Try to load from cache
        cached = self._load_cached_tokens()
        
        if cached:
            self.access_token = cached.get("access_token")
            self.refresh_token = cached.get("refresh_token")
            self.token_expires_at = cached.get("expires_at")
            
            # Try to refresh if needed
            if self.token_expires_at and self.token_expires_at <= time.time() + 300:
                if self._refresh_access_token():
                    return
            
            if self.access_token:
                self.log.info(" + Authentication successful using cached token")
                return
        
        # Need new token
        self._perform_device_flow()

    def _refresh_access_token(self) -> bool:
        """Refresh access token using refresh token"""
        if not self.refresh_token:
            return False
        
        self.log.info(" + Refreshing access token...")
        
        headers = {
            "User-Agent": self.config["device"]["user_agent"],
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        form_data = {
            "client_id": self.config["device"]["vr"]["client_id"],
            "client_secret": self.config["device"]["vr"]["client_secret"],
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }
        
        try:
            res = self.session.post(
                self.config["endpoints"]["token"],
                headers=headers,
                data=form_data
            )
            res.raise_for_status()
            data = res.json()
            
            self.access_token = data["access_token"]
            self.token_expires_at = time.time() + data.get("expires_in", 3600)
            
            # Save updated tokens
            token_record = {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "expires_in": data.get("expires_in", 3600),
            }
            self._save_tokens_to_cache(token_record, data.get("expires_in", 3600))
            
            self.log.info(" + Token refreshed successfully")
            return True
        except Exception as e:
            self.log.warning(f" - Token refresh failed: {e}")
            return False

    def _perform_device_flow(self) -> None:
        """Perform OAuth device flow authentication"""
        device_code_data = self._request_device_code()
        verification_url = device_code_data.get("verification_url") or device_code_data.get("verification_uri")
        user_code = device_code_data["user_code"]
        device_code = device_code_data.get("device_code")
        interval = int(device_code_data.get("interval", 5))

        self.log.info(f"Open this URL: {verification_url}")
        self.log.info(f"Enter code: {user_code}")
        self.log.info("Authorize the requested Google account and approve access.")
        self.log.info("Waiting for authorization...")

        max_attempts = 180  # 15 minutes max
        attempts = 0
        
        while self.access_token is None and attempts < max_attempts:
            try:
                token_data = self._get_token(device_code)
                
                self.access_token = token_data["access_token"]
                self.refresh_token = token_data.get("refresh_token")
                expires_in = token_data.get("expires_in", 3600)
                self.token_expires_at = time.time() + expires_in
                
                token_record = {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "expires_in": expires_in,
                }
                self._save_tokens_to_cache(token_record, expires_in)
                
                self.log.info(" + Authentication successful!")
                return
            except requests.HTTPError as e:
                if e.response.status_code == 428:  # Authorization pending
                    attempts += 1
                    time.sleep(interval)
                    continue
                raise
            except Exception as e:
                if "authorization_pending" in str(e):
                    attempts += 1
                    time.sleep(interval)
                    continue
                raise
        
        if not self.access_token:
            raise self.log.exit("Authentication timeout. Please try again.")

    def _request_device_code(self) -> dict:
        """Request device code from Google OAuth"""
        headers = {
            "User-Agent": self.config["device"]["user_agent"],
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form_data = {
            "client_id": self.config["device"]["vr"]["client_id"],
            "scope": "https://www.googleapis.com/auth/youtube",
        }
        res = self.session.post(
            self.config["endpoints"]["device_code"],
            headers=headers,
            data=form_data
        )
        res.raise_for_status()
        return res.json()

    def _get_token(self, device_code: str) -> dict:
        """Get access token using device code"""
        headers = {
            "User-Agent": self.config["device"]["user_agent"],
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form_data = {
            "client_id": self.config["device"]["vr"]["client_id"],
            "client_secret": self.config["device"]["vr"]["client_secret"],
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        }
        res = self.session.post(
            self.config["endpoints"]["token"],
            headers=headers,
            data=form_data
        )
        res.raise_for_status()
        return res.json()

    def get_titles(self):
        """Get title information"""
        if self.is_show:
            return self._get_show_titles()
        elif self.is_playlist:
            return self.get_titles_from_playlist()

        player_response = self._fetch_player_data(self.title_id)
        
        if not player_response:
            raise self.log.exit("Unable to get metadata. Is the title ID correct?")

        try:
            video_id = player_response["videoDetails"]["videoId"]
            title_name = player_response["videoDetails"]["title"]
        except KeyError:
            try:
                error = player_response["playabilityStatus"]["reason"]
                raise self.log.exit(f"Couldn't obtain video information: {error}")
            except KeyError:
                self.log.exit(f"Failed to get video information: {player_response}")
        
        year = None
        if player_response.get("microformat"):
            microformat = player_response["microformat"].get("playerMicroformatRenderer", {})
            publish_date = microformat.get("publishDate")
            if publish_date:
                year = publish_date[:4]
        
        return Title(
            id_=video_id,
            type_=Title.Types.MOVIE,
            name=title_name,
            year=year,
            original_lang="en",
            source=self.ALIASES[0],
            service_data=player_response
        )

    def get_tracks(self, title):
        """Get tracks for a title with proper DRM detection"""
        if "streamingData" not in title.service_data:
            title.service_data = self._fetch_player_data(title.id)
        
        streaming_data = title.service_data.get("streamingData", {})
        
        if not streaming_data:
            self.log.warning("No streaming data found")
            return Tracks()
        
        tracks = Tracks()
        
        # Check for DRM protection at stream level
        has_license_infos = "licenseInfos" in streaming_data
        license_infos = streaming_data.get("licenseInfos", [])
        
        # Get PSSH from licenseInfos if available
        pssh_data = None
        kid_data = None
        for lic_info in license_infos:
            drm_family = lic_info.get("drmFamily")
            target_family = "PLAYREADY" if self.playready else "WIDEVINE"
            if drm_family == target_family:
                pssh_data = lic_info.get("psshData")
                if "keyIds" in lic_info and lic_info["keyIds"]:
                    kid_data = lic_info["keyIds"][0]
                break
        
        adaptive_formats = streaming_data.get("adaptiveFormats", [])
        progressive_formats = streaming_data.get("formats", [])
        
        self.log.debug(f"Found {len(adaptive_formats)} adaptive formats, {len(progressive_formats)} progressive formats")
        
        all_formats = adaptive_formats + progressive_formats
        
        for fmt in all_formats:
            mime_type = fmt.get("mimeType", "")
            has_video = fmt.get("width") is not None and fmt.get("height") is not None
            
            # Detect DRM - formats without URL are typically DRM protected
            has_url = bool(fmt.get("url"))
            has_drm = not has_url or fmt.get("hasDrm", False) or fmt.get("has_drm", False) or has_license_infos
            
            # Video track
            if has_video:
                # Extract codec
                codec_str = fmt.get("codecs", "")
                if not codec_str and "codecs" in mime_type:
                    match = re.search(r'codecs=["\']([^"\']+)["\']', mime_type)
                    if match:
                        codec_str = match.group(1)
                
                codec_lower = codec_str.lower()
                if "avc1" in codec_lower or "h264" in codec_lower:
                    video_codec = "H264"
                elif "vp09" in codec_lower or "vp9" in codec_lower:
                    video_codec = "VP9"
                elif "av01" in codec_lower or "av1" in codec_lower:
                    video_codec = "AV1"
                else:
                    video_codec = "H264"
                
                quality_label = fmt.get("qualityLabel", "")
                is_hdr = "HDR" in quality_label or fmt.get("dynamicRange") == "HDR10"
                
                track = VideoTrack(
                    id_=str(fmt["itag"]),
                    source="YT",
                    url=fmt.get("url"),
                    codec=video_codec,
                    bitrate=fmt.get("bitrate", 0),
                    width=fmt["width"],
                    height=fmt["height"],
                    fps=fmt.get("fps", 0),
                    hdr10=is_hdr,
                    descriptor=Track.Descriptor.URL,
                    encrypted=has_drm,
                    pssh=pssh_data if has_drm else None,
                    kid=kid_data,
                )
                tracks.add(track)
                self.log.debug(f"Added video: {fmt['width']}x{fmt['height']} - {video_codec}, encrypted={has_drm}")
            
            # Audio track
            elif "audio" in mime_type:
                codec_str = fmt.get("codecs", "")
                language = fmt.get("language", "und")
                is_descriptive = "desc" in language
                language = language.replace("-desc", "")
                
                codec_lower = codec_str.lower()
                if "mp4a" in codec_lower:
                    audio_codec = "AAC"
                elif "opus" in codec_lower:
                    audio_codec = "OPUS"
                else:
                    audio_codec = "AAC"
                
                track = AudioTrack(
                    id_=str(fmt["itag"]),
                    source="YT",
                    url=fmt.get("url"),
                    codec=audio_codec,
                    language=language,
                    descriptive=is_descriptive,
                    bitrate=fmt.get("bitrate", 0),
                    channels=fmt.get("audioChannels", 2),
                    needs_proxy=True,
                    needs_repack=False,
                    encrypted=has_drm,
                    pssh=pssh_data if has_drm else None,
                    kid=kid_data,
                )
                tracks.add(track)
        
        self.log.info(f"Found {len(tracks.videos)} video tracks, {len(tracks.audios)} audio tracks")
        
        # Process subtitles
        self._process_subtitles(title, tracks)
        
        # NO FILTERS - Show all tracks
        self.log.debug("Showing all tracks without filtering")
        
        # Set original language
        for audio in tracks.audios:
            if audio.language and audio.language != "und":
                title.original_lang = Language.get(audio.language)
                break
        
        return tracks

    def _process_subtitles(self, title: Title, tracks: Tracks) -> None:
        """Process subtitle tracks"""
        captions_data = title.service_data.get("captions", {}).get("playerCaptionsTracklistRenderer", {})
        caption_tracks = captions_data.get("captionTracks", [])
        
        for track_info in caption_tracks:
            base_url = track_info["baseUrl"]
            lang_code = track_info["languageCode"]
            vss_id = track_info["vssId"]
            
            is_asr = track_info.get("kind") == "asr"
            if is_asr and not self.auto_generated:
                continue
            
            # Convert to VTT format
            if "fmt=srv3" in base_url:
                track_url = base_url.replace("fmt=srv3", "fmt=vtt")
            else:
                track_url = base_url + "&fmt=vtt"
            
            # Determine subtitle flags - CRITICAL: these are mutually exclusive!
            # YouTube vssId format:
            #   - ".en" = normal
            #   - ".en.f" = forced
            #   - ".en.a" = SDH
            #   - ".en.c" = CC
            #   - ".en.asr" = auto-generated
            vss_lower = vss_id.lower()
            
            is_forced = "f" in vss_lower
            is_sdh = "a" in vss_lower
            is_cc = "c" in vss_lower
            
            # Forced and SDH/CC cannot both be True
            if is_forced:
                is_sdh = False
                is_cc = False
            
            # SDH and CC cannot both be True (pick one)
            if is_sdh and is_cc:
                # Prefer SDH over CC for YouTube
                is_cc = False
            
            tracks.add(
                TextTrack(
                    id_=md5(track_url.encode()).hexdigest()[0:6],
                    source="YT",
                    url=track_url,
                    codec=self.scodec.lower(),
                    language=lang_code,
                    forced=is_forced,
                    sdh=is_sdh,
                    cc=is_cc,
                    descriptor=Track.Descriptor.URL,
                ),
                warn_only=True,
            )

    def license(self, challenge, title, track, *_, **__):
        """Get license for DRM content with PlayReady support"""
        if "streamingData" not in title.service_data:
            raise self.log.exit("No streaming data, licensing not possible.")
        
        self._content_keys = {}
        streaming_data = title.service_data["streamingData"]
        license_infos = streaming_data.get("licenseInfos", [])
        
        # Find appropriate license URL
        license_url = None
        for lic_info in license_infos:
            drm_family = lic_info.get("drmFamily")
            target_family = "PLAYREADY" if self.playready else "WIDEVINE"
            if drm_family == target_family:
                license_url = lic_info.get("url")
                break
        
        if license_url:
            headers = {}
            if self.playready:
                headers["Content-Type"] = "text/xml; charset=utf-8"
                headers["SOAPAction"] = '"http://schemas.microsoft.com/DRM/2007/03/protocols/AcquireLicense"'
            else:
                headers["Content-Type"] = "application/octet-stream"
            
            res = self.session.post(license_url, data=challenge, headers=headers)
            
            if res.status_code == 200:
                if self.playready:
                    license_data = base64.b64encode(res.content.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in res.content else res.content).decode()
                else:
                    license_data = res.content.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in res.content else res.content
                
                self._extract_keys_from_license(license_data)
                return license_data
        
        # Fallback to YouTube API
        try:
            res = self.session.post(
                url=self.config["endpoints"]["license"],
                json={
                    "context": {
                        "client": {
                            "clientName": "ANDROID_VR",
                            "clientVersion": "1.61.48",
                        }
                    },
                    "videoId": title.id,
                    "drmSystem": f"DRM_SYSTEM_{'PLAYREADY' if self.playready else 'WIDEVINE'}",
                    "licenseRequest": base64.b64encode(challenge).decode('utf-8'),
                },
            ).json()
            
            license_data = base64.urlsafe_b64decode(res["license"])
            self._extract_keys_from_license(license_data)
            return license_data
        except Exception as e:
            self.log.error(f"License request failed: {e}")
            raise

    def _extract_keys_from_license(self, license_data):
        """Extract content keys from license response"""
        try:
            from pywidevine.license_protocol import License
            license_obj = License.load(license_data)
            
            for key in license_obj.keys:
                if key.type == 'CONTENT':
                    kid_hex = key.kid.hex()
                    key_hex = key.key.hex()
                    self._content_keys[kid_hex] = key_hex
                    self.log.info(f" + Extracted key for KID: {kid_hex[:8]}...")
        except Exception as e:
            self.log.debug(f"License parsing failed: {e}")

    def configure(self):
        """Configure session with authentication"""
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "X-YouTube-Client-Name": "28",
            "X-YouTube-Client-Version": "1.61.48",
            "Content-Type": "application/json",
        })

    def _fetch_player_data(self, video_id: str) -> dict:
        """Fetch player data using authenticated client"""
        payload = {
            "context": {
                "client": {
                    "deviceMake": "Oculus",
                    "deviceModel": "Quest 3",
                    "clientName": "ANDROID_VR",
                    "clientVersion": "1.61.48",
                    "osName": "Android",
                    "osVersion": "12",
                    "hl": "en_US",
                    "gl": "US",
                },
            },
            "videoId": video_id,
            "racyCheckOk": True,
            "contentCheckOk": True,
        }
        
        try:
            res = self.session.post(
                self.config["endpoints"]["player"],
                json=payload
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            self.log.debug(f"VR client failed: {e}, trying web...")
            return self._fetch_player_data_web(video_id)

    def _fetch_player_data_web(self, video_id: str) -> dict:
        """Fallback to web client"""
        try:
            url = f"https://www.youtube.com/watch?v={video_id}"
            res = self.session.get(url, headers={
                "User-Agent": self.config["device"]["user_agent"],
            })
            
            if match := re.search(r'var ytInitialPlayerResponse\s*=\s*({.*?});', res.text, re.DOTALL):
                return json.loads(match.group(1))
        except Exception as e:
            self.log.debug(f"Web client failed: {e}")
        
        return {}

    def get_chapters(self, title):
        return []

    def get_titles_from_playlist(self):
        """Get titles from a playlist"""
        res = self.session.get(f"https://www.youtube.com/playlist?list={self.playlist_id}").text
        if not (match := re.search(r"var ytInitialData = ({.+?});</script>", res, re.MULTILINE)):
            raise self.log.exit("Failed to retrieve ytInitialData")
        
        init_object = json.loads(match.group(1))
        episode_items = init_object["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0]["tabRenderer"]["content"]["sectionListRenderer"]["contents"][0]["itemSectionRenderer"]["contents"][0]["playlistVideoListRenderer"]["contents"]
        
        playlist_name = init_object["sidebar"]["playlistSidebarRenderer"]["items"][0]["playlistSidebarPrimaryInfoRenderer"]["title"]["runs"][0]["text"]
        
        titles = []
        for idx, item in enumerate(episode_items):
            video_id = item["playlistVideoRenderer"]["videoId"]
            titles.append(Title(
                id_=video_id,
                type_=Title.Types.TV,
                name=playlist_name,
                episode=idx + 1,
                episode_name=item["playlistVideoRenderer"]["title"]["runs"][0]["text"],
                source=self.ALIASES[0],
                service_data=self._fetch_player_data(video_id),
            ))
        return titles

    def _get_show_titles(self):
        """Get titles for a show"""
        url = f"https://www.youtube.com/show/{self.title_id}"
        res = self.session.get(url).text
        if not (match := re.search(r"var ytInitialData = ({.+?});</script>", res, re.MULTILINE)):
            raise self.log.exit("Failed to retrieve ytInitialData")
        
        return []  # Simplified for now