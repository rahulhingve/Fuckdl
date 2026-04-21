import base64
import json
import re
from datetime import datetime
from urllib.parse import unquote
from typing import List, Optional, Dict, Any

import click
import m3u8
import requests

from fuckdl.objects import AudioTrack, TextTrack, Title, Tracks, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.collections import as_list
from fuckdl.utils import try_get, is_close_match
from fuckdl.vendor.pymp4.parser import Box
from fuckdl.utils.widevine.device import LocalDevice


class AppleTVPlus(BaseService):
    """
    Service code for Apple's TV Plus streaming service (https://tv.apple.com).

    Updated By @AnotherBigUserHere
    Fixed By @Hugov - Updated license URI format with w parameter and id=2
    V4
    
    + Fixed Developer token, to obtain the titles
    + Better handling of Widevine and playready of their licence
    + Shows the manifest, HLS manifiest, and says all the information
    + Totally Functional, but enjoy this new rewrite of the service
    + Updated license URI format to include w parameter for watermarking

    For now, it is the most updated script of AppleTV, patch provided by @hugov, by the M3U observation
    but tell me if you locate another issuse or the developers that works on it, Thanks a lot to all

    \b
    Authorization: Cookies (Apple Music Token)
    Security: 
    - PR SL3000 4K / 1080p

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["ATVP", "appletvplus", "appletv+"]
    TITLE_RE = r"^(?:https?://tv\.apple\.com(?:/[a-z]{2})?/(?:movie|show|episode)/[a-z0-9-]+/)?(?P<id>umc\.cmc\.[a-z0-9]+)"

    VIDEO_CODEC_MAP = {
        "H264": ["avc"],
        "H265": ["hvc", "hev", "dvh"]
    }
    
    AUDIO_CODEC_MAP = {
        "AAC": ["HE", "stereo"],
        "AC3": ["ac3"],
        "EC3": ["ec3", "atmos"]
    }

    # ISO-Country Code to Storefront ID mapping
    STOREFRONT_MAP = {
        "US": "143441", "GB": "143444", "DE": "143443", "FR": "143442", 
        "CA": "143455", "AU": "143460", "JP": "143462", "BR": "143503",
        "MX": "143468", "ES": "143454", "IT": "143450", "NL": "143452",
        "SE": "143456", "CH": "143459", "AT": "143445", "BE": "143446",
        "DK": "143458", "FI": "143447", "IE": "143449", "NO": "143457",
        "PT": "143453", "PL": "143478", "RU": "143469", "TR": "143480",
        "CN": "143465", "HK": "143463", "TW": "143470", "KR": "143466",
        "SG": "143464", "IN": "143467", "AE": "143481", "SA": "143479",
        "ZA": "143472", "NZ": "143461", "CL": "143483", "CO": "143501",
        "AR": "143505", "PE": "143507", "VE": "143502", "PH": "143474",
        "MY": "143473", "TH": "143475", "ID": "143476", "VN": "143471",
        "IL": "143491", "EG": "143516", "NG": "143561", "KE": "143529"
    }

    @staticmethod
    @click.command(name="AppleTVPlus", short_help="https://tv.apple.com")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return AppleTVPlus(ctx, **kwargs)

    def __init__(self, ctx, title):
        super().__init__(ctx)
        self.parse_title(ctx, title)
        self.cdm = ctx.obj.cdm
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.alang = ctx.parent.params["alang"]
        self.slang = ctx.parent.params["slang"]
        self.subs_only = ctx.parent.params["subs_only"]
        
        self.extra_server_parameters = None
        self.storefront = None
        self.developer_token = None
        self.watermarktoken = None
        self.watermark_param = None  # New: store the w parameter from HLS
        
        self.configure()

    def get_titles(self) -> List[Title]:
        """Get title(s) based on the provided ID."""
        title_info = self._get_title_info()
        
        if not title_info:
            raise self.log.exit(f" - Title ID {self.title!r} could not be found.")
        
        if title_info["type"] == "Movie":
            return self._process_movie_title(title_info)
        else:
            return self._process_tv_series(title_info)

    def get_tracks(self, title: Title) -> Tracks:
        """Get available tracks for the title."""
        stream_data = self._get_stream_data(title.service_data["id"])
        
        if not stream_data["isEntitledToPlay"]:
            raise self.log.exit(" - User is not entitled to play this title")
        
        self.extra_server_parameters = stream_data["assets"]["fpsKeyServerQueryParameters"]
        hls_url = stream_data["assets"]["hlsUrl"]
        
        self.log.info(f"Manifiest URL: {hls_url}")
        master_playlist = self._fetch_hls_manifest(hls_url)
        tracks = self._parse_tracks_from_hls(master_playlist, hls_url)
        
        # Extract w parameter from the license URI in the manifest
        self.watermarktoken = None
        self.watermark_param = None
        
        # Look for w parameter in the video playlist URLs or EXT-X-KEY tags
        if hasattr(master_playlist, 'playlists') and master_playlist.playlists:
            for playlist in master_playlist.playlists:
                # Check if the playlist URI has w parameter
                if hasattr(playlist, 'uri') and playlist.uri:
                    match_w = re.search(r'[?&]w=([^&]+)', playlist.uri)
                    if match_w:
                        self.watermark_param = match_w.group(1)
                        self.log.info(f" - Found w parameter in playlist URI: {self.watermark_param}")
                        break
                
                # Also check for EXT-X-KEY tags which contain license URIs
                if hasattr(playlist, 'keys') and playlist.keys:
                    for key in playlist.keys:
                        if key and hasattr(key, 'uri') and key.uri:
                            match_w = re.search(r'[?&]w=([^&]+)', key.uri)
                            if match_w:
                                self.watermark_param = match_w.group(1)
                                self.log.info(f" - Found w parameter in EXT-X-KEY URI: {self.watermark_param}")
                                break
                
                if self.watermark_param:
                    break
        
        # If still not found, try to extract from video track URLs as fallback
        if not self.watermark_param:
            for track in tracks:
                if isinstance(track, VideoTrack):
                    # Extract watermarkingToken
                    match_token = re.search(r'watermarkingToken=([^&]+)', track.url)
                    if match_token:
                        self.watermarktoken = unquote(match_token.group(1))
                        self.log.info(f" - Found watermarking token: {self.watermarktoken}")
                    
                    # Try to extract w parameter from video URL
                    match_w = re.search(r'[?&]w=([^&]+)', track.url)
                    if match_w:
                        self.watermark_param = match_w.group(1)
                        self.log.info(f" - Found w parameter in video URL: {self.watermark_param}")
                        break
        
        tracks = self._filter_and_enhance_tracks(tracks)
        return tracks

    def get_chapters(self, title: Title) -> List:
        """Get chapters for the title (currently not supported)."""
        return []

    def certificate(self, **_) -> Optional[bytes]:
        """Get Widevine certificate."""
        return None  # Uses common privacy cert

    def license(self, challenge: bytes, track, title, **_) -> bytes:
        """Get license for the track."""
        return self._request_license(challenge, track)

    # Helper methods

    def configure(self):
        """Configure session with necessary headers and tokens."""
        self.log.info("Configuring Apple TV+ session...")
        
        # Set storefront
        self._set_storefront()
        
        # Get developer token
        self._get_developer_token()
        
        # Update session headers
        self._update_session_headers()

    def _set_storefront(self):
        """Set storefront based on cookies or default."""
        self.log.info("Setting storefront...")
        
        # Try to get from cookie first
        try:
            # Get all cookies with name 'itua' and use the most specific one (tv.apple.com)
            itu_cookies = []
            for cookie in self.session.cookies:
                if cookie.name == 'itua':
                    itu_cookies.append(cookie)
            
            if itu_cookies:
                # Sort by domain specificity (more specific domains first)
                # For Apple, we want tv.apple.com > music.apple.com > .apple.com
                itu_cookies.sort(key=lambda c: (
                    -len(c.domain),  # Longer domain = more specific
                    0 if c.domain == 'tv.apple.com' else 
                    1 if c.domain == 'music.apple.com' else 
                    2 if c.domain == '.apple.com' else 3
                ))
                
                itu_cookie = itu_cookies[0]
                itu_value = itu_cookie.value.upper()
                
                if itu_value in self.STOREFRONT_MAP:
                    self.storefront = self.STOREFRONT_MAP[itu_value]
                    self.log.info(f"Auto-detected storefront: {self.storefront} (from cookie itu: {itu_value} on domain {itu_cookie.domain})")
                else:
                    self.storefront = "143441"  # US default
                    self.log.warning(f"Country code {itu_value} not in map, using default US storefront")
            else:
                self.storefront = "143441"  # US default
                self.log.warning("No 'itua' cookie found, using default US storefront")
                
        except Exception as e:
            self.log.error(f"Error setting storefront: {e}")
            self.storefront = "143441"  # US default
            self.log.warning("Using default US storefront due to error")
        
        self.log.info(f"Final storefront: {self.storefront}")

    def _get_developer_token(self):
        """Get developer token from Apple TV+ or Apple Music."""
        self.log.info("Acquiring developer token...")
        
        tokens = {}
                
        # Apple Music Method
        if not tokens:
            self.log.info("Obtaining Authoritation Token...")
            music_token = self._get_music_token()
            if music_token:
                tokens["Apple Music"] = music_token
                self.log.info("âœ“ Got token from Apple Music")
        
        # Log token sources
        if tokens:
            self.log.info("Successfully obtained tokens from:")
            for source, token in tokens.items():
                token_preview = token[:50] + "..." if len(token) > 50 else token
                self.log.info(f"  â€¢ {source}: {token_preview}")
            
            # Use the first available token
            self.developer_token = next(iter(tokens.values()))
            self.log.info(f"Using token from: {next(iter(tokens.keys()))}")
        else:
            self.log.error("Failed to obtain any developer token")
            raise ValueError("No developer token could be obtained")

    def _get_music_token(self):
        """Get token from Apple Music JavaScript."""
        try:
            r = self.session.get("https://music.apple.com/us/browse")
            
            # Method 1: Look for JavaScript files
            js_patterns = [
                r'src="(/assets/index~[^"]+\.js)"',
                r'src="(/assets/index\.[^"]+\.js)"',
                r'src="(https://[^"]+apple\.com/[^"]+\.js)"'
            ]
            
            js_url = None
            for pattern in js_patterns:
                match = re.search(pattern, r.text)
                if match:
                    js_url = match.group(1)
                    if not js_url.startswith('http'):
                        js_url = "https://music.apple.com" + js_url
                    break
            
            if not js_url:
                # Method 2: Look for inline scripts with tokens
                self.log.info("Looking for inline scripts with tokens...")
                inline_tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{200,}', r.text)
                if inline_tokens:
                    # Return the longest token (usually the valid one)
                    return max(inline_tokens, key=len)
            
            if js_url:
                self.log.info(f"Fetching JavaScript from: {js_url}")
                r2 = self.session.get(js_url)
                tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{200,}', r2.text)
                if tokens:
                    # Return the longest token (usually the valid one)
                    return max(tokens, key=len)
            
            # Method 3: Try alternative Apple Music endpoints
            alt_endpoints = [
                "https://music.apple.com/assets/index.js",
                "https://music.apple.com/us/assets/index.js",
                "https://amp-api.music.apple.com"
            ]
            
            for endpoint in alt_endpoints:
                try:
                    self.log.info(f"Trying alternative endpoint: {endpoint}")
                    r3 = self.session.get(endpoint)
                    if r3.ok:
                        tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{200,}', r3.text)
                        if tokens:
                            return max(tokens, key=len)
                except Exception:
                    continue
                    
        except Exception as e:
            self.log.debug(f"Failed to get token from Apple Music: {e}")
        return None

    def _update_session_headers(self):
        """Update session headers with authentication."""
        if not self.developer_token:
            raise ValueError("Missing developer token")
        
        # Get media-user-token cookie if available
        media_token = None
        try:
            cookie_dict = self.session.cookies.get_dict()
            if "media-user-token" in cookie_dict:
                media_token = cookie_dict["media-user-token"]
                self.log.info("Found media-user-token cookie")
            else:
                self.log.warning("No 'media-user-token' cookie found")
        except Exception as e:
            self.log.debug(f"Error getting media-user-token: {e}")
        
        # Update headers
        headers = {
            "User-Agent": self.config.get("user_agent", "AppleTV6,2/11.1"),
            "Authorization": f"Bearer {self.developer_token}",
            **self.config.get("headers", {})
        }
        
        # Add media token headers if available
        if media_token:
            headers.update({
                "media-user-token": media_token,
                "x-apple-music-user-token": media_token
            })
        
        self.session.headers.update(headers)
        self.log.info("Session headers updated successfully")

    def _get_title_info(self) -> Optional[Dict]:
        """Get title information from API."""
        for media_type in ["shows", "movies"]:
            try:
                params = self._get_base_params()
                params["sf"] = self.storefront
                
                response = self.session.get(
                    url=self.config["endpoints"]["title"].format(
                        type=media_type, 
                        id=self.title
                    ),
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                return data.get("data", {}).get("content")
                
            except requests.HTTPError as e:
                if e.response.status_code != 404:
                    raise
            except json.JSONDecodeError:
                self.log.error(f"Failed to parse JSON response for {media_type}")
        
        return None

    def _process_movie_title(self, title_info: Dict) -> List[Title]:
        """Process movie title information."""
        release_date = title_info.get("releaseDate")
        year = None
        if release_date:
            try:
                year = datetime.utcfromtimestamp(release_date / 1000).year
            except (TypeError, ValueError):
                pass
        
        return [Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title_info["title"],
            year=year,
            original_lang=title_info.get("originalSpokenLanguages", [{}])[0].get("locale", "en"),
            source=self.ALIASES[0],
            service_data=title_info
        )]

    def _process_tv_series(self, title_info: Dict) -> List[Title]:
        """Process TV series episodes."""
        params = self._get_base_params()
        params["sf"] = self.storefront
        
        response = self.session.get(
            url=self.config["endpoints"]["tv_episodes"].format(id=self.title),
            params=params
        )
        response.raise_for_status()
        
        data = response.json()
        episodes = data.get("data", {}).get("episodes", [])
        
        titles = []
        for episode in episodes:
            titles.append(Title(
                id_=self.title,
                type_=Title.Types.TV,
                name=episode.get("showTitle", ""),
                season=episode.get("seasonNumber", 1),
                episode=episode.get("episodeNumber", 1),
                episode_name=episode.get("title"),
                original_lang=title_info.get("originalSpokenLanguages", [{}])[0].get("locale", "en"),
                source=self.ALIASES[0],
                service_data=episode
            ))
        
        return titles

    def _get_stream_data(self, content_id: str) -> Dict:
        """Get stream data for content ID."""
        params = self._get_base_params()
        params["sf"] = self.storefront
        
        response = self.session.get(
            url=self.config["endpoints"]["manifest"].format(id=content_id),
            params=params
        )
        response.raise_for_status()
        
        data = response.json()
        return data.get("data", {}).get("content", {}).get("playables", [{}])[0]

    def _fetch_hls_manifest(self, hls_url: str) -> m3u8.M3U8:
        """Fetch and parse HLS manifest."""
        headers = {
            'User-Agent': self.config.get("user_agent", 'AppleTV6,2/11.1')
        }
        
        response = requests.get(url=hls_url, headers=headers)
        response.raise_for_status()
        
        return m3u8.loads(response.text, hls_url)

    def _parse_tracks_from_hls(self, master_playlist: m3u8.M3U8, base_url: str) -> Tracks:
        """Parse tracks from HLS master playlist."""
        tracks = Tracks.from_m3u8(
            master=master_playlist,
            source=self.ALIASES[0]
        )
        
        # Store original manifest data
        for track in tracks:
            if hasattr(track, 'extra'):
                track.extra = {"manifest": track.extra}
            else:
                track.extra = {"manifest": None}
        
        return tracks

    def _filter_and_enhance_tracks(self, tracks: Tracks) -> Tracks:
        """Filter and enhance track information."""
        # Filter video tracks by codec
        if self.vcodec and self.vcodec in self.VIDEO_CODEC_MAP:
            tracks.videos = [
                x for x in tracks.videos 
                if any(codec in (x.codec or "").lower() for codec in self.VIDEO_CODEC_MAP[self.vcodec])
            ]
        
        # Filter audio tracks by codec
        if self.acodec and self.acodec in self.AUDIO_CODEC_MAP:
            tracks.audios = [
                x for x in tracks.audios 
                if any(codec in (x.codec or "").lower() for codec in self.AUDIO_CODEC_MAP[self.acodec])
            ]
        
        # Enhance track information
        for track in tracks:
            self._enhance_track_info(track)
        
        # Filter subtitle tracks by language using slang (subtitle language param).
        # CC and SDH tracks are only included if their language was explicitly
        # requested â€” they never pass via is_original_lang to avoid English CC
        # bleeding into downloads for other languages.
        # Deduplicate subtitles by URL (ATVP returns each track 3 times
        # via different CDN paths â€” keep only unique URLs).
        seen_urls = set()
        deduped = []
        for x in tracks.subtitles:
            if x.url not in seen_urls:
                seen_urls.add(x.url)
                deduped.append(x)
        tracks.subtitles = deduped

        slang_strs = [s.lower() for s in self.slang]
        if "all" in slang_strs or self.subs_only:
            pass  # keep all subtitles
        else:
            has_orig = "orig" in slang_strs
            # Build per-requested-lang: base â†’ territory (or None if no territory)
            # e.g. "es-ES" â†’ {"es": "es"}, "es" â†’ {"es": None}, "fr" â†’ {"fr": None}
            req_langs = {}
            for s in slang_strs:
                if s in ("all", "orig"):
                    continue
                parts = s.split("-")
                base = parts[0]
                territory = parts[1].lower() if len(parts) > 1 else None
                req_langs[base] = territory

            def _sub_matches(x):
                track_lang = str(x.language).lower()
                parts = track_lang.split("-")
                track_base = parts[0]
                track_territory = parts[1] if len(parts) > 1 else None

                if track_base not in req_langs:
                    return False

                req_territory = req_langs[track_base]
                if req_territory is None:
                    # User asked for plain "es" â€” accept all es-* variants
                    return True
                # User asked for specific territory â€” only accept exact match
                # or tracks with no territory tag at all
                return track_territory is None or track_territory == req_territory

            tracks.subtitles = [
                x for x in tracks.subtitles
                if _sub_matches(x)
                or (x.is_original_lang and has_orig
                    and str(x.language).split("-")[0].lower() in req_langs)
            ]
        
        # Filter by CDN (keep only vod-ak CDN for consistency).
        # Apply per track type to preserve the subtitle language filtering above.
        filtered_tracks = Tracks()
        filtered_tracks.videos = [x for x in tracks.videos if "vod-ak" in x.url]
        filtered_tracks.audios = [x for x in tracks.audios if "vod-ak" in x.url]
        # Subtitles are already language-filtered above â€” only apply CDN filter,
        # do NOT re-add tracks that were excluded by the language filter.
        filtered_tracks.subtitles = [x for x in tracks.subtitles if "vod-ak" in x.url]
        
        return filtered_tracks

    def _enhance_track_info(self, track):
        """Enhance track with additional information."""
        if isinstance(track, VideoTrack):
            track.encrypted = True
            track.needs_ccextractor_first = True
            
            # Try to determine quality from URL
            if track.extra.get("manifest") and track.extra["manifest"].uri:
                uri = track.extra["manifest"].uri
                for quality_str, quality_val in self.config["quality_map"].items():
                    if quality_str.lower() in uri.lower():
                        track.extra["quality"] = quality_val
                        break
        
        elif isinstance(track, AudioTrack):
            track.encrypted = True
            
            # Extract bitrate from URL
            bitrate_match = re.search(r"&g=(\d+?)&", track.url) or re.search(r"_gr(\d+)_", track.url)
            if bitrate_match:
                bitrate_str = bitrate_match.group(1)
                if len(bitrate_str) >= 3:
                    track.bitrate = int(bitrate_str[-3:]) * 1000
            
            # Clean up codec string
            if track.codec:
                track.codec = track.codec.replace("_vod", "")
        
        elif isinstance(track, TextTrack):
            track.codec = "vtt"

    def _request_license(self, challenge: bytes, track) -> bytes:
        """Request license from Apple's license server."""
        license_request = self._build_license_request(challenge, track)
        
        try:
            response = self.session.post(
                url=self.config["endpoints"]["license"],
                json=license_request
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Validate response structure
            if "streaming-response" not in data:
                raise ValueError("Invalid license response: missing streaming-response")
            
            streaming_keys = data["streaming-response"].get("streaming-keys", [])
            if not streaming_keys:
                raise ValueError("No streaming keys in license response")
            
            # Get license data
            license_data = streaming_keys[0].get("license")
            if not license_data:
                raise ValueError("No license data in streaming key")
            
            return base64.b64decode(license_data)
            
        except requests.HTTPError as e:
            self.log.error(f"License request failed: {e}")
            if e.response.text:
                try:
                    error_data = e.response.json()
                    self.log.error(f"Error details: {error_data}")
                except:
                    self.log.error(f"Raw error: {e.response.text}")
            raise
  
    def _build_license_request(self, challenge: bytes, track) -> Dict:
        """Build license request based on CDM type with updated URI format."""
        
        # Get w parameter from manifest
        w_param = self.watermark_param or ""
        
        _is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                        (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                         self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        if _is_playready:
            key_system = "com.microsoft.playready"
            
            # Handle PlayReady PSSH
            if hasattr(track, 'pr_pssh'):
                if isinstance(track.pr_pssh, str):
                    pssh_bytes = base64.b64decode(track.pr_pssh)
                else:
                    pssh_bytes = track.pr_pssh
            else:
                pssh_bytes = b''
            
            pssh_b64 = base64.b64encode(pssh_bytes).decode('utf-8')
            
            # Build URI with w parameter
            if w_param:
                uri = f"data:text/plain;w={w_param};charset=UTF-16;base64,{pssh_b64}"
            else:
                uri = f"data:text/plain;charset=UTF-16;base64,{pssh_b64}"
            
            challenge_b64 = base64.b64encode(challenge).decode('utf-8')
            key_id = 2
            
        else:  # Widevine
            key_system = "com.widevine.alpha"
            pssh_box = Box.build(track.pssh) if hasattr(track, 'pssh') else b''
            pssh_b64 = base64.b64encode(pssh_box).decode()
            
            # Build URI with w parameter
            if w_param:
                uri = f"data:text/plain;w={w_param};base64,{pssh_b64}"
            else:
                uri = f"data:text/plain;base64,{pssh_b64}"
            
            challenge_b64 = base64.b64encode(challenge).decode()
            key_id = 2
        
        # Get adamId and svcId
        adam_id = ""
        svc_id = ""
        if self.extra_server_parameters:
            adam_id = self.extra_server_parameters.get('adamId', '')
            svc_id = self.extra_server_parameters.get('svcId', '')
        
        # Build license request
        streaming_keys = {
            "challenge": challenge_b64,
            "key-system": key_system,
            "uri": uri,
            "id": key_id,
            "lease-action": "start",
            "adamId": adam_id,
            "isExternal": True,
            "svcId": svc_id,
        }
        
        if self.extra_server_parameters:
            streaming_keys["extra-server-parameters"] = self.extra_server_parameters
        
        return {
            'streaming-request': {
                'version': 1,
                'streaming-keys': [streaming_keys],
            }
        }

    def _get_base_params(self):
        """Get base parameters for API requests."""
        return self.config.get("params", {}).copy()