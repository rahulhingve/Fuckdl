from __future__ import annotations
import re
import uuid
import json
import os
from pathlib import Path
from hashlib import md5
from langcodes import Language
from typing import Any, Optional, Union
from copy import copy
import click
import requests
import m3u8
from fuckdl.objects import Title, Tracks, AudioTrack, MenuTrack, TextTrack, Track, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.widevine.device import LocalDevice
from requests.adapters import HTTPAdapter, Retry
from fuckdl.config import config


class FOD(BaseService):
    """
    Service code for Fuji TV On Demand streaming service (https://fod.fujitv.co.jp/).
    
    @nsbc_crash Original creator
    @AnotherBigUserHere Modifications
    
    \b
    Authorization: Credentials / Cookies
    Security:
        Widevine:
            L3: 1080p
        PlayReady:
            L3: 1080p (if available)
    
    \b
    Notes:
        - Requires Japan IP address or X-Forwarded-For header
        - Supports both TV and Web authentication methods
        - Token caching implemented for credential-based auth
        - Automatic fallback to web endpoint when TV endpoint fails (400 error)
        - Improved subtitle handling
        - Better season detection
        - Persistent device ID
    """
    
    ALIASES = ["FOD", "fujitv", "Fujitv"]
    TITLE_RE = [
        r"^(?:https?://fod\.fujitv\.co\.jp/title/)?(?P<id>[0-9a-z]+)",
        r"^(?P<id>[0-9a-z]+)$"
    ]
    
    TV_USER_AGENT = "Dalvik/2.1.0 (Linux; U; Android 16; AOSP TV on x86 Build/BT2A.250323.001.A4)"
    WEB_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    
    @staticmethod
    @click.command(name="FOD", short_help="https://fod.fujitv.co.jp", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.option("-s", "--single", is_flag=True, default=False,
                  help="Force Single Season instead of getting All Season.")
    @click.option("-a", "--auth", type=click.Choice(['cookies', 'credentials'], case_sensitive=False),
                  default="credentials", help="Authentication method to use.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return FOD(ctx, **kwargs)
    
    def __init__(self, ctx, title, single, auth):
        super().__init__(ctx)
        data = self.parse_title(ctx, title)
        self.title = data.get("id")
        self.single = single
        self.auth_method = auth
        self.token = None
        self.session_uuid = None
        self.stream_data = None
        
        # Detect PlayReady CDM
        self._is_playready = self._detect_playready()
        
        # Initialize device ID (persistent)
        self._init_device_id()
        
        self.configure()
    
    def _detect_playready(self) -> bool:
        """Detect if we're using a PlayReady CDM."""
        try:
            if hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__:
                return True
            if hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type'):
                return self.cdm.device.type == LocalDevice.Types.PLAYREADY
        except Exception:
            pass
        return False
    
    def _init_device_id(self):
        """Initialize or load persistent device ID from cache."""
        device_cache = Path(self.get_cache("fod_device_id.json"))
        if device_cache.is_file():
            try:
                data = json.loads(device_cache.read_text())
                self.device_id = data.get("device_id")
                self.log.debug(f"Loaded existing device ID: {self.device_id}")
            except Exception:
                self.device_id = str(uuid.uuid4())
                self.log.debug(f"Generated new device ID (cache corrupt): {self.device_id}")
        else:
            self.device_id = str(uuid.uuid4())
            self.log.debug(f"Generated new device ID: {self.device_id}")
        
        # Save device ID
        device_cache.parent.mkdir(parents=True, exist_ok=True)
        device_cache.write_text(json.dumps({"device_id": self.device_id}))
    
    def get_session(self):
        """Creates a Python-requests Session with retries and common headers."""
        session = requests.Session()
        session.mount("https://", HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
        ))
        session.hooks = {
            "response": lambda r, *_, **__: r.raise_for_status(),
        }
        session.headers.update(config.headers)
        session.cookies.update(self.cookies or {})
        return session
    
    def _generate_jwt(self, payload: dict) -> str:
        """Generate JWT token for authentication."""
        import jwt
        return jwt.encode(
            payload,
            self.config["jwt_secret"],
            algorithm='HS256'
        )
    
    def _is_session_valid(self, headers: dict) -> bool:
        """Check if current session is valid."""
        try:
            req = self.session.get(
                url=self.config["endpoints"]["user_status"],
                headers=headers,
                params={"dv_type": "tv"}
            )
            self.log.debug(f"Validation response status: {req.status_code}")
            req.raise_for_status()
            user_data = req.json()
            if req.status_code == 200 and user_data.get("is_fod_member") is True:
                if "uuid" in req.cookies:
                    self.session_uuid = req.cookies.get("uuid")
                return True
            return False
        except requests.RequestException as e:
            self.log.debug(f"Session validation check failed: {e}")
            return False
    
    def _perform_login(self) -> bool:
        """Perform fresh login with credentials."""
        self.log.info(" + Performing fresh login...")
        username = self.config.get("username")
        password = self.config.get("password")
        
        if not (username and password) and self.credentials:
            username = self.credentials.username
            password = self.credentials.password
        
        if not (username and password):
            self.log.error(" - Username and Password are required to perform a new login.")
            return False
        
        minimal_headers = {"User-Agent": self.TV_USER_AGENT}
        temp_token = self._generate_jwt({
            "iss": "FOD", 
            "dv_type": "androidTV", 
            "dv_id": "google_google_aosp tv on x86_13"
        })
        auth_headers = minimal_headers.copy()
        auth_headers["X-Authorization"] = f"Bearer {temp_token}"
        auth_headers["X-Forwarded-For"] = self.config.get("forwarded_ip", "35.77.152.175")
        
        try:
            login_res = self.session.post(
                url=self.config["endpoints"]["login_app"], 
                headers=auth_headers, 
                json={"mail_address": username, "password": password}
            )
            login_data = login_res.json()
            self.log.debug(f"Login response received")
            
            fodid_login_token = None
            if login_data.get("hash_key"):
                self.log.info(" + This account requires two-factor authentication (2FA).")
                hash_key = login_data["hash_key"]
                auth_code = input(" - Please enter the authentication code from your email (2FA): ").strip()
                if not auth_code:
                    self.log.error(" - Authentication code is required.")
                    return False
                auth_code_res = self.session.post(
                    url=self.config["endpoints"]["check_auth_code"], 
                    headers=auth_headers, 
                    json={"auth_code": auth_code, "hash_key": hash_key}
                )
                auth_code_res.raise_for_status()
                auth_code_data = auth_code_res.json()
                fodid_login_token = auth_code_data.get("fodid_login_token")
                if not fodid_login_token:
                    error_message = auth_code_data.get("message", "Invalid or expired 2FA code.")
                    self.log.error(f" - 2FA check failed: {error_message}")
                    return False
            elif "fodid_login_token" in login_data:
                self.log.info(" + Login successful without 2FA.")
                fodid_login_token = login_data["fodid_login_token"]
            else:
                self.log.error(f" - Login failed: {login_data.get('message', 'Invalid credentials')}")
                return False
            
            check_token_res = self.session.post(
                url=self.config["endpoints"]["check_token"], 
                headers=auth_headers, 
                json={"fodid_login_token": fodid_login_token}
            )
            check_token_res.raise_for_status()
            uid = check_token_res.json()["uid"]
        except (requests.RequestException, json.JSONDecodeError) as e:
            self.log.error(f" - An error occurred during login flow: {e}")
            return False
        
        access_token = self._generate_jwt({
            "iss": "FOD",
            "uid": uid,
            "dv_type": "androidTV",
            "dv_id": "google_google_aosp tv on x86_13"
        })
        validation_headers = minimal_headers.copy()
        validation_headers["X-Authorization"] = f"Bearer {access_token}"
        
        if not self._is_session_valid(validation_headers):
            self.log.error(" - Failed to validate final access token")
            return False
        
        self.token = {"access_token": access_token, "uuid": self.session_uuid}
        self.log.info(" + New token acquired and validated successfully.")
        
        # Cache token
        cache_path = Path(self.get_cache("fod_token.json"))
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(self.token))
        
        return True
    
    def configure(self) -> None:
        """Configure authentication for FOD service."""
        self.log.info(" + Configuring FOD authentication")
        self.session.headers.update({
            "Origin": "https://fod.fujitv.co.jp",
            "Referer": "https://fod.fujitv.co.jp/",
        })
        
        if self.auth_method == 'credentials':
            self._configure_credentials()
        else:
            self._configure_cookies()
    
    def _configure_cookies(self):
        """Configure cookie-based authentication."""
        self.log.info(" + Using cookie-based authentication...")
        self.session.headers.update({
            "User-Agent": self.WEB_USER_AGENT,
            "X-Forwarded-For": self.config.get("forwarded_ip", "35.77.152.175"),
        })
        
        if not self.session.cookies:
            raise ValueError(" - Cookies not found! Please provide cookies for this auth method.")
    
    def _configure_credentials(self) -> None:
        """Configure credential-based authentication with token caching."""
        cache_path = Path(self.get_cache("fod_token.json"))
        need_auth = True
        
        if cache_path.is_file():
            try:
                self.token = json.loads(cache_path.read_text())
                self.log.info(" + Validating token from cache...")
                validation_headers = {
                    "User-Agent": self.TV_USER_AGENT,
                    "X-Authorization": f"Bearer {self.token.get('access_token')}",
                    "X-Forwarded-For": self.config.get("forwarded_ip", "35.77.152.175")
                }
                if self._is_session_valid(validation_headers):
                    self.log.info(" + Token from cache is still valid.")
                    self.token["uuid"] = self.session_uuid
                    need_auth = False
                else:
                    self.log.info(" - Token from cache has expired or is invalid.")
            except Exception as e:
                self.log.warning(f" - Cached token invalid, re-authenticating: {e}")
        
        if need_auth:
            if not self._perform_login():
                raise ValueError(" - Initial authentication failed.")
        
        self.session.headers.update({
            "User-Agent": self.TV_USER_AGENT,
            "X-Authorization": f"Bearer {self.token['access_token']}",
        })
        if self.config.get("forwarded_ip"):
            self.session.headers["X-Forwarded-For"] = self.config["forwarded_ip"]
            self.log.info(f" + Using X-Forwarded-For header with IP: {self.config['forwarded_ip']}")
        
        self.session_uuid = self.token.get("uuid")
    
    def get_titles(self) -> Union[list[Title], Title]:
        """Fetch titles (episodes or movies) from FOD."""
        titles = list()
        
        headers = self.session.headers.copy()
        headers["User-Agent"] = self.WEB_USER_AGENT
        
        if self.auth_method == 'cookies':
            try:
                headers["X-Authorization"] = f"Bearer {self.session.cookies.get_dict()['CT']}"
            except KeyError:
                raise ValueError(" - 'CT' cookie is missing for cookie authentication.")
        
        req = self.session.get(
            url=self.config["endpoints"]["title"],
            headers=headers,
            params={
                "lu_id": self.title,
                "is_premium": "true",
                "dv_type": "web"
            },
        )
        data = req.json()
        self.log.debug(f"Title data received for: {self.title}")
        
        # Check if it's a movie (genre_id "JF" = Japanese Film)
        if data.get("genre", {}).get("genre_id") == "JF":
            # Handle Movies
            series_name = data.get("series", {}).get("series_name") or data.get("detail", {}).get("lu_title")
            year = None
            period = data.get("detail", {}).get("period")
            if period:
                try:
                    year = int(period)
                except (ValueError, TypeError):
                    year = None
            
            for episode in data.get("episodes", []):
                # Skip previews and trailers
                if "pv" in episode.get("sales_type", []) or "äºˆå‘Š" in episode.get("ep_title", ""):
                    continue
                titles.append(
                    Title(
                        id_=episode.get("ep_id"),
                        type_=Title.Types.MOVIE,
                        name=series_name,
                        year=year,
                        source=self.ALIASES[0],
                        service_data=episode
                    )
                )
            return titles
        else:
            # Handle TV Series
            season_number = 1
            seasons_data = data.get("series", {}).get("seasons", [])
            if seasons_data:
                for season in seasons_data:
                    if season.get("lu_id") == self.title:
                        season_number = season.get("season_no", 1)
                        break
            
            series_name = data.get("series", {}).get("series_name") or data.get("detail", {}).get("lu_title")
            year = None
            period = data.get("detail", {}).get("period")
            if period:
                try:
                    year = int(period)
                except (ValueError, TypeError):
                    year = None
            
            for episode in data.get("episodes", []):
                # Skip previews and trailers
                if "pv" in episode.get("sales_type", []) or "äºˆå‘Š" in episode.get("ep_title", ""):
                    continue
                
                # Parse episode number from sort_number (usually 10, 20, 30...)
                episode_num = 0
                if episode.get("sort_number"):
                    try:
                        episode_num = int(episode.get("sort_number", 0)) // 10
                    except (ValueError, TypeError):
                        episode_num = 0
                
                titles.append(
                    Title(
                        id_=episode.get("ep_id"),
                        type_=Title.Types.TV,
                        name=series_name,
                        episode_name=episode.get("ep_title"),
                        season=season_number,
                        episode=episode_num,
                        year=year,
                        source=self.ALIASES[0],
                        service_data=episode
                    )
                )
            
            # If not single season and there are more seasons, fetch them
            if not self.single and seasons_data:
                self.single = True
                for season in seasons_data:
                    season_id = season.get("lu_id")
                    if season_id and season_id != self.title:
                        # Store original title to restore later
                        original_title = self.title
                        self.title = season_id
                        try:
                            more_titles = self.get_titles()
                            if isinstance(more_titles, list):
                                titles.extend(more_titles)
                        finally:
                            self.title = original_title
            
            return titles
    
    def get_tracks(self, title: Title) -> Tracks:
        """Fetch tracks (video, audio, subtitles) for a title."""
        tracks = Tracks()
        data = None
        manifest_url = None
        used_web_endpoint = False
        
        for attempt in range(2):
            try:
                manifest_headers = self.session.headers.copy()
                req = None
                
                if self.auth_method == 'cookies':
                    req = self.session.get(
                        url=self.config["endpoints"]["manifest_web"],
                        cookies={
                            "U": self.session.cookies.get_dict()["U"],
                            "UP": self.session.cookies.get_dict()["UP"],
                        },
                        headers={
                            "X-Authorization": f"Bearer {self.session.cookies.get_dict()['UT']}",
                            "User-Agent": self.WEB_USER_AGENT
                        },
                        params={
                            "site_id": "fodapp",
                            "ep_id": title.id,
                            "qa": "auto",
                            "uuid": self.session.cookies.get("uuid"),
                            "starttime": "0",
                            "is_pt": "false",
                            "dt": ""
                        }
                    )
                else:
                    manifest_headers['User-Agent'] = self.TV_USER_AGENT
                    uuid_to_use = str(uuid.uuid4())
                    self.log.debug(f"Generated new episode UUID: {uuid_to_use}")
                    
                    try:
                        req = self.session.get(
                            url=self.config["endpoints"]["manifest_tv"],
                            headers=manifest_headers,
                            params={
                                "site_id": "fodapp",
                                "ep_id": title.id,
                                "qa": "auto",
                                "uuid": uuid_to_use,
                                "starttime": "0",
                                "wvsl": "3",
                                "dv_type": "tv"
                            }
                        )
                        req.raise_for_status()
                        self.log.debug("Using TV endpoint")
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 400:
                            self.log.warning(f"TV endpoint returned 400 for {title.id}, trying web endpoint...")
                            used_web_endpoint = True
                            req = self.session.get(
                                url=self.config["endpoints"]["manifest_web"],
                                headers={
                                    "User-Agent": self.WEB_USER_AGENT,
                                    "X-Authorization": f"Bearer {self.token['access_token']}",
                                    "X-Forwarded-For": self.config.get("forwarded_ip", "35.77.152.175")
                                },
                                params={
                                    "site_id": "fodapp",
                                    "ep_id": title.id,
                                    "qa": "auto",
                                    "uuid": uuid_to_use,
                                    "starttime": "0",
                                    "is_pt": "false",
                                    "dt": ""
                                }
                            )
                            req.raise_for_status()
                        else:
                            raise
                
                req.raise_for_status()
                data = req.json()
                self.stream_data = data
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [401, 403] and attempt == 0 and self.auth_method == 'credentials':
                    self.log.warning("Authentication token may have expired. Re-authenticating...")
                    if self._perform_login():
                        self.session.headers["X-Authorization"] = f"Bearer {self.token['access_token']}"
                        continue
                    else:
                        raise ValueError(" - Re-authentication failed.")
                raise ValueError(f" - Failed to get manifest: {e}")
        
        if not data or "url" not in data:
            raise ValueError(f" - Failed to Get Manifest! Response: {data}")
        
        manifest_url = data["url"]
        self.log.debug(f"Manifest URL: {manifest_url}")
        
        # Modify manifest URL for better quality when using web/cookie auth
        if self.auth_method == 'cookies' or used_web_endpoint:
            try:
                manifest_url = (manifest_url
                    .replace("fod-wse-svod", "fod-wse-avod")
                    .replace("113abr.mpd", "117abr.mpd")
                    .replace("113.mpd", "117abr.mpd")
                    .replace("me113", "me117")
                    .replace("fod-plus7", "fod-plus7-high"))
                self.log.debug(f"Modified manifest URL for better quality: {manifest_url}")
            except Exception:
                self.log.warning(" - Failed to modify Manifest URL!")
        
        # Get manifest content
        manifest_response = self.session.get(url=manifest_url)
        manifest_response.raise_for_status()
        manifest_data = manifest_response.text
        self.log.debug(f"Manifest content type: {manifest_response.headers.get('content-type', 'unknown')}")
        
        # Check if it's HLS (M3U8) or DASH (MPD)
        if manifest_url.endswith('.m3u8') or 'm3u8' in manifest_response.headers.get('content-type', '').lower() or manifest_data.strip().startswith('#EXTM3U'):
            # HLS stream
            self.log.info(" + Detected HLS manifest")
            try:
                try:
                    master_playlist = m3u8.loads(manifest_data, uri=manifest_url)
                except TypeError:
                    # Fallback without uri
                    master_playlist = m3u8.M3U8(manifest_data, base_uri=manifest_url)
                
                from fuckdl.parsers import m3u8 as m3u8_parser
                hls_tracks = m3u8_parser.parse(master_playlist, source=self.ALIASES[0])
                tracks.add(hls_tracks)
                
                for track in tracks:
                    track.encrypted = False
                    
            except Exception as e:
                self.log.error(f"Failed to parse HLS manifest: {e}")
                # Fallback: create basic tracks
                video_track = VideoTrack(
                    id_=md5(manifest_url.encode()).hexdigest()[0:6],
                    source=self.ALIASES[0],
                    url=manifest_url,
                    codec="h264",
                    language=None,
                    bitrate=0,
                    width=0,
                    height=0,
                    fps=None,
                    hdr10=False,
                    dv=False,
                    descriptor=Track.Descriptor.M3U,
                    encrypted=False
                )
                tracks.add(video_track)
                
                audio_track = AudioTrack(
                    id_=md5((manifest_url + "_audio").encode()).hexdigest()[0:6],
                    source=self.ALIASES[0],
                    url=manifest_url,
                    codec="aac",
                    language=Language.get("ja"),
                    bitrate=0,
                    channels="2.0",
                    descriptor=Track.Descriptor.M3U,
                    encrypted=False
                )
                tracks.add(audio_track)
        else:
            # DASH stream
            self.log.info(" + Detected DASH manifest")
            try:
                # Check if it's valid XML before parsing
                if not manifest_data.strip().startswith('<?xml') and not manifest_data.strip().startswith('<MPD'):
                    self.log.error(f"Invalid DASH manifest (not XML): {manifest_data[:200]}")
                    raise ValueError(" - Manifest is not valid DASH/XML")
                
                tracks = Tracks.from_mpd(url=manifest_url, data=manifest_data, session=self.session, source=self.ALIASES[0])
            except Exception as e:
                self.log.error(f"Failed to parse DASH manifest: {e}")
                raise ValueError(f" - DASH parsing failed: {e}")
        
        # Add subtitles
        subtitle_url = None
        if data and data.get("vtt_url"):
            subtitle_url = data.get("vtt_url")
        elif data and data.get('lu_id') and data.get('mediaid'):
            subtitle_url = f"https://90eb6371cc8f0687441fdfe2c0d32ea0.cdnext.stream.ne.jp/{data.get('lu_id')}/{data.get('mediaid')}_ja.vtt"
        
        if subtitle_url:
            try:
                sub_response = self.session.get(url=subtitle_url)
                content_text = sub_response.text.strip()
                if sub_response.ok and content_text.startswith("WEBVTT"):
                    # Check if subtitle already exists
                    subtitle_exists = False
                    for existing_track in tracks:
                        if isinstance(existing_track, TextTrack) and existing_track.url == subtitle_url:
                            subtitle_exists = True
                            break
                    
                    if not subtitle_exists:
                        self.log.info(f" + Adding subtitle: {subtitle_url}")
                        tracks.add(TextTrack(
                            id_=md5(subtitle_url.encode()).hexdigest()[0:6],
                            url=subtitle_url,
                            codec=TextTrack.Codec.WebVTT,
                            language=Language.get("ja"),
                            source=self.ALIASES[0],
                            forced=False,
                            sdh=False,
                        ))
            except Exception as e:
                self.log.debug(f" - Error checking subtitle: {e}")
        
        # Set audio language to Japanese for all audio tracks
        for track in tracks:
            track.extra = data
            if isinstance(track, AudioTrack):
                if not track.language:
                    track.language = Language.get("ja")
                track.is_original_lang = True
        
        return tracks
    
    def get_chapters(self, title: Title) -> list[MenuTrack]:
        """Fetch chapters for a title."""
        chapters = []
        
        def to_hms(s):
            if s is None:
                return None
            try:
                val = float(s)
                if val <= 0.1:
                    return None
            except (ValueError, TypeError):
                return None
            m, s = divmod(val, 60)
            h, m = divmod(m, 60)
            return "{:02}:{:02}:{:06.3f}".format(int(h), int(m), s)
        
        skip_data = []
        if self.stream_data:
            skip_data = self.stream_data.get("skip", [])
            if not skip_data and isinstance(self.stream_data.get("chapters"), list):
                skip_data = self.stream_data.get("chapters")
        
        if skip_data:
            for item in skip_data:
                start = item.get("start") or item.get("start_time") or item.get("intro_start")
                end = item.get("end") or item.get("end_time") or item.get("intro_end")
                label = item.get("label") or item.get("type") or "Chapter"
                label_prefix = str(label).capitalize()
                
                if start and to_hms(start):
                    chapters.append(MenuTrack(
                        name=f"{label_prefix} Start",
                        timestamp=to_hms(start)
                    ))
                if end and to_hms(end):
                    chapters.append(MenuTrack(
                        name=f"{label_prefix} End",
                        timestamp=to_hms(end)
                    ))
        
        return sorted(chapters, key=lambda x: x.timestamp)
    
    def license(self, challenge: bytes, title: Title, track: Track, *_, **__) -> Optional[Union[bytes, str]]:
        """Get Widevine or PlayReady license."""
        # For PlayReady, the challenge might need different handling
        if self._is_playready:
            self.log.debug(f"[FOD] PlayReady license request for track: {track.id if track else 'unknown'}")
        
        req = self.session.post(
            url=self.config["endpoints"]["license"],
            data=challenge,
            params={"custom_data": track.extra.get("ticket") if track.extra else None},
        )
        self.log.debug(f"[FOD] Using ticket: {track.extra.get('ticket') if track.extra else 'None'}")
        return req.content
    
    def certificate(self, challenge: bytes, title: Title, track: Track, session_id: str) -> Optional[Union[bytes, str]]:
        """Get service certificate if needed (for Widevine)."""
        # Some services need a service certificate, FOD might not
        # Return the challenge or None to use default
        return self.license(challenge, title, track, session_id)
    
    def parse_title(self, ctx, title):
        """Parse title ID from URL or direct ID."""
        title = title or ctx.parent.params.get("title") if ctx.parent else title
        if not title:
            self.log.error(" - No title ID specified")
            return {}
        
        regexes = self.TITLE_RE if isinstance(self.TITLE_RE, list) else [self.TITLE_RE]
        for regex in regexes:
            m = re.search(regex, title)
            if m:
                return m.groupdict()
        self.log.warning(f" - Unable to parse title ID {title!r}, using as-is")
        return {"id": title}