import base64
import json
import re
import click
import os
import uuid
import xmltodict
from typing import Optional

from langcodes import Language
from fuckdl.objects import TextTrack, Title, Tracks, VideoTrack
from fuckdl.objects.tracks import AudioTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.vendor.pymp4.parser import Box


class Mubi(BaseService):
    """
    Service code for MUBI (https://mubi.com)

    \b
    Authorization: Cookies (lt + _mubi_session) OR Credentials
    Security: UHD@L3
    Supports: Movies and Series

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["MUBI", "mubi"]
    
    # Regex patterns for movies and series
    FILM_TITLE_RE = r"^(?:https?://(?:www\.)?mubi\.com)(?:/[^/]+)*?/films/(?P<slug>[^/?#]+)$"
    SERIES_TITLE_RE = r"^https?://(?:www\.)?mubi\.com(?:/[^/]+)*?/series/(?P<series_slug>[^/]+)(?:/season/(?P<season_slug>[^/]+))?$"
    
    TITLE_RE = [
        FILM_TITLE_RE,
        SERIES_TITLE_RE
    ]

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "AC3": "ac-3",
        "EC3": "ec-3"
    }

    @staticmethod
    @click.command(name="Mubi", short_help="https://mubi.com")
    @click.argument("title", type=str, required=False)
    @click.option("--cookies", type=str, help="Cookies string (lt and _mubi_session required)")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Mubi(ctx, **kwargs)

    def __init__(self, ctx, title, cookies=None):
        super().__init__(ctx)
        self.ctx = ctx  # Save context
        
        # Parse title to determine if it's a series or movie
        self.is_series = False
        self.slug = None
        self.series_slug = None
        self.season_slug = None
        self.title_id = None
        
        m_film = re.match(self.FILM_TITLE_RE, title) if title else None
        m_series = re.match(self.SERIES_TITLE_RE, title) if title else None
        
        if m_film:
            self.slug = m_film.group("slug")
        elif m_series:
            self.is_series = True
            self.series_slug = m_series.group("series_slug")
            self.season_slug = m_series.group("season_slug")
            self.log.info(f"Detected series: {self.series_slug}")
        else:
            # Legacy mode - direct ID
            if title and re.match(r'^\d+$', title):
                self.title_id = title
                self.log.info(f"Using legacy film ID: {self.title_id}")
            else:
                raise ValueError(f"Invalid MUBI URL or ID: {title}")
        
        # Video/audio parameters
        self.vcodec = (ctx.parent.params.get("vcodec") or "h264").lower()
        self.acodec = ctx.parent.params.get("acodec")
        self.range = ctx.parent.params["range_"]
        self.quality = ctx.parent.params.get("quality")
        self.cookies_str = cookies
        
        # Authentication variables
        self.lt_token = None
        self.session_token = None
        self.user_id = None
        self.country_code = None
        self.anonymous_user_id = None
        self.bearer = None
        self.dtinfo = None
        
        # Headers
        self.headers = {
            "authority": "api.mubi.com",
            "accept": "application/json",
            "accept-language": "en-US",
            "client": self.config["device"]["client_name"],
            "client-version": "20.2",
            "client-device-identifier": self.config["device"]["device_identifier"],
            "client-app": "mubi",
            "client-device-brand": "Google",
            "client-accept-audio-codecs": "eac3, ac3, aac",
            "client-device-model": self.config["device"]["device_model"],
            "client-device-os": self.config["device"]["device_os"],
            "client-country": "US",
            "content-type": "application/json; charset=UTF-8",
            "host": "api.mubi.com",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip",
            "user-agent": self.config["device"]["user_agent"],
        }
        
        # Video codec configuration
        if self.vcodec == "vp9":
            self.headers["client-accept-video-codecs"] = "vp9"
        elif self.vcodec == "h265":
            self.headers["client-accept-video-codecs"] = "h265"
        elif self.vcodec == "h264":
            self.headers["client-accept-video-codecs"] = "h264"
        else:
            self.headers["client-accept-video-codecs"] = "vp9,h265,h264"
        
        self.configure()

    def _load_cookies(self):
        """Load cookies from file or parameter"""
        cookies_str = None
        
        # 1. Try from command line parameter
        if self.cookies_str:
            cookies_str = self.cookies_str
            self.log.info(" + Using cookies from command line")
        
        # 2. Try from cookie file
        if not cookies_str:
            cookies_str = self._load_cookies_from_file()
        
        # Parse cookies if we have them
        if cookies_str:
            return self._parse_cookies(cookies_str)
        
        return None
    
    def _load_cookies_from_file(self):
        """Load cookies from the Fuckdl cookies directory"""
        # Try multiple possible locations
        possible_paths = [
            os.path.join(".Fuckdl", "cookies", "Mubi", "default.txt"),
            os.path.join("Fuckdl", "cookies", "Mubi", "default.txt"),
            os.path.join(os.path.expanduser("~"), ".Fuckdl", "cookies", "Mubi", "default.txt"),
            os.path.join("C:", "DRMLab", "Fuckdl", "cookies", "Mubi", "default.txt"),
        ]
        
        # Also try using cookie_dir from context if available
        if hasattr(self, 'ctx') and hasattr(self.ctx, 'obj') and hasattr(self.ctx.obj, 'cookie_dir'):
            cookie_dir = self.ctx.obj.cookie_dir
            possible_paths.insert(0, os.path.join(cookie_dir, "Mubi", "default.txt"))
        
        for cookie_file_path in possible_paths:
            if os.path.isfile(cookie_file_path):
                try:
                    with open(cookie_file_path, 'r', encoding='utf-8') as f:
                        cookies = f.read().strip()
                    
                    # Debug: show what we're reading
                    self.log.debug(f"Reading cookie file: {cookie_file_path}")
                    self.log.debug(f"File content preview: {cookies[:200]}...")
                    
                    # Check if we have the required cookies
                    if 'lt=' in cookies or 'lt\t' in cookies:
                        self.log.info(f" + Loaded cookies from: {cookie_file_path}")
                        return cookies
                    else:
                        self.log.warning(f" - Cookie file found but 'lt' cookie not in file")
                        
                except Exception as e:
                    self.log.warning(f" - Could not read cookie file {cookie_file_path}: {e}")
        
        self.log.debug(" - No cookie file found in standard locations")
        return None
    
    def _parse_cookies(self, cookies_str):
        """Parse cookies string into dictionary, handling Netscape format"""
        cookies = {}
        
        # Check if this is a Netscape format cookie file
        if 'Netscape HTTP Cookie File' in cookies_str or 'mubi.com\t' in cookies_str:
            # Parse Netscape format
            lines = cookies_str.split('\n')
            for line in lines:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith('#') or line.startswith('//'):
                    continue
                
                # Parse Netscape format: domain\tflag\tpath\tsecure\texpiration\tname\tvalue
                parts = line.split('\t')
                if len(parts) >= 7:
                    name = parts[5].strip()
                    value = parts[6].strip()
                    cookies[name] = value
        else:
            # Parse simple format: name1=value1; name2=value2
            for cookie in cookies_str.split(';'):
                cookie = cookie.strip()
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    cookies[key] = value
        
        return cookies
    def configure(self):
        """Configure authentication using cookies or credentials"""
        # Try to load cookies first (from file or parameter)
        cookies = self._load_cookies()
        
        if cookies:
            # Authenticate with cookies (Unshackle method)
            self._authenticate_with_cookies(cookies)
        else:
            # Try credentials-based authentication (legacy VT method)
            self._authenticate_with_credentials()
    
    def _authenticate_with_cookies(self, cookies):
        """Authenticate using cookies (Unshackle style)"""
        self.log.info(" + Authenticating with cookies...")
        
        # Extract essential tokens from cookies
        self.lt_token = cookies.get('lt')
        self.session_token = cookies.get('_mubi_session')
        snow_id = cookies.get('_snow_id.c006')
        
        if not self.lt_token:
            raise self.log.exit(" - Missing 'lt' cookie (Bearer token).")
        if not self.session_token:
            raise self.log.exit(" - Missing '_mubi_session' cookie.")
        
        # Set authorization header
        self.headers["authorization"] = f"Bearer {self.lt_token}"
        
        # Extract anonymous_user_id
        if snow_id and "." in snow_id:
            self.anonymous_user_id = snow_id.split(".")[0]
        else:
            self.anonymous_user_id = str(uuid.uuid4())
            self.log.warning(" - No _snow_id.c006 cookie found, generated anonymous ID")
        
        # Get geolocation
        self._get_geolocation()
        
        # Update headers with additional info
        self.headers.update({
            "ANONYMOUS_USER_ID": self.anonymous_user_id,
            "Client-Country": self.country_code or "US",
            "Origin": "https://mubi.com",
            "Referer": "https://mubi.com/",
        })
        
        # Get account information
        self._get_account_info()
        
        # Bind anonymous user ID
        self._bind_anonymous_user()
        
        self.log.info(f" + Authenticated as user: {self.user_id}, country: {self.country_code}")
    
    def _authenticate_with_credentials(self):
        """Authenticate using username/password (legacy VT method)"""
        self.log.info(" + Authenticating with credentials...")
        
        # Check if we have cached tokens
        tokens_cache_path = self.get_cache("tokens_mubi.json")
        
        if os.path.isfile(tokens_cache_path):
            try:
                with open(tokens_cache_path, encoding="utf-8") as fd:
                    tokens = json.load(fd)
                self.bearer = tokens.get("authorization")
                self.dtinfo = tokens.get("dt-custom-data")
                
                if self.bearer and self.dtinfo:
                    self.headers["authorization"] = self.bearer
                    self.log.info(" + Using cached authentication tokens")
                    return
            except Exception as e:
                self.log.warning(f" - Could not load cached tokens: {e}")
        
        # Need fresh authentication
        if not hasattr(self, 'credentials') or not self.credentials or not self.credentials.username:
            raise self.log.exit("""
 - MUBI requires authentication.

 Options:
   1. Add cookies to: .Fuckdl\\cookies\\Mubi\\default.txt
      Format: lt=xxx; _mubi_session=yyy

   2. Use --cookies parameter:
      --cookies "lt=xxx; _mubi_session=yyy"

   3. Set credentials in fuckdl.yml:
        mubi:
          username: "your_email@example.com"
          password: "your_password"

   4. Enter credentials when prompted
""")
        
        # Get password if not in config
        if not self.credentials.password:
            import getpass
            self.credentials.password = getpass.getpass("Enter MUBI password: ")
        
        # Request authentication token
        req_payload = json.dumps({
            "identifier": self.credentials.username,
            "magic_link": True
        })
        
        try:
            auth_resp = self.session.post(
                url=self.config["endpoints"]["authtok_url"],
                data=req_payload,
                headers=self.headers
            ).json()
            
            # Login with credentials
            payload = json.dumps({
                "auth_request_token": auth_resp["auth_request_token"],
                "identifier": self.credentials.username,
                "password": self.credentials.password
            })
            
            response = self.session.post(
                url=self.config["endpoints"]["loginurl"],
                data=payload,
                headers=self.headers
            ).json()
            
            # Prepare dt-custom-data for DRM
            json_str = {
                "merchant": "mubi",
                "sessionId": response["token"],
                "userId": str(response["user"]["id"])
            }
            
            self.bearer = "Bearer " + response["token"]
            self.dtinfo = base64.b64encode(json.dumps(json_str).encode('utf-8')).decode('utf-8')
            self.user_id = response["user"]["id"]
            self.headers["authorization"] = self.bearer
            
            # Save tokens to cache
            save_data = {
                "authorization": self.bearer,
                "dt-custom-data": self.dtinfo
            }
            
            os.makedirs(os.path.dirname(tokens_cache_path), exist_ok=True)
            with open(tokens_cache_path, "w", encoding="utf-8") as fd:
                json.dump(save_data, fd)
            
            self.log.info(f" + Authentication successful, tokens saved to cache")
            
        except Exception as e:
            self.log.error(f" - Authentication failed: {e}")
            raise self.log.exit(" - Could not authenticate with credentials.")
    
    def _get_geolocation(self):
        """Get geolocation from IP"""
        try:
            r_ip = self.session.get(self.config["endpoints"]["ip_geolocation"], timeout=5)
            if r_ip.ok:
                ip_data = r_ip.json()
                self.country_code = ip_data.get("country", "US")
                self.log.debug(f"Detected country from IP: {self.country_code}")
        except Exception as e:
            self.country_code = "US"
            self.log.debug(f"IP geolocation failed, using default: US ({e})")
    
    def _get_account_info(self):
        """Get account information"""
        try:
            r_account = self.session.get(self.config["endpoints"]["account"], headers=self.headers)
            if r_account.ok:
                account_data = r_account.json()
                self.user_id = account_data.get("id")
                
                # Get country from account
                country_data = account_data.get("country", {})
                if isinstance(country_data, dict):
                    account_country = country_data.get("code")
                    if account_country:
                        self.country_code = account_country
                        self.headers["Client-Country"] = self.country_code
        except Exception as e:
            self.log.warning(f"Could not get account info: {e}")
    
    def _bind_anonymous_user(self):
        """Bind anonymous user ID to account"""
        try:
            r = self.session.put(
                self.config["endpoints"]["current_user"],
                json={"anonymous_user_uuid": self.anonymous_user_id},
                headers={"Content-Type": "application/json"}
            )
            if r.ok:
                self.log.debug("Anonymous user ID bound to account")
        except Exception as e:
            self.log.debug(f"Could not bind anonymous user ID: {e}")

    def get_titles(self):
        if self.is_series:
            return self._get_series_titles()
        elif self.slug:
            return self._get_film_title_by_slug()
        else:
            return self._get_film_title_by_id()

    def _get_film_title_by_slug(self):
        self.log.info(f" + Getting film metadata by slug: {self.slug}")
        
        url = self.config["endpoints"]["film_by_slug"].format(slug=self.slug)
        response = self.session.get(url, headers=self.headers)
        
        if not response.ok:
            raise self.log.exit(f" - Failed to get film metadata: {response.status_code}")
        
        data = response.json()
        self.title_id = data["id"]
        
        # Get original language from reels
        original_language = "en"
        try:
            url_reels = self.config["endpoints"]["reels"].format(film_id=self.title_id)
            r_reels = self.session.get(url_reels, headers=self.headers)
            if r_reels.ok:
                reels_data = r_reels.json()
                if reels_data and reels_data[0].get("audio_tracks"):
                    first_audio = reels_data[0]["audio_tracks"][0]
                    if "language_code" in first_audio:
                        original_language = first_audio["language_code"]
        except Exception as e:
            self.log.debug(f"Could not fetch reels for language detection: {e}")
        
        return Title(
            id_=self.title_id,
            type_=Title.Types.MOVIE,
            name=data["title"],
            year=data.get("year"),
            source=self.ALIASES[0],
            service_data={
                "film_data": data,
                "original_lang": original_language
            }
        )

    def _get_film_title_by_id(self):
        self.log.info(f" + Getting film metadata by ID: {self.title_id}")
        
        response = self.session.get(
            self.config["endpoints"]["metadata"].format(title_id=self.title_id),
            headers=self.headers
        )
        
        if not response.ok:
            raise self.log.exit(f" - Failed to get film metadata: {response.status_code}")
        
        data = response.json()
        
        return Title(
            id_=self.title_id,
            type_=Title.Types.MOVIE,
            name=data["title"],
            year=data.get("year"),
            source=self.ALIASES[0],
            service_data=data
        )

    def _get_series_titles(self):
        self.log.info(f" + Getting series metadata: {self.series_slug}")
        
        # Get series metadata
        series_url = self.config["endpoints"]["series"].format(series_slug=self.series_slug)
        r_series = self.session.get(series_url, headers=self.headers)
        
        if not r_series.ok:
            raise self.log.exit(f" - Failed to get series metadata: {r_series.status_code}")
        
        series_data = r_series.json()
        episodes = []
        
        # Get episodes for specific season or all seasons
        if self.season_slug:
            episodes_data = self._get_season_episodes(self.series_slug, self.season_slug)
            self._add_episodes_to_list(episodes, episodes_data, series_data)
        else:
            # Get all seasons - but we need to make sure not to duplicate
            seasons = series_data.get("seasons", [])
            if not seasons:
                raise self.log.exit(" - No seasons found for this series.")
            
            self.log.info(f" + Found {len(seasons)} season(s)")
            
            # Use a set to avoid duplicate seasons
            processed_seasons = set()
            
            for season in seasons:
                season_slug = season.get("slug")
                season_number = season.get("number")
                
                # Avoid processing the same season twice
                if season_slug in processed_seasons:
                    self.log.debug(f" - Season {season_number} ({season_slug}) already processed, skipping")
                    continue
                
                processed_seasons.add(season_slug)
                
                try:
                    self.log.info(f" + Getting episodes for season {season_number}...")
                    episodes_data = self._get_season_episodes(self.series_slug, season_slug)
                    
                    if episodes_data:
                        self._add_episodes_to_list(episodes, episodes_data, series_data)
                        self.log.info(f" + Found {len(episodes_data)} episodes in season {season_number}")
                    else:
                        self.log.warning(f" - No episodes found in season {season_number}")
                        
                except ValueError as e:
                    # Season not found
                    self.log.info(f" - Season {season_number} not available: {e}")
                except Exception as e:
                    self.log.warning(f" - Error getting season {season_number}: {e}")
        
        if not episodes:
            raise self.log.exit(" - No episodes found for this series.")
        
        self.log.info(f" + Total episodes collected: {len(episodes)}")
        
        return sorted(episodes, key=lambda x: (x.season, x.episode))
    
    def _get_season_episodes(self, series_slug, season_slug):
        """Get episodes for a specific season"""
        eps_url = self.config["endpoints"]["season_episodes"].format(
            series_slug=series_slug,
            season_slug=season_slug
        )
        
        r_eps = self.session.get(eps_url, headers=self.headers)
        
        if r_eps.status_code == 404:
            raise ValueError(f"Season '{season_slug}' not found.")
        
        if not r_eps.ok:
            raise self.log.exit(f" - Failed to get episodes: {r_eps.status_code}")
        
        data = r_eps.json()
        episodes = data.get("episodes", [])
        
        # Filter duplicate episodes by ID
        unique_episodes = []
        seen_ids = set()
        
        for ep in episodes:
            ep_id = ep.get("id")
            if ep_id and ep_id not in seen_ids:
                seen_ids.add(ep_id)
                unique_episodes.append(ep)
            elif ep_id:
                self.log.debug(f" - Skipping duplicate episode ID: {ep_id}")
        
        return unique_episodes

    def _add_episodes_to_list(self, episodes_list, episodes_data, series_data):
        """Helper to add episodes to list, avoiding duplicates"""
        added_count = 0
        duplicate_count = 0
        
        for ep in episodes_data:
            ep_id = ep.get("id")
            
            # Check if an episode with this ID already exists
            if any(existing_ep.id == ep_id for existing_ep in episodes_list):
                duplicate_count += 1
                continue
            
            # Detect language
            playback_langs = ep.get("consumable", {}).get("playback_languages", {})
            audio_langs = playback_langs.get("audio_options", ["English"])
            lang_code = audio_langs[0].split()[0].lower() if audio_langs else "en"
            
            # Create full episode name
            series_title = series_data.get("title", "Unknown Series")
            episode_title = ep.get("title", f"Episode {ep['episode']['number']}")
            season_num = ep["episode"]["season_number"]
            episode_num = ep["episode"]["number"]
            
            full_name = f"{series_title} - S{season_num:02d}E{episode_num:02d} - {episode_title}"
            
            episodes_list.append(Title(
                id_=ep_id,
                type_=Title.Types.TV,
                name=full_name,
                season=season_num,
                episode=episode_num,
                source=self.ALIASES[0],
                service_data={
                    "episode_data": ep,
                    "series_data": series_data,
                    "original_lang": lang_code
                }
            ))
            added_count += 1
        
        if duplicate_count > 0:
            self.log.debug(f" - Skipped {duplicate_count} duplicate episodes")
        
        return added_count

    def create_pssh_from_kid(self, kid: str):
        """Create PSSH from KID"""
        WV_SYSTEM_ID = [237, 239, 139, 169, 121, 214, 74, 206, 163, 200, 39, 220, 213, 29, 33, 237]
        kid = uuid.UUID(kid).bytes

        init_data = bytearray(b'\x12\x10')
        init_data.extend(kid)
        init_data.extend(b'H\xe3\xdc\x95\x9b\x06')

        pssh = bytearray([0, 0, 0])
        pssh.append(32 + len(init_data))
        pssh[4:] = bytearray(b'pssh')
        pssh[8:] = [0, 0, 0, 0]
        pssh[13:] = WV_SYSTEM_ID
        pssh[29:] = [0, 0, 0, 0]
        pssh[31] = len(init_data)
        pssh[32:] = init_data

        return base64.b64encode(pssh).decode('UTF-8')
    
    def get_pssh_from_mpd(self, mpd_url):
        """Extract PSSH from MPD"""
        response = self.session.get(mpd_url, headers=self.headers)

        if response.status_code != 200:
            raise Exception(response.text)

        mpd = xmltodict.parse(response.text, dict_constructor=dict)

        # Find video adaptation set with DRM
        period = mpd.get('MPD', {}).get('Period')
        if not period:
            raise Exception("No Period found in MPD")
        
        adaptation_sets = period.get('AdaptationSet', [])
        if isinstance(adaptation_sets, dict):
            adaptation_sets = [adaptation_sets]
        
        for adaptation_set in adaptation_sets:
            mime_type = adaptation_set.get('@mimeType', '')
            if mime_type.startswith('video/'):
                content_protection = adaptation_set.get('ContentProtection')
                if content_protection:
                    if isinstance(content_protection, list):
                        for cp in content_protection:
                            if cp.get('@schemeIdUri', '').lower() == 'urn:mpeg:dash:mp4protection:2011':
                                if '@cenc:default_KID' in cp:
                                    return self.create_pssh_from_kid(cp['@cenc:default_KID'])
                    elif content_protection.get('@schemeIdUri', '').lower() == 'urn:mpeg:dash:mp4protection:2011':
                        if '@cenc:default_KID' in content_protection:
                            return self.create_pssh_from_kid(content_protection['@cenc:default_KID'])
        
        raise Exception("No PSSH found in MPD")

    def get_tracks(self, title: Title):
        self.log.info(f" + Getting tracks for title ID: {title.id}")
        
        # Get original language from service_data
        original_lang = "en"
        if hasattr(title, 'service_data') and isinstance(title.service_data, dict):
            original_lang = title.service_data.get("original_lang", "en")
        
        # Start viewing session
        try:
            # Try v4 API first
            url_view = self.config["endpoints"]["initiate_viewing"].format(film_id=title.id)
            response = self.session.post(
                url_view,
                json={},
                headers={**self.headers, "Content-Type": "application/json"}
            )
            
            if response.ok:
                # Use v4 secure URL
                secure_url = self.config["endpoints"]["secure_url"].format(film_id=title.id)
            else:
                raise Exception("v4 endpoint failed")
        except Exception:
            # Fallback to v3 API
            try:
                response = self.session.post(
                    self.config["endpoints"]["viewing"].format(title_id=title.id),
                    headers=self.headers
                )
                if response.ok:
                    data = response.json()
                    lang = data.get("audio_track_id", "")
                    if lang:
                        original_lang = lang.replace('audio_main_', '')
                    secure_url = self.config["endpoints"]["manifest"].format(title_id=title.id)
                else:
                    raise Exception("v3 endpoint failed")
            except Exception as e:
                raise self.log.exit(f"Failed to start viewing session: {e}")
        
        # Get manifest URL
        response = self.session.get(secure_url, headers=self.headers)
        if not response.ok:
            raise self.log.exit(f"Failed to get secure URL: {response.status_code}")
        
        data = response.json()
        
        # Extract MPD URL
        mpd_url = None
        if data.get("url"):
            mpd_url = data["url"]
        elif data.get("urls"):
            for entry in data.get("urls", []):
                if entry.get("content_type") == "application/dash+xml":
                    mpd_url = entry["src"]
                    break
        
        if not mpd_url:
            raise self.log.exit("No DASH manifest URL found")
        
        # Adjust quality if requested
        if self.quality == 2160:
            mpd_url = re.sub(r"/default/.*\.mpd$", "/default/2160.mpd", mpd_url)
        
        # Get PSSH
        try:
            pssh = self.get_pssh_from_mpd(mpd_url)
            video_pssh = Box.parse(base64.b64decode(pssh))
        except Exception as e:
            self.log.warning(f"Could not get PSSH from MPD: {e}")
            # Create dummy PSSH
            pssh = self.create_pssh_from_kid(str(uuid.uuid4()))
            video_pssh = Box.parse(base64.b64decode(pssh))
        
        # Get tracks from MPD
        tracks = Tracks.from_mpd(
            url=mpd_url,
            session=self.session,
            source=self.ALIASES[0],
        )
        
        # Add PSSH to tracks
        for track in tracks.videos:
            if not hasattr(track, 'pssh') or not track.pssh:
                track.pssh = video_pssh
                track.psshWV = pssh
        
        for track in tracks.audios:
            if not hasattr(track, 'pssh') or not track.pssh:
                track.pssh = video_pssh
                track.psshWV = pssh
        
        # Filter by audio codec if specified
        if self.acodec:
            tracks.audios = [
                x for x in tracks.audios 
                if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]
            ]
        
        # Add subtitles if available
        if "text_track_urls" in data:
            subtitles = []
            for sub in data.get("text_track_urls", []):
                lang_code = sub.get("language_code", "und")
                vtt_url = sub.get("url")
                
                if not vtt_url:
                    continue
                
                subtitles.append(TextTrack(
                    id_=sub.get("id", f"sub_{lang_code}"),
                    source=self.ALIASES[0],
                    url=vtt_url,
                    language=Language.get(lang_code),
                    forced=False,
                    sdh=False,
                    codec="vtt"
                ))
            
            tracks.subtitles = subtitles
        
        return tracks

    def get_chapters(self, title: Title):
        return []

    def certificate(self, **kwargs):
        self.log.info(" + Certificate method called")
        return None

    def license(self, challenge, **kwargs):
        self.log.info(" + Requesting License...")
        
        # Prepare dt-custom-data based on authentication method
        dt_custom_data = None
        
        if self.dtinfo:
            # From credentials authentication
            dt_custom_data = self.dtinfo
        elif self.lt_token and self.user_id:
            # From cookies authentication
            dt_custom_data_json = {
                "userId": self.user_id,
                "sessionId": self.lt_token,
                "merchant": "mubi"
            }
            dt_custom_data = base64.b64encode(json.dumps(dt_custom_data_json).encode()).decode()
        else:
            # Try to load from cache
            tokens_cache_path = self.get_cache("tokens_mubi.json")
            if os.path.isfile(tokens_cache_path):
                try:
                    with open(tokens_cache_path, encoding="utf-8") as fd:
                        tokens = json.load(fd)
                    dt_custom_data = tokens.get("dt-custom-data")
                except:
                    pass
        
        if not dt_custom_data:
            # Create minimal dt-custom-data
            dt_custom_data_json = {
                "userId": self.user_id or "anonymous",
                "sessionId": self.lt_token or str(uuid.uuid4()),
                "merchant": "mubi"
            }
            dt_custom_data = base64.b64encode(json.dumps(dt_custom_data_json).encode()).decode()
            self.log.warning(" - Using fallback dt-custom-data, license may fail")
        
        # Prepare headers for license request
        headers = {
            "dt-custom-data": dt_custom_data,
            "Content-Type": "application/octet-stream",
            "User-Agent": self.config["device"]["user_agent"],
            "Accept": "*/*",
            "Origin": "https://mubi.com",
            "Referer": "https://mubi.com/",
        }
        
        try:
            response = self.session.post(
                url=self.config["endpoints"]["license"],
                headers=headers,
                data=challenge,
                timeout=30
            )
            
            self.log.info(f" + License Response Status: {response.status_code}")
            
            if response.status_code != 200:
                self.log.error(f" - License request failed: {response.status_code}")
                self.log.error(f" - Response: {response.text[:500]}")
                return None
            
            # Parse license response (DRM Today returns JSON)
            try:
                license_data = response.json()
                
                if license_data.get("status") == "OK" and "license" in license_data:
                    license_b64 = license_data["license"]
                elif "license" in license_data:
                    license_b64 = license_data["license"]
                elif "payload" in license_data:
                    license_b64 = license_data["payload"]
                else:
                    # Try to find license in any field
                    for key, value in license_data.items():
                        if isinstance(value, str) and len(value) > 100:
                            license_b64 = value
                            break
                    else:
                        return None
                
                license_bytes = base64.b64decode(license_b64)
                self.log.info(f" + License received: {len(license_bytes)} bytes")
                return license_bytes
                
            except json.JSONDecodeError:
                # Return binary response directly
                self.log.info(f" + License received: {len(response.content)} bytes (binary)")
                return response.content
            
        except Exception as e:
            self.log.error(f" - Exception during license request: {e}")
            return None
