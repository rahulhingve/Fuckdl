import re
import os
import json
import base64
import uuid
import time
from pathlib import Path
from typing import Any, Union, Dict, List, Optional, Tuple
from http.cookiejar import CookieJar

import click
import requests
from requests.adapters import HTTPAdapter, Retry
import yaml
from langcodes import Language

from fuckdl.objects import Title, Tracks, AudioTrack, TextTrack, Track, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.config import directories
from fuckdl.parsers.mpd import parse as parse_mpd

os.system("")
RED = '\033[31m'
YELLOW = '\033[33m'
RESET = '\033[0m'
GREEN = '\033[32m'
CYAN = '\033[36m'
BLUE = '\033[34m'
MAGENTA = '\033[35m'
ORANGE = '\033[38;5;208m'
WHITE = '\033[37m'


class MGMPlus(BaseService):
    """
    Service code for MGM+ streaming service (https://www.mgmplus.com).
    
    Version: 2.0.2
    Original Author: @sp4rk.y
    Author: @AnotherBigUserHere 
    
    Features:
    - Movies and Series compatibility
    - 4K UHD HEVC with Dolby Digital Plus / Atmos
    - Device pairing authentication flow
    - Automatic token refresh between episodes
    - PlayFlow with pre-roll resolution
    - Amazon HD fallback with bitrate comparison
    - Improved subtitle handling (WebVTT/SRT from Amazon)
    
    \b
    Authorization: Device Login Pair (cookies fallback)
    Security: 
    - WV L3 1080p/4K
    - BuyDRM KeyOS / Amazon Widevine

    Updates in v2.0.2:
    - Audio tracks now selected from Amazon if higher bitrate than MGM CENC
    - Stale web sessions are cleared between episodes
    - Better subtitle extraction from Amazon timedTextUrls
    - Improved error handling and fallback logic

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    """
    ALIASES = ["MGM+", "MGM", "MGMPlus", "EPIX", "mgmplus"]
    TITLE_RE = [
        r"^https?://(?:www\.)?mgmplus\.com/(?:watch/)?(?P<type>movies|series)/(?P<id>[a-zA-Z0-9-]+)",
        r"^https?://(?:www\.)?mgmplus\.com/movie/(?P<id>[a-zA-Z0-9-]+)",
        r"^https?://(?:www\.)?mgmplus\.com/series/(?P<id>[a-zA-Z0-9-]+)",
        r"^https?://(?:www\.)?mgmplus\.com/series/(?P<slug>[^/?#]+)/watch/season/(?P<season>\d+)/episode/(?P<episode>\d+)",
        r"^(?P<id>[a-zA-Z0-9-]+)"
    ]

    @staticmethod
    @click.command(name="MGMPlus", short_help="https://mgmplus.com", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--manifest-only", is_flag=True, default=False, help="Only get manifest URLs")
    @click.option("-s", "--series", is_flag=True, default=False, help="Force series mode")
    @click.option("-e", "--episode", is_flag=True, default=False, help="Force episode mode")
    @click.pass_context
    def cli(ctx, **kwargs):
        return MGMPlus(ctx, **kwargs)

    def __init__(self, ctx, title: str = None, manifest_only: bool = False, 
                 series: bool = False, episode: bool = False, **kwargs):
        self.session_token = None
        self.web_session_token = None
        self.aws_waf_token = None
        self.guid = None
        self.vendor_id = None
        self.user_data = {}
        self.playflow_data = {}
        self.manifest_only = manifest_only
        self.amazon_device_id = str(uuid.uuid4())
        
        # Load configuration
        self.config = self._load_config()
        
        # Parse title/ID
        if title:
            self.original_input = title
            parsed = self._parse_title_input(title)
            self.content_id = parsed["id"]
            self.content_type = parsed.get("kind", "movies")
            self.season = parsed.get("season")
            self.episode_num = parsed.get("episode")
            
            # Override with flags if provided
            if series:
                self.content_type = "series"
            elif episode:
                self.content_type = "episode"
        else:
            self.content_id = None
            self.content_type = "movies"
            self.season = None
            self.episode_num = None
        
        super().__init__(ctx)
        
        # Initialize GUID
        self.guid = self._get_or_create_guid()
        self.vendor_id = self._get_or_create_vendor_id()
        
        # Load cookies if provided
        if hasattr(ctx, 'parent') and ctx.parent and ctx.parent.params.get('cookies'):
            cookies_file = ctx.parent.params.get('cookies')
            import http.cookiejar
            try:
                jar = http.cookiejar.MozillaCookieJar(cookies_file)
                jar.load(ignore_discard=True, ignore_expires=True)
                self.cookies = jar
                self._extract_tokens_from_cookies()
            except Exception as e:
                self.log.error(f"Error loading cookies: {e}")
        
        self.setup_service()

    def _load_config(self) -> dict:
        """Load configuration from YAML file"""
        config_path = Path(__file__).parent / "mgmplus.yml"
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}

    def _parse_title_input(self, value: str) -> dict:
        """Parse title input to extract ID, type, season, episode"""
        text = value.strip()

        # Match series watch URL
        watch_match = re.search(r'mgmplus\.com/series/([^/?#]+)/watch/season/(\d+)/episode/(\d+)', text)
        if watch_match:
            return {
                "kind": "episode",
                "id": watch_match.group(1),
                "season": int(watch_match.group(2)),
                "episode": int(watch_match.group(3))
            }

        # Match movie URL
        movie_match = re.search(r'mgmplus\.com/movie/([^/?#]+)', text)
        if movie_match:
            return {"kind": "movies", "id": movie_match.group(1), "season": None, "episode": None}

        # Match series URL
        series_match = re.search(r'mgmplus\.com/series/([^/?#]+)', text)
        if series_match:
            return {"kind": "series", "id": series_match.group(1), "season": None, "episode": None}

        # Check for base64 prefixes
        if text.startswith("bW92aWU7"):  # movie;
            return {"kind": "movies", "id": text, "season": None, "episode": None}
        if text.startswith("c2VyaWVz"):  # series;
            return {"kind": "series", "id": text, "season": None, "episode": None}
        if text.startswith("ZXBpc29kZQ=="):  # episode;
            return {"kind": "episode", "id": text, "season": None, "episode": None}

        # Try to parse series-s01e01 format
        ep_pattern = r'(.+?)-s(\d{1,2})e(\d{1,2})'
        ep_match = re.match(ep_pattern, text.lower())
        if ep_match:
            return {
                "kind": "episode",
                "id": ep_match.group(1),
                "season": int(ep_match.group(2)),
                "episode": int(ep_match.group(3))
            }

        return {"kind": None, "id": text, "season": None, "episode": None}

    def _get_or_create_guid(self) -> str:
        """Get GUID from cache or create new one"""
        cache_file = Path(directories.cache) / "mgmplus_guid.txt"
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                return f.read().strip()
        guid = str(uuid.uuid4())
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            f.write(guid)
        return guid

    def _get_or_create_vendor_id(self) -> str:
        """Get vendor ID from cache or create new one"""
        cache_file = Path(directories.cache) / "mgmplus_vendor.txt"
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                return f.read().strip()
        vendor_id = uuid.uuid4().hex[:16]
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            f.write(vendor_id)
        return vendor_id

    def _decode_jwt_payload(self, token: str) -> dict:
        """Decode JWT payload without verification"""
        parts = token.split(".")
        if len(parts) < 2:
            raise ValueError("Invalid JWT")
        payload_b64 = parts[1] + ("=" * (-len(parts[1]) % 4))
        return json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8"))

    def _is_token_expired(self, token: str) -> bool:
        """Check if token is expired or will expire soon"""
        try:
            payload = self._decode_jwt_payload(token)
            return time.time() >= payload.get("exp", 0) - 60
        except Exception:
            return True

    def get_session(self):
        session = requests.Session()
        session.mount("https://", HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
            )
        ))
        
        headers = self.config.get("headers", {})
        session.headers.update(headers)
        
        if self.aws_waf_token:
            session.headers.update({'x-aws-waf-token': self.aws_waf_token})
        
        if self.cookies:
            if isinstance(self.cookies, dict):
                session.cookies.update(self.cookies)
            elif hasattr(self.cookies, '__iter__'):
                for cookie in self.cookies:
                    if hasattr(cookie, 'name') and hasattr(cookie, 'value'):
                        session.cookies.set(cookie.name, cookie.value, domain=cookie.domain, path=cookie.path)
        
        return session

    def _extract_tokens_from_cookies(self):
        """Extract tokens from cookies if available"""
        if not self.cookies:
            return
            
        if hasattr(self.cookies, '__iter__') and not isinstance(self.cookies, (dict, list)):
            try:
                for cookie in self.cookies:
                    if hasattr(cookie, 'name') and hasattr(cookie, 'value'):
                        if cookie.name == 'aws-waf-token':
                            self.aws_waf_token = cookie.value
                        elif cookie.name == 'epx_guid':
                            self.guid = cookie.value
            except Exception:
                pass

    def _create_device_session(self) -> dict:
        """Create device session via EPIX API"""
        session = self.get_session()
        
        device_info = self.config.get("client", {}).get("device", {}).copy()
        device_info["guid"] = self.guid
        device_info["vendor_id"] = self.vendor_id
        
        response = session.post(
            url=self.config["endpoints"]["sessions"],
            json={
                "apikey": self.config["client"]["apikey"],
                "amazon_receipt": {"receipt_id": "", "user_id": ""},
                "device": device_info
            }
        )
        response.raise_for_status()
        return response.json()

    def _get_registration_code(self, session_token: str) -> dict:
        """Get device registration code for pairing"""
        session = self.get_session()
        response = session.post(
            url=self.config["endpoints"]["registration_code"],
            data=b"",
            headers={"x-session-token": session_token}
        )
        response.raise_for_status()
        return response.json()

    def _cache_session_token(self, token: str) -> None:
        """Cache session token with proper TTL"""
        self.session_token = token
        cache_file = Path(directories.cache) / "mgmplus_token.txt"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate TTL from JWT
        ttl = 86400
        try:
            payload = self._decode_jwt_payload(token)
            exp = payload.get("exp", 0)
            remaining = int(exp - time.time())
            if remaining > 60:
                ttl = remaining - 60
        except Exception:
            pass
        
        # Store token with timestamp
        with open(cache_file, 'w') as f:
            json.dump({
                "token": token,
                "expires": time.time() + ttl
            }, f)

    def _load_cached_token(self) -> Optional[str]:
        """Load cached token if not expired"""
        cache_file = Path(directories.cache) / "mgmplus_token.txt"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            if data.get("expires", 0) > time.time():
                return data["token"]
        except Exception:
            pass
        
        return None

    def _ensure_token_fresh(self) -> None:
        """Ensure session token is fresh, refresh if needed"""
        # Try to load cached token
        cached_token = self._load_cached_token()
        if cached_token and not self._is_token_expired(cached_token):
            self.session_token = cached_token
            return

        # Create new device session
        self.log.info(f"{YELLOW}â†’ Creating device session...{RESET}")
        session_data = self._create_device_session()
        device_session = session_data["device_session"]
        
        # If user exists, we're already paired
        if device_session.get("user") is not None:
            self.log.info(f"{GREEN}âœ“ Using existing device pairing{RESET}")
            self._cache_session_token(device_session["session_token"])
            return

        # Start pairing flow
        self.log.info(f"{YELLOW}â†’ Starting device pairing flow...{RESET}")
        anon_token = device_session["session_token"]
        
        code_data = self._get_registration_code(anon_token)
        code = code_data["device"]["code"]
        
        self.log.info(f"{ORANGE}â•”â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•—{RESET}")
        self.log.info(f"{ORANGE}â•‘        MGM+ DEVICE ACTIVATION      â•‘{ORANGE}")
        self.log.info(f"{ORANGE}â• â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•£{RESET}")
        self.log.info(f"{ORANGE}â•‘  {WHITE}âž¤  {YELLOW}Visit:{ORANGE}  mgmplus.com/activate   â•‘{ORANGE}")
        self.log.info(f"{ORANGE}â•‘  {WHITE}âž¤  {YELLOW}Code:{ORANGE}  {GREEN}{code:^8}{ORANGE}                â•‘{ORANGE}")
        self.log.info(f"{ORANGE}â•šâ•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•{RESET}\n")
        
        self.log.info(f"{CYAN}Waiting for activation (5 minutes)...{RESET}")
        
        for i in range(100):  # 5 minutes (3s * 100)
            time.sleep(3)
            session_data = self._create_device_session()
            device_session = session_data["device_session"]
            
            if device_session.get("user") is not None:
                self.log.info(f"{GREEN}âœ“ Device paired successfully!{RESET}")
                self._cache_session_token(device_session["session_token"])
                return
            
            if i % 10 == 0:
                self.log.info(f"  Still waiting... ({i*3}s)")
        
        self.log.exit("Device pairing timed out. Please try again.")

    def setup_service(self) -> None:
        """Initialize service and ensure authentication"""
        self.log.info(f"{YELLOW}â†’ Configuring MGM+ Service{RESET}")
        self._ensure_token_fresh()

    # =========================================================================
    # GRAPHQL METHODS
    # =========================================================================
    def _graphql(self, operation: str, variables: dict, query_name: str) -> dict:
        """Execute GraphQL query"""
        self._ensure_token_fresh()
        session = self.get_session()
        
        queries = self.config.get("graphql_queries", {})
        query = queries.get(query_name)
        if not query:
            raise ValueError(f"Unknown query: {query_name}")
        
        headers = {
            "Content-Type": "application/json",
            "x-session-token": self.session_token,
        }
        if self.aws_waf_token:
            headers["x-aws-waf-token"] = self.aws_waf_token
        
        if "widgetsSupportedTemplates" in variables:
            del variables["widgetsSupportedTemplates"]
        
        response = session.post(
            url=self.config["endpoints"]["graphql"],
            json={
                "operationName": operation,
                "variables": variables,
                "query": query
            },
            headers=headers
        )
        
        if not response.ok:
            self.log.error(f"GraphQL error: {response.status_code}")
            self.log.debug(response.text[:500])
            response.raise_for_status()
        
        data = response.json()
        if data.get("errors"):
            error_msg = data['errors'][0]['message'] if data['errors'] else "Unknown error"
            raise ValueError(f"GraphQL error: {error_msg}")
        
        return data

    # =========================================================================
    # MOVIE METHODS
    # =========================================================================
    def get_movie_info(self, movie_id=None) -> Union[Title, None]:
        """Get movie information"""
        if movie_id is None:
            movie_id = self.content_id
        
        self.log.info(f"{WHITE}â†’ Getting movie info for: {movie_id}{RESET}")
        
        try:
            data = self._graphql(
                "Movie",
                {"id": movie_id},
                "movie_details"
            )
            
            movie_data = data.get("data", {}).get("movie")
            if not movie_data:
                return None
            
            title = movie_data.get("title", "Unknown")
            year = movie_data.get("releaseYear")
            
            self.log.info(f"{GREEN}âœ“ Found movie: {title} ({year}){RESET}")
            
            return Title(
                id_=movie_data["id"],
                type_=Title.Types.MOVIE,
                name=title,
                year=year,
                source=self.ALIASES[0],
                original_lang="en",
                service_data=movie_data
            )
            
        except Exception as e:
            self.log.error(f"Error getting movie: {e}")
            return None

    # =========================================================================
    # SERIES METHODS
    # =========================================================================
    def get_series_info(self, series_id=None) -> Optional[dict]:
        """Get series basic information"""
        if series_id is None:
            series_id = self.content_id
        
        try:
            data = self._graphql(
                "Series",
                {"id": series_id},
                "series_basic"
            )
            return data.get("data", {}).get("series")
        except Exception:
            return None

    def get_series_episodes(self, series_id=None) -> list[Title]:
        """Get all episodes from a series"""
        titles = []
        
        if series_id is None:
            series_id = self.content_id
        
        self.log.info(f"{WHITE}â†’ Getting series info for: {series_id}{RESET}")
        
        try:
            # Get series with seasons
            data = self._graphql(
                "Series",
                {"id": series_id},
                "series_details"
            )
            
            series_data = data.get("data", {}).get("series")
            if not series_data:
                return []
            
            series_title = series_data.get("title", "Unknown Series")
            seasons = series_data.get("seasons", {}).get("nodes", [])
            
            self.log.info(f"  â””â”€ Seasons found: {len(seasons)}")
            
            for season in seasons:
                season_id = season.get("id")
                season_number = season.get("number", 1)
                
                # Filter by season if specified
                if self.season is not None and season_number != self.season:
                    continue
                
                self.log.info(f"{ORANGE}  â†’ Season {season_number} episodes{RESET}")
                
                # Get episodes for this season
                ep_data = self._graphql(
                    "Episodes",
                    {"seasonId": season_id, "first": 100},
                    "episodes_by_season"
                )
                
                episodes = ep_data.get("data", {}).get("episodes", {}).get("nodes", [])
                
                for episode in episodes:
                    ep_number = episode.get("number", 1)
                    
                    # Filter by episode if specified
                    if self.episode_num is not None and ep_number != self.episode_num:
                        continue
                    
                    title = Title(
                        id_=episode["id"],
                        type_=Title.Types.TV,
                        name=series_title,
                        season=season_number,
                        episode=ep_number,
                        year=episode.get("releaseYear"),
                        original_lang="en",
                        source=self.ALIASES[0],
                        episode_name=episode.get("shortTitle") or episode.get("title"),
                        service_data=episode
                    )
                    titles.append(title)
                    self.log.info(f"    âœ“ S{season_number:02d}E{ep_number:02d}: {title.episode_name}")
            
            self.log.info(f"{GREEN}âœ“ Total episodes: {len(titles)}{RESET}")
            return titles
            
        except Exception as e:
            self.log.error(f"Error getting series: {e}")
            return []

    # =========================================================================
    # EPISODE METHODS
    # =========================================================================
    def get_episode_by_id(self, episode_id=None) -> Union[Title, None]:
        """Get episode by base64 ID"""
        if episode_id is None:
            episode_id = self.content_id
        
        try:
            data = self._graphql(
                "Episode",
                {"id": episode_id},
                "episode_by_id"
            )
            
            episode = data.get("data", {}).get("episode")
            if not episode:
                return None
            
            series_title = episode.get("series", {}).get("title", "Unknown")
            season = episode.get("seasonNumber", 1)
            ep_number = episode.get("number", 1)
            
            self.log.info(f"{GREEN}âœ“ Found episode: {series_title} S{season:02d}E{ep_number:02d}{RESET}")
            
            return Title(
                id_=episode["id"],
                type_=Title.Types.TV,
                name=series_title,
                season=season,
                episode=ep_number,
                year=episode.get("releaseYear"),
                original_lang="en",
                source=self.ALIASES[0],
                episode_name=episode.get("shortTitle") or episode.get("title"),
                service_data=episode
            )
            
        except Exception:
            return None

    def get_episode_by_slug(self, series_slug: str, season: int, episode_num: int) -> Union[Title, None]:
        """Get episode by series slug, season, episode numbers"""
        self.log.info(f"{CYAN}â†’ Looking for: {series_slug} S{season:02d}E{episode_num:02d}{RESET}")
        
        try:
            # First get series ID
            series = self.get_series_info(series_slug)
            if not series:
                return None
            
            series_id = series["id"]
            series_title = series.get("title", "Unknown")
            
            # Get seasons
            data = self._graphql(
                "Series",
                {"id": series_id, "widgetsSupportedTemplates": ["Standard"]},
                "series_details"
            )
            
            series_data = data.get("data", {}).get("series", {})
            seasons = series_data.get("seasons", {}).get("nodes", [])
            
            # Find matching season
            for s in seasons:
                if s.get("number") == season:
                    season_id = s.get("id")
                    
                    # Get episodes for this season
                    ep_data = self._graphql(
                        "Episodes",
                        {"seasonId": season_id, "first": 100},
                        "episodes_by_season"
                    )
                    
                    episodes = ep_data.get("data", {}).get("episodes", {}).get("nodes", [])
                    
                    # Find matching episode
                    for ep in episodes:
                        if ep.get("number") == episode_num:
                            self.log.info(f"{GREEN}âœ“ Found episode ID: {ep['id']}{RESET}")
                            
                            return Title(
                                id_=ep["id"],
                                type_=Title.Types.TV,
                                name=series_title,
                                season=season,
                                episode=episode_num,
                                year=ep.get("releaseYear"),
                                original_lang="en",
                                source=self.ALIASES[0],
                                episode_name=ep.get("shortTitle") or ep.get("title"),
                                service_data=ep
                            )
            
            return None
            
        except Exception as e:
            self.log.debug(f"Error finding episode: {e}")
            return None

    # =========================================================================
    # MAIN TITLE RESOLUTION
    # =========================================================================
    def get_titles(self) -> Union[list[Title], Title]:
        """Main method to get titles based on content type"""
        if not self.content_id:
            self.log.exit("No title ID provided")
        
        titles = []
        
        self.log.info(f"{ORANGE}â†’ Resolving content: {self.content_id}{RESET}")
        self.log.info(f"  â””â”€ Type: {self.content_type}")
        
        # Route based on content type
        if self.content_type == "movies":
            self.log.info(f"{BLUE}â†’ Processing as MOVIE{RESET}")
            movie = self.get_movie_info()
            if movie:
                titles = [movie]
            else:
                self.log.error(f"{RED}Could not resolve movie{RESET}")
                
        elif self.content_type == "series":
            self.log.info(f"{WHITE}â†’ Processing as SERIES{RESET}")
            titles = self.get_series_episodes()
            
        elif self.content_type == "episode":
            self.log.info(f"{WHITE}â†’ Processing as EPISODE{RESET}")
            
            # Try direct ID first
            episode = self.get_episode_by_id()
            
            # If not found and we have season/episode, try by slug
            if not episode and self.season is not None and self.episode_num is not None:
                episode = self.get_episode_by_slug(self.content_id, self.season, self.episode_num)
            
            if episode:
                titles = [episode]
            else:
                self.log.error(f"{RED}Could not resolve episode{RESET}")
        
        if not titles:
            self.log.error(f"{RED}No titles found for: {self.content_id}{RESET}")
            self.log.info("\nUsage:")
            self.log.info("  â€¢ Movie:  vt MGMPlus \"slug\"")
            self.log.info("  â€¢ Series: vt MGMPlus \"slug\" --series")
            self.log.info("  â€¢ Episode: vt MGMPlus \"series-s01e01\" --episode")
            self.log.info("  â€¢ Episode URL: vt MGMPlus \"https://mgmplus.com/series/.../watch/season/1/episode/1\"")
        
        return titles

    # =========================================================================
    # PLAYBACK METHODS
    # =========================================================================
    def _graphql_play_flow(self, content_id: str, context: Optional[str] = None) -> dict:
        """Get PlayFlow data from GraphQL"""
        self._ensure_token_fresh()
        session = self.get_session()
        
        playflow_config = self.config.get("playflow", {})
        variables = {
            "id": content_id,
            "context": context or "",
            "behavior": "DEFAULT",
            "supportedActions": playflow_config.get("supported_actions", [])
        }
        
        # Build query
        query = """
        query PlayFlow($id: String!, $supportedActions: [PlayFlowActionEnum!]!, $context: String, $behavior: BehaviorEnum = DEFAULT) {
          playFlow(id: $id, context: $context, behavior: $behavior, supportedActions: $supportedActions) {
            __typename
            ... on PlayContent {
              type
              continuationContext
              playheadPosition
              streams(types: [
                { packagingSystem: DASH, encryptionScheme: CBCS },
                { packagingSystem: DASH, encryptionScheme: CENC },
                { packagingSystem: HLS, encryptionScheme: NONE }
              ]) {
                id
                internalStreamId
                videoQuality { width height }
                widevine { authenticationToken licenseServerUrl }
                packagingSystem
                encryptionScheme
                playlistUrl
              }
              closedCaptions {
                vtt { location }
              }
              currentItem {
                content {
                  __typename
                  ... on Movie { id title underlyingId duration }
                  ... on Episode { id title underlyingId duration }
                }
              }
              hints {
                videoId
                videoTitle
                resourceId
                isLive
              }
            }
            ... on ShowNotice {
              type
              actions {
                continuationContext
                text
              }
            }
            ... on ContinuePlay { type }
            ... on LogIn { type }
            ... on Noop { type }
          }
        }
        """
        
        response = session.post(
            url=self.config["endpoints"]["graphql"],
            json={
                "operationName": "PlayFlow",
                "variables": variables,
                "query": query
            },
            headers={
                "Content-Type": "application/json",
                "x-session-token": self.session_token,
            }
        )
        
        if not response.ok:
            self.log.error(f"PlayFlow error: {response.status_code}")
            return {}
        
        data = response.json()
        if data.get("errors"):
            self.log.error(f"PlayFlow GraphQL errors: {data['errors']}")
            return {}
        
        return data.get("data", {}).get("playFlow", {})

    def _resolve_playflow(self, content_id: str) -> dict:
        """Resolve PlayFlow to actual content, handling pre-rolls"""
        play_flow = self._graphql_play_flow(content_id)
        seen_contexts = set()
        
        for attempt in range(6):
            if not play_flow:
                break
            
            typename = play_flow.get("__typename", "")
            pf_type = play_flow.get("type", "")
            
            if typename == "PlayContent" or pf_type == "play_content":
                streams = play_flow.get("streams", [])
                if any(s.get("packagingSystem") == "DASH" and s.get("widevine") for s in streams):
                    return play_flow
                
                context = play_flow.get("continuationContext")
                if context and context not in seen_contexts:
                    seen_contexts.add(context)
                    play_flow = self._graphql_play_flow(content_id, context=context)
                    continue
                return play_flow
            
            context = play_flow.get("continuationContext")
            if not context:
                for action in play_flow.get("actions", []):
                    if isinstance(action, dict) and action.get("continuationContext"):
                        context = action["continuationContext"]
                        break
            
            if context and context not in seen_contexts:
                seen_contexts.add(context)
                play_flow = self._graphql_play_flow(content_id, context=context)
                continue
            
            break
        
        return play_flow

    def _create_web_session(self) -> str:
        """Create web session for Amazon fallback - cleared between episodes"""
        # Clear stale web session between episodes (Unshackle improvement)
        if self.web_session_token:
            self.web_session_token = None
            
        web_config = self.config.get("web", {})
        session = self.get_session()
        
        device_info = web_config.get("device", {}).copy()
        device_info["guid"] = self.guid
        
        response = session.post(
            url=web_config["endpoints"]["sessions"],
            json={
                "device": device_info,
                "apikey": web_config["apikey"]
            },
            headers={
                "Origin": "https://www.mgmplus.com",
                "Referer": "https://www.mgmplus.com/"
            }
        )
        response.raise_for_status()
        data = response.json()
        self.web_session_token = data["device_session"]["session_token"]
        return self.web_session_token

    def _web_play_flow(self, content_id: str) -> Optional[dict]:
        """Get web PlayFlow for Amazon fallback"""
        web_token = self._create_web_session()
        web_config = self.config.get("web", {})
        session = self.get_session()
        
        playflow_config = self.config.get("playflow", {})
        
        response = session.post(
            url=web_config["endpoints"]["graphql"],
            json={
                "operationName": "PlayFlow",
                "variables": {
                    "id": content_id,
                    "supportedActions": playflow_config.get("supported_actions", []),
                    "streamTypes": playflow_config.get("stream_types", [])
                },
                "query": """
                query PlayFlow($id: String!, $supportedActions: [PlayFlowActionEnum!]!, $context: String, $behavior: BehaviorEnum = DEFAULT, $streamTypes: [StreamDefinition!]) {
                  playFlow(id: $id, supportedActions: $supportedActions, context: $context, behavior: $behavior) {
                    ... on ShowNotice {
                      type
                      actions { continuationContext text }
                      description
                      title
                      __typename
                    }
                    ... on PlayContent {
                      type
                      continuationContext
                      heartbeatToken
                      currentItem {
                        content {
                          __typename
                          ... on Movie { id shortName amazonContentId duration }
                          ... on Episode {
                            id
                            series { shortName __typename }
                            seasonNumber
                            number
                            amazonContentId
                            duration
                          }
                        }
                      }
                      amazonPlayback {
                        playbackEnvelope
                        playbackId
                        __typename
                      }
                      playheadPosition
                      closedCaptions {
                        vtt { location __typename }
                        __typename
                      }
                      streams(types: $streamTypes) {
                        id
                        playlistUrl
                        packagingSystem
                        encryptionScheme
                        videoQuality { height width __typename }
                        widevine { authenticationToken licenseServerUrl __typename }
                        __typename
                      }
                      __typename
                    }
                    ... on ContinuePlay { type __typename }
                    ... on LogIn { type __typename }
                    ... on Noop { type __typename }
                    __typename
                  }
                }
                """
            },
            headers={
                "Content-Type": "application/json",
                "x-session-token": web_token,
                "Origin": "https://www.mgmplus.com",
                "Referer": "https://www.mgmplus.com/"
            }
        )
        
        if not response.ok:
            return None
        
        data = response.json()
        if data.get("errors"):
            self.log.debug(f"Web PlayFlow errors: {data['errors']}")
            return None
        
        return data.get("data", {}).get("playFlow")

    def _generate_nerid(self, e: int) -> str:
        """Generate Amazon network edge request ID"""
        base64_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        timestamp = int(time.time() * 1000)
        epoch_chars = []
        for _ in range(7):
            epoch_chars.append(base64_chars[timestamp % 64])
            timestamp //= 64
        base64_epoch = "".join(reversed(epoch_chars))
        random_bytes = os.urandom(15)
        random_chars = [base64_chars[b % 64] for b in random_bytes]
        random_part = "".join(random_chars)
        suffix = f"{e % 100:02d}"
        return base64_epoch + random_part + suffix

    def _extract_amazon_mpd_url(self, result: dict) -> Optional[str]:
        """Extract MPD URL from Amazon response with CDN preference (Unshackle improvement)"""
        cdn_preference = ["akamai", "cloudfront"]
        
        # Try playlisted format first
        playlisted = (result.get("vodPlaylistedPlaybackUrls") or {}).get("result") or {}
        playback_urls = playlisted.get("playbackUrls") or {}
        playlist_items = playback_urls.get("intraTitlePlaylist") or []
        
        if playlist_items:
            main_item = next((p for p in playlist_items if p.get("type") == "Main"), None)
            if not main_item and playlist_items:
                main_item = playlist_items[0]
            if main_item:
                urls = main_item.get("urls") or []
                # Try CDN preference order
                for preferred in cdn_preference:
                    for u in urls:
                        if isinstance(u, dict) and u.get("cdn", "").lower() == preferred and u.get("url"):
                            return self._clean_mpd_url(u["url"])
                # Fallback to first available URL
                for u in urls:
                    if isinstance(u, dict) and u.get("url"):
                        return self._clean_mpd_url(u["url"])
        
        # Fallback to legacy format
        vod_urls = result.get("vodPlaybackUrls", {}).get("result", {})
        playback_urls = vod_urls.get("playbackUrls", {})
        url_sets = playback_urls.get("urlSets", [])
        
        if isinstance(url_sets, list):
            for url_set in url_sets:
                if isinstance(url_set, dict) and url_set.get("url"):
                    return self._clean_mpd_url(url_set["url"])
        
        if isinstance(url_sets, dict):
            for url_set in url_sets.values():
                if isinstance(url_set, dict):
                    if url_set.get("url"):
                        return self._clean_mpd_url(url_set["url"])
                    for cdn_info in (url_set.get("urls") or {}).values():
                        if isinstance(cdn_info, dict) and cdn_info.get("url"):
                            return self._clean_mpd_url(cdn_info["url"])
        
        return None

    @staticmethod
    def _clean_mpd_url(mpd_url: str) -> str:
        """Clean up Amazon MPD URL by stripping custom/dm segments"""
        if "custom=true" in mpd_url:
            return mpd_url
        mpd_url = re.sub(r".@[^/]+/|custom=true&", "", mpd_url)
        try:
            from urllib.parse import urlparse, urlunparse
            parsed_url = urlparse(mpd_url)
            new_path = "/".join(
                segment for segment in parsed_url.path.split("/") 
                if not any(sub in segment for sub in ["$", "dm"])
            )
            return urlunparse(parsed_url._replace(path=new_path))
        except Exception:
            return mpd_url

    def _get_amazon_playback(self, content_id: str) -> Optional[Tuple[str, str, str, str, dict, dict]]:
        """Get Amazon DASH playback with full manifest (Unshackle improvement)"""
        play_flow = self._web_play_flow(content_id)
        if not play_flow:
            return None
        
        amazon = play_flow.get("amazonPlayback") or {}
        playback_envelope = amazon.get("playbackEnvelope")
        playback_id = amazon.get("playbackId")
        
        if not playback_envelope or not playback_id:
            self.log.info("  â†’ No Amazon playback data available")
            return None
        
        amazon_config = self.config.get("amazon", {})
        session = self.get_session()
        
        response = session.post(
            url=f"{amazon_config['base']}/playback/prs/GetVodPlaybackResources",
            params={
                "deviceID": self.amazon_device_id,
                "deviceTypeID": amazon_config["device_type"],
                "gascEnabled": "false",
                "marketplaceID": amazon_config["marketplace"],
                "uxLocale": "en_US",
                "firmware": 1,
                "titleId": playback_id,
                "nerid": self._generate_nerid(0)
            },
            headers={
                "Accept": "*/*",
                "Content-Type": "text/plain",
                "x-atv-client-type": "XpPlayer",
                "Origin": "https://www.mgmplus.com",
                "Referer": "https://www.mgmplus.com/"
            },
            json={
                "globalParameters": {
                    "deviceCapabilityFamily": "WebPlayer",
                    "playbackEnvelope": playback_envelope,
                    "capabilityDiscriminators": {
                        "operatingSystem": {"name": "Windows", "version": "10.0"},
                        "middleware": {"name": "Chrome", "version": "145.0.0.0"},
                        "nativeApplication": {"name": "Chrome", "version": "145.0.0.0"},
                        "hfrControlMode": "Legacy",
                        "displayResolution": {"height": 2160, "width": 3840}
                    }
                },
                "vodPlaybackUrlsRequest": {
                    "device": {
                        "hdcpLevel": "2.2",
                        "maxVideoResolution": "2160p",
                        "supportedStreamingTechnologies": ["DASH"],
                        "streamingTechnologies": {
                            "DASH": {
                                "bitrateAdaptations": ["CBR", "CVBR"],
                                "codecs": ["H265", "H264"],
                                "drmKeyScheme": "DualKey",
                                "drmType": "Widevine",
                                "dynamicRangeFormats": ["None"],
                                "edgeDeliveryAuthorizationSchemes": ["PVExchangeV1", "Transparent"],
                                "fragmentRepresentations": ["ByteOffsetRange", "SeparateFile"],
                                "frameRates": ["Standard", "High"],
                                "stitchType": "MultiPeriod",
                                "segmentInfoType": "Base",
                                "timedTextRepresentations": ["NotInManifestNorStream", "SeparateStreamInManifest"],
                                "trickplayRepresentations": ["NotInManifestNorStream"],
                                "variableAspectRatio": "supported"
                            }
                        },
                        "displayWidth": 3840,
                        "displayHeight": 2160
                    },
                    "playbackSettingsRequest": {
                        "firmware": "UNKNOWN",
                        "playerType": "xp",
                        "responseFormatVersion": "1.0.0",
                        "titleId": playback_id
                    }
                },
                "timedTextUrlsRequest": {"supportedTimedTextFormats": ["TTMLv2", "DFXP"]}
            }
        )
        
        if not response.ok:
            return None
        
        result = response.json()
        
        # Extract MPD URL using improved method
        mpd_url = self._extract_amazon_mpd_url(result)
        if not mpd_url:
            return None
        
        session_handoff_token = result.get("sessionization", {}).get("sessionHandoffToken", "")
        if not session_handoff_token:
            return None
        
        return mpd_url, playback_envelope, session_handoff_token, playback_id, play_flow, result

    def _add_amazon_subtitles(self, tracks: Tracks, manifest: dict) -> None:
        """Extract subtitles from Amazon manifest (Unshackle improvement)"""
        timed_text = (manifest.get("timedTextUrls") or {}).get("result") or {}
        subtitle_urls = timed_text.get("subtitleUrls") or []
        forced_urls = timed_text.get("forcedNarrativeUrls") or []
        
        for sub in subtitle_urls + forced_urls:
            sub_type = sub.get("type", "").lower()
            is_forced = sub_type == "forcednarrative"
            lang_code = sub.get("languageCode", "en")
            
            try:
                name = Language.get(lang_code).display_name()
            except:
                name = lang_code
            
            url = sub.get("url", "")
            if not url:
                continue
            
            # Prefer SRT format
            url = os.path.splitext(url)[0] + ".srt"
            
            tracks.add(TextTrack(
                id_=sub.get("timedTextTrackId", sub.get("timedSubtitleId", f"{lang_code}_{sub_type}")),
                source=self.ALIASES[0],
                url=url,
                codec="srt",
                language=lang_code,
                name=name,
                forced=is_forced,
                sdh=sub_type == "sdh",
                cc=sub_type == "cc"
            ))

    def _get_amazon_widevine_license(self, challenge: bytes, amazon_ctx: dict) -> bytes:
        """Get Widevine license from Amazon"""
        amazon_config = self.config.get("amazon", {})
        playback_id = amazon_ctx["playback_id"]
        session = self.get_session()
        
        response = session.post(
            url=f"{amazon_config['base']}/playback/drm-vod/GetWidevineLicense",
            params={
                "deviceID": self.amazon_device_id,
                "deviceTypeID": amazon_config["device_type"],
                "gascEnabled": "false",
                "marketplaceID": amazon_config["marketplace"],
                "uxLocale": "en_US",
                "firmware": 1,
                "titleId": playback_id,
                "nerid": self._generate_nerid(0)
            },
            headers={
                "Accept": "*/*",
                "Content-Type": "text/plain",
                "x-atv-client-type": "XpPlayer",
                "Origin": "https://www.mgmplus.com",
                "Referer": "https://www.mgmplus.com/"
            },
            json={
                "includeHdcpTestKey": True,
                "licenseChallenge": base64.b64encode(challenge).decode(),
                "playbackEnvelope": amazon_ctx["playback_envelope"],
                "sessionHandoffToken": amazon_ctx["session_handoff_token"]
            }
        )
        
        if not response.ok:
            self.log.exit(f"Amazon license error: {response.status_code}")
        
        result = response.json()
        license_b64 = result.get("widevineLicense", {}).get("license")
        if not license_b64:
            self.log.exit("Amazon returned no Widevine license")
        
        return base64.b64decode(license_b64)

    def _select_best_stream(self, streams: list[dict]) -> Optional[dict]:
        """Select highest quality stream, ALWAYS preferring full manifest over quality-specific ones"""
        dash_wv = [
            s for s in streams 
            if s.get("packagingSystem") == "DASH" and s.get("playlistUrl") and s.get("widevine")
        ]
        if not dash_wv:
            return None
        
        full_manifests = []
        quality_manifests = []
        
        for s in dash_wv:
            url = s.get("playlistUrl", "")
            height = (s.get("videoQuality") or {}).get("height", 0)
            
            is_full = "playlist.mpd" in url and "playlist_" not in url
            
            stream_info = {
                "stream": s,
                "url": url,
                "height": height,
                "is_hevc": "hvc1" in url or "hev1" in url,
                "is_full": is_full
            }
            
            if is_full:
                full_manifests.append(stream_info)
            else:
                quality_manifests.append(stream_info)
        
        if full_manifests:
            full_manifests.sort(key=lambda x: (
                -x["height"],
                -int(x["is_hevc"])
            ))
            best = full_manifests[0]
            self.log.debug(f"  â†’ Selected FULL manifest: {best['height']}p {'HEVC' if best['is_hevc'] else 'H.264'}")
            return best["stream"]
        
        if quality_manifests:
            quality_manifests.sort(key=lambda x: (
                -x["height"],
                -int(x["is_hevc"])
            ))
            best = quality_manifests[0]
            self.log.debug(f"  â†’ Selected QUALITY-specific manifest: {best['height']}p {'HEVC' if best['is_hevc'] else 'H.264'}")
            return best["stream"]
        
        return None

    def get_tracks(self, title: Title) -> Tracks:
        """Get tracks from manifest for a title - with improved audio selection (Unshackle v2.0.2)"""
        if title.type == Title.Types.MOVIE:
            self.log.info(f"{BLUE}â†’ Getting tracks for movie: {title.name}{RESET}")
        else:
            self.log.info(f"{WHITE}â†’ Getting tracks for: {title.name} S{title.season:02d}E{title.episode:02d}{RESET}")
        
        content_id = title.id
        self.log.info(f"  â†’ Content ID: {content_id}")
        
        # Resolve PlayFlow
        playflow_data = self._resolve_playflow(content_id)
        
        if not playflow_data:
            self.log.error(f"{RED}Failed to get PlayFlow{RESET}")
            self.log.exit("Could not get PlayFlow data")
        
        # Check if it's a notice
        if playflow_data.get("__typename") == "ShowNotice":
            notice_title = playflow_data.get("title", "Unavailable")
            notice_desc = playflow_data.get("description", "")
            msg = f"{notice_title}: {notice_desc}" if notice_desc else notice_title
            self.log.info(f"  â†’ Not available: {msg}")
            return Tracks()
        
        streams = playflow_data.get("streams", [])
        
        # Try DASH Widevine streams first
        dash_streams = [s for s in streams if s.get("packagingSystem") == "DASH" and s.get("widevine")]
        
        if dash_streams:
            manifest_url = None
            license_url = None
            auth_token = None
            height = 0
            
            selected_stream = self._select_best_stream(dash_streams)
            if not selected_stream:
                selected_stream = dash_streams[0]
            
            manifest_url = selected_stream.get("playlistUrl")
            if not manifest_url:
                self.log.error("  â†’ No playlist URL in selected stream")
            else:
                height = selected_stream.get("videoQuality", {}).get("height", 0)
                widevine_info = selected_stream.get("widevine", {})
                license_url = widevine_info.get("licenseServerUrl")
                auth_token = widevine_info.get("authenticationToken")
                
                self.log.info(f"  â†’ Selected {height}p DASH stream from MGM+")
                
                # PARCH: Intentar actualizar a manifiesto completo si es especÃ­fico de calidad
                original_manifest_url = manifest_url
                if "playlist_" in manifest_url and "playlist.mpd" in manifest_url:
                    # Construir URL del manifiesto completo
                    full_manifest_url = re.sub(r'playlist_\d+\.mpd', 'playlist.mpd', manifest_url)
                    
                    if full_manifest_url != manifest_url:
                        self.log.info(f"  â†’ Attempting to upgrade to FULL manifest: {full_manifest_url}")
                        
                        # Verificar si el manifiesto completo existe
                        try:
                            head_response = self.get_session().head(full_manifest_url, timeout=5)
                            if head_response.ok:
                                self.log.info(f"  â†’ âœ“ FULL manifest available, using it instead")
                                manifest_url = full_manifest_url
                                selected_stream["playlistUrl"] = full_manifest_url
                            else:
                                self.log.info(f"  â†’ âœ— FULL manifest not available, staying with quality-specific")
                        except Exception as e:
                            self.log.info(f"  â†’ âœ— Could not verify FULL manifest: {e}")
                
                if "playlist.mpd" in manifest_url and "playlist_" not in manifest_url:
                    self.log.info(f"  â†’ âœ“ Using FULL manifest (all qualities available)")
                else:
                    self.log.info(f"  â†’ âš  Using quality-specific manifest (may be limited)")
                
                if self.manifest_only:
                    self.log.info(f"\n{GREEN}Manifest:{RESET} {manifest_url}")
                    self.log.info(f"{GREEN}License:{RESET} {license_url}")
                    return Tracks()
                
                # Parse manifest
                try:
                    self.log.info(f"  â†’ Downloading manifest...")
                    response = self.get_session().get(manifest_url, timeout=30)
                    if not response.ok:
                        self.log.exit(f"Error downloading manifest: {response.status_code}")
                    
                    tracks = parse_mpd(
                        url=manifest_url,
                        data=response.text,
                        source=self.ALIASES[0],
                        session=self.get_session()
                    )
                    
                    # Si originalmente era especÃ­fico y logramos obtener el completo,
                    # ya tenemos todas las calidades, no necesitamos hacer mÃ¡s
                    
                    # Try Amazon HD for better audio (Unshackle improvement)
                    amazon_result = None
                    try:
                        amazon_result = self._get_amazon_playback(content_id)
                    except Exception as e:
                        self.log.warning(f"  â†’ Amazon playback request failed: {e}")
                    
                    if amazon_result:
                        mpd_url, playback_envelope, session_handoff_token, playback_id, web_play_flow, manifest = amazon_result
                        
                        try:
                            # Parse Amazon manifest for audio comparison
                            amazon_response = self.get_session().get(mpd_url, timeout=30)
                            if amazon_response.ok:
                                amazon_tracks = parse_mpd(
                                    url=mpd_url,
                                    data=amazon_response.text,
                                    source=self.ALIASES[0],
                                    session=self.get_session()
                                )
                                
                                # Compare audio bitrates
                                mgm_best = max((float(a.bitrate or 0) for a in tracks.audios), default=0)
                                amzn_best = max((float(a.bitrate or 0) for a in amazon_tracks.audios), default=0)
                                
                                if amazon_tracks.audios and amzn_best > mgm_best:
                                    self.log.info(f"  â†’ Using Amazon audio (better bitrate: {amzn_best} > {mgm_best})")
                                    
                                    # Replace audio tracks with Amazon's
                                    for audio in amazon_tracks.audios:
                                        audio.encrypted = True
                                        audio.license = "amazon"
                                        audio.customdata = {
                                            "playback_envelope": playback_envelope,
                                            "session_handoff_token": session_handoff_token,
                                            "playback_id": playback_id
                                        }
                                        
                                        # Fix language from audioTrackId
                                        if hasattr(audio, 'data') and "dash" in audio.data:
                                            audio_track_id = audio.data["dash"]["adaptation_set"].get("audioTrackId")
                                            sub_type = audio.data["dash"]["adaptation_set"].get("audioTrackSubtype")
                                            if audio_track_id is not None:
                                                try:
                                                    lang_code = audio_track_id.split("_")[0]
                                                    audio.language = Language.get(lang_code)
                                                except:
                                                    pass
                                            if sub_type is not None and "descriptive" in sub_type.lower():
                                                audio.descriptive = True
                                        
                                        tracks.audios = list(amazon_tracks.audios)
                                        
                                        # Add Amazon subtitles
                                        self._add_amazon_subtitles(tracks, manifest)
                        except Exception as e:
                            self.log.debug(f"  â†’ Amazon audio comparison failed: {e}")
                    
                    # Apply MGM+ license info to video tracks
                    for track in tracks.videos:
                        track.encrypted = True
                        track.license = license_url
                        track.customdata = auth_token
                    
                    # Apply license info to audio tracks (if not replaced by Amazon)
                    for track in tracks.audios:
                        if hasattr(track, 'encrypted') and track.encrypted and getattr(track, 'license', None) != "amazon":
                            track.license = license_url
                            track.customdata = auth_token
                        elif not hasattr(track, 'encrypted'):
                            track.encrypted = False
                    
                    # NO ABORTAR POR FALTA DE 1080P - continuar con la mejor calidad disponible
                    if tracks.videos:
                        max_height = max((track.height for track in tracks.videos), default=0)
                        if max_height < 1080:
                            self.log.warning(f"{YELLOW}  âš  Only {max_height}p video available (below 1080p){RESET}")
                            self.log.info(f"{YELLOW}  â†’ Continuing with best available quality{RESET}")
                        else:
                            self.log.info(f"{GREEN}  âœ“ Found {max_height}p video{RESET}")
                        
                        self.log.info(f"{GREEN}âœ“ Got {len(tracks.videos)} video, {len(tracks.audios)} audio, {len(tracks.subtitles)} subs{RESET}")
                        return tracks
                    else:
                        self.log.error("  â†’ No video tracks found in manifest")
                    
                except Exception as e:
                    self.log.error(f"Error parsing manifest: {e}")
                    # No return here, continue to fallback
        
        # Fallback to full Amazon HD
        self.log.info(f"{YELLOW}  â†’ No DASH Widevine streams or manifest error, trying Amazon HD fallback...{RESET}")
        
        try:
            amazon_result = self._get_amazon_playback(content_id)
            if amazon_result:
                mpd_url, playback_envelope, session_handoff_token, playback_id, web_play_flow, manifest = amazon_result
                
                self.log.info(f"  â†’ Got Amazon HD stream")
                
                if self.manifest_only:
                    self.log.info(f"\n{GREEN}Manifest (Amazon):{RESET} {mpd_url}")
                    return Tracks()
                
                # Parse manifest
                response = self.get_session().get(mpd_url, timeout=30)
                if not response.ok:
                    self.log.exit(f"Error downloading Amazon manifest: {response.status_code}")
                
                tracks = parse_mpd(
                    url=mpd_url,
                    data=response.text,
                    source=self.ALIASES[0],
                    session=self.get_session()
                )
                
                # Add Amazon context to tracks
                for track in tracks.videos:
                    track.encrypted = True
                    track.license = "amazon"
                    track.customdata = {
                        "playback_envelope": playback_envelope,
                        "session_handoff_token": session_handoff_token,
                        "playback_id": playback_id
                    }
                
                for track in tracks.audios:
                    if hasattr(track, 'encrypted') and track.encrypted:
                        track.license = "amazon"
                        track.customdata = {
                            "playback_envelope": playback_envelope,
                            "session_handoff_token": session_handoff_token,
                            "playback_id": playback_id
                        }
                    
                    # Fix language from audioTrackId
                    if hasattr(track, 'data') and "dash" in track.data:
                        audio_track_id = track.data["dash"]["adaptation_set"].get("audioTrackId")
                        sub_type = track.data["dash"]["adaptation_set"].get("audioTrackSubtype")
                        if audio_track_id is not None:
                            try:
                                lang_code = audio_track_id.split("_")[0]
                                track.language = Language.get(lang_code)
                            except:
                                pass
                        if sub_type is not None and "descriptive" in sub_type.lower():
                            track.descriptive = True
                
                # Add Amazon subtitles (improved method)
                self._add_amazon_subtitles(tracks, manifest)
                
                # Fallback to PlayFlow VTT if no subtitles
                if not tracks.subtitles:
                    cc = web_play_flow.get("closedCaptions") or {}
                    vtt_url = (cc.get("vtt") or {}).get("location")
                    if vtt_url:
                        tracks.add(TextTrack(
                            id_=str(uuid.uuid4()),
                            source=self.ALIASES[0],
                            url=vtt_url,
                            codec="vtt",
                            language="en",
                            forced=False,
                            sdh=False
                        ))
                
                if tracks.videos:
                    max_height = max((track.height for track in tracks.videos), default=0)
                    self.log.info(f"{GREEN}âœ“ Got {len(tracks.videos)} video (max {max_height}p), {len(tracks.audios)} audio, {len(tracks.subtitles)} subs (Amazon){RESET}")
                    return tracks
                else:
                    self.log.error("  â†’ No video tracks found in Amazon manifest")
                    
        except Exception as e:
            self.log.debug(f"Amazon fallback failed: {e}")
        
        # Check for notice in web PlayFlow
        try:
            web_pf = self._web_play_flow(content_id)
            if web_pf and web_pf.get("__typename") == "ShowNotice":
                notice_title = web_pf.get("title", "Unavailable")
                notice_desc = web_pf.get("description", "")
                msg = f"{notice_title}: {notice_desc}" if notice_desc else notice_title
                self.log.info(f"  â†’ Not available: {msg}")
                return Tracks()
        except Exception:
            pass
        
        available = [f"{s.get('packagingSystem')}/{s.get('encryptionScheme')}" for s in streams]
        self.log.exit(f"No playable streams found. Available: {available or 'none'}")
        return Tracks()

    def license(self, challenge: bytes, title: Title, track: Track, *_, **__) -> bytes:
        """Get license for a track"""
        # Check if it's Amazon license
        if getattr(track, 'license', None) == "amazon":
            amazon_ctx = getattr(track, 'customdata', {})
            return self._get_amazon_widevine_license(challenge, amazon_ctx)
        
        # Standard MGM+ license
        license_url = getattr(track, 'license', None)
        customdata = getattr(track, 'customdata', None)
        
        if not license_url:
            self.log.exit("No license URL found")
        
        session = self.get_session()
        
        headers = {
            'Origin': 'https://www.mgmplus.com',
            'Referer': 'https://www.mgmplus.com/',
            'Content-Type': 'application/octet-stream',
        }
        
        if customdata:
            headers['x-keyos-authorization'] = customdata
        
        if self.aws_waf_token:
            headers['x-aws-waf-token'] = self.aws_waf_token
        if self.session_token:
            headers['x-session-token'] = self.session_token
        
        response = session.post(
            url=license_url,
            headers=headers,
            data=challenge
        )
        
        if not response.ok:
            self.log.exit(f"License error: {response.status_code}")
        
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            data = response.json()
            if 'license' in data:
                return base64.b64decode(data['license'])
            elif 'data' in data:
                return base64.b64decode(data['data'])
        
        return response.content