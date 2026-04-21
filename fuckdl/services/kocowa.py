import json
import re
import uuid
import hashlib
from http.cookiejar import CookieJar
from typing import Optional, List
from pathlib import Path

import click
import requests
from langcodes import Language

from fuckdl.objects import Title, Tracks
from fuckdl.objects.tracks import AudioTrack, TextTrack, VideoTrack, Track
from fuckdl.services.BaseService import BaseService


class Kocowa(BaseService):
    """
    Service code for Kocowa Plus (kocowa.com).

    Ported by @AnotherBigUserHere

    FairTrade was the original developer of this service (https://cdm-project.com/FairTrade/unshackle-services/src/branch/main/KOCW)
    
    + Series and movies retrieving
    + ID accepted (for example 349022 - /running-man)

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    Version: 1.0.0

    Auth: Credential (username + password)
    Security: FHD@L3, HD@L3, SD@L3
    """

    ALIASES = ["KOCW", "kocowa", "kocowa+", "kocowaplus", "kowp"]
    TITLE_RE = [
        r"^(?:https?://(?:www\.)?kocowa\.com/[^/]+/season/)?(?P<id>\d+)",
        r"^(?:https?://(?:www\.)?kocowa\.com/[^/]+/movie/)?(?P<id>\d+)",
    ]

    @staticmethod
    @click.command(name="Kocowa", short_help="https://www.kocowa.com")
    @click.argument("title", type=str)
    @click.option("--extras", is_flag=True, default=False, help="Include teasers/extras")
    @click.option("--movie", is_flag=True, default=False, help="Title is a Movie.")
    @click.pass_context
    def cli(ctx: click.Context, **kwargs):
        return Kocowa(ctx, **kwargs)

    def __init__(self, ctx: click.Context, title: str, extras: bool = False, movie: bool = False):
        super().__init__(ctx)
        self.title_input = title
        self.include_extras = extras
        self.movie = movie
        
        # Initialize attributes
        self.title_id = None
        self.access_token = None
        self.middleware_token = None
        self.playback_token = None
        self.brightcove_account_id = None
        self.brightcove_pk = None
        self.widevine_license_url = None
        self._is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                             (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                              self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.device_id = None
        
        # Parse title - extract ID from URL
        self._parse_title_id()
        
        # Configure service (uses self.config from BaseService)
        self._configure()

    def _parse_title_id(self):
        """Parse the title ID from the input URL or string."""
        # Try to match with regex patterns
        for regex in self.TITLE_RE:
            m = re.search(regex, self.title_input)
            if m:
                self.title_id = m.group("id")
                self.log.debug(f"Parsed title ID: {self.title_id}")
                return
        
        # If no match, check if it's just a number
        if self.title_input.isdigit():
            self.title_id = self.title_input
            self.log.debug(f"Using numeric ID: {self.title_id}")
            return
        
        # If it's a URL with a different pattern, try to extract any number
        numbers = re.findall(r'\d+', self.title_input)
        if numbers:
            self.title_id = numbers[0]
            self.log.debug(f"Extracted numeric ID from URL: {self.title_id}")
            return
        
        # Fallback: use as search keyword
        self.log.warning(f"Unable to parse title ID '{self.title_input}', using as search keyword")
        self.title_id = self.title_input

    def _generate_device_id(self):
        """Generate a device ID similar to the Unshackle version."""
        if not self.credentials:
            return None
            
        email = self.credentials.username.lower().strip()
        uuid_seed = hashlib.md5(email.encode()).digest()
        fake_uuid = str(uuid.UUID(bytes=uuid_seed[:16]))
        
        device_id = f"a_{fake_uuid}_{email}"
        self.log.debug(f"Generated device ID: {device_id}")
        return device_id

    def _configure(self):
        """Configure service using self.config from BaseService."""
        # self.config is already loaded from the YAML file by BaseService
        # It should be in: C:\DRMLab\Fuckdl\services\kocowa.yml
        
        if not self.config or 'endpoints' not in self.config:
            raise self.log.exit(
                "Configuration not found. Please ensure 'kocowa.yml' exists in the services directory.\n"
                "Expected location: services/kocowa.yml\n\n"
                "Example configuration:\n"
                "endpoints:\n"
                "  login: 'https://prod-sgwv3.kocowa.com/api/v01/user/signin'\n"
                "  middleware_auth: 'https://middleware.bcmw.kocowa.com/authenticate-user'\n"
                "  metadata: 'https://prod-fms.kocowa.com/api/v01/fe/content/get?id={title_id}'\n"
                "  authorize: 'https://middleware.bcmw.kocowa.com/api/playback/authorize/{episode_id}'"
            )
        
        # Generate device ID using the Unshackle method
        self.device_id = self._generate_device_id()
        
        # Authenticate
        if not self.credentials:
            raise self.log.exit("Credentials required,put into fuckdl.yml")
        
        self.log.info("Authenticating to Kocowa...")
        self.authenticate()
        
        # Fetch Brightcove configuration
        self._fetch_brightcove_config()

    def authenticate(self, cookies: Optional[CookieJar] = None, credential: Optional[object] = None) -> None:
        """Authenticate with Kocowa using the Unshackle method."""
        if not self.credentials:
            raise ValueError("Kocowa requires username and password")

        # Use the exact payload from Unshackle
        payload = {
            "username": self.credentials.username,
            "password": self.credentials.password,
            "device_id": self.device_id,
            "device_type": "mobile",
            "device_model": "SM-A525F",
            "device_version": "Android 15",
            "push_token": None,
            "app_version": "v4.0.11",
        }

        self.log.debug(f"Authenticating with device_id: {self.device_id}")

        login_url = self.config["endpoints"]["login"]
        
        try:
            r = self.session.post(
                login_url,
                json=payload,
                headers={
                    "Authorization": "anonymous", 
                    "Origin": "https://www.kocowa.com",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 15; SM-A525F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=30
            )
            r.raise_for_status()
            res = r.json()
            
            if res.get("code") != "0000":
                raise PermissionError(f"Login failed: {res.get('message')}")

            self.access_token = res["object"]["access_token"]
            self.log.info("Login successful")

            # Get middleware token
            middleware_url = self.config["endpoints"]["middleware_auth"]
            r = self.session.post(
                middleware_url,
                json={"token": f"wA-Auth.{self.access_token}"},
                headers={
                    "Origin": "https://www.kocowa.com",
                    "Referer": "https://www.kocowa.com/",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 15; SM-A525F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36",
                },
                timeout=30
            )
            r.raise_for_status()
            self.middleware_token = r.json()["token"]
            self.log.debug("Middleware token obtained")
            
        except requests.exceptions.RequestException as e:
            raise PermissionError(f"Authentication failed: {e}")

    def _fetch_brightcove_config(self):
        """Fetch Brightcove account_id and policy_key from Kocowa's public config endpoint."""
        try:
            r = self.session.get(
                "https://middleware.bcmw.kocowa.com/api/config",
                headers={
                    "Origin": "https://www.kocowa.com",
                    "Referer": "https://www.kocowa.com/",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 15; SM-A525F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36"
                },
                timeout=30
            )
            r.raise_for_status()
            config = r.json()

            self.brightcove_account_id = config.get("VC_ACCOUNT_ID")
            self.brightcove_pk = config.get("BCOV_POLICY_KEY")

            if not self.brightcove_account_id:
                raise ValueError("VC_ACCOUNT_ID missing in /api/config response")
            if not self.brightcove_pk:
                raise ValueError("BCOV_POLICY_KEY missing in /api/config response")

            self.log.info(f"Brightcove config loaded: account_id={self.brightcove_account_id}")

        except Exception as e:
            raise RuntimeError(f"Failed to fetch or parse Brightcove config: {e}")

    def get_titles(self):
        """Get titles (movie or TV episodes) based on input."""
        if self.movie:
            return self._get_movie_titles()
        return self._get_show_titles()

    def _get_movie_titles(self):
        """Get movie information."""
        # Check if we have a numeric ID, otherwise search
        if not self.title_id.isdigit():
            search_results = self.search()
            if search_results:
                self.title_id = search_results[0].id
                self.log.info(f"Found movie ID: {self.title_id}")
            else:
                raise self.log.exit(f"Could not find movie with title: {self.title_input}")
        
        movie_url = self.config["endpoints"]["metadata"].format(title_id=self.title_id)
        
        r = self.session.get(
            movie_url,
            headers={"Authorization": self.access_token, "Origin": "https://www.kocowa.com"}
        )
        r.raise_for_status()
        data = r.json()["object"]
        
        title_name = data.get("meta", {}).get("title", {}).get("en", "Unknown Movie")
        year = data.get("meta", {}).get("year", "")
        
        self.log.info(f"Movie found: {title_name} ({year})")
        
        return [
            Title(
                id_=str(data["id"]),
                type_=Title.Types.MOVIE,
                name=title_name,
                year=year,
                original_lang="en",
                source=self.ALIASES[0],
                service_data=data,
            )
        ]

    def _get_show_titles(self):
        """Get episodes list for a TV series."""
        # Check if we have a numeric ID, otherwise search
        if not self.title_id.isdigit():
            search_results = self.search()
            if search_results:
                self.title_id = search_results[0].id
                self.log.info(f"Found show ID: {self.title_id}")
            else:
                raise self.log.exit(f"Could not find show with title: {self.title_input}")
        
        all_episodes = []
        offset = 0
        limit = 20
        series_title = None

        while True:
            url = self.config["endpoints"]["metadata"].format(title_id=self.title_id)
            sep = "&" if "?" in url else "?"
            url += f"{sep}offset={offset}&limit={limit}"

            r = self.session.get(
                url,
                headers={"Authorization": self.access_token, "Origin": "https://www.kocowa.com"}
            )
            r.raise_for_status()
            data = r.json()["object"]

            # Extract the series title only from the very first page
            if series_title is None and "meta" in data:
                series_title = data["meta"]["title"]["en"]

            page_objects = data.get("next_episodes", {}).get("objects", [])
            if not page_objects:
                break

            for ep in page_objects:
                is_episode = ep.get("detail_type") == "episode"
                is_extra = ep.get("detail_type") in ("teaser", "extra")
                if is_episode or (self.include_extras and is_extra):
                    all_episodes.append(ep)

            offset += limit
            total = data.get("next_episodes", {}).get("total_count", 0)
            if len(all_episodes) >= total or len(page_objects) < limit:
                break

        # If we never got the series title, exit with an error
        if series_title is None:
            raise ValueError("Could not retrieve series metadata to get the title.")

        titles = []
        for ep in all_episodes:
            meta = ep["meta"]
            ep_type = "Episode" if ep["detail_type"] == "episode" else ep["detail_type"].capitalize()
            ep_num = meta.get("episode_number", 0)
            title_name = meta["title"].get("en") or f"{ep_type} {ep_num}"

            titles.append(
                Title(
                    id_=str(ep["id"]),
                    type_=Title.Types.TV,
                    name=series_title,
                    season=meta.get("season_number", 1),
                    episode=ep_num,
                    episode_name=title_name,
                    original_lang="en",
                    source=self.ALIASES[0],
                    service_data=ep,
                )
            )
        
        # Sort by season and episode
        titles.sort(key=lambda x: (x.season or 0, x.episode or 0))
        
        # Log season statistics
        season_counts = {}
        for t in titles:
            season_counts[t.season] = season_counts.get(t.season, 0) + 1
        
        season_info = ", ".join([f"S{s} ({c})" for s, c in sorted(season_counts.items())])
        self.log.info(f"Found {len(titles)} total episodes: {season_info}")
        
        return titles

    def get_tracks(self, title: Title):
        """Get audio, video and subtitle tracks for the title."""
        
        # Authorize playback
        auth_url = self.config["endpoints"]["authorize"].format(episode_id=title.id)
        r = self.session.post(
            auth_url,
            headers={"Authorization": f"Bearer {self.middleware_token}"}
        )
        r.raise_for_status()
        auth_data = r.json()
        
        if not auth_data.get("Success"):
            raise PermissionError("Playback authorization failed")
        
        self.playback_token = auth_data["token"]
        self.log.debug("Playback authorized")

        # Fetch Brightcove manifest
        manifest_url = (
            f"https://edge.api.brightcove.com/playback/v1/accounts/{self.brightcove_account_id}/videos/ref:{title.id}"
        )
        r = self.session.get(
            manifest_url,
            headers={"Accept": f"application/json;pk={self.brightcove_pk}"}
        )
        r.raise_for_status()
        manifest = r.json()

        # Get DASH URL + Widevine license
        dash_url = widevine_url = None
        for src in manifest.get("sources", []):
            if src.get("type") == "application/dash+xml":
                dash_url = src["src"]
                widevine_url = (
                    src.get("key_systems", {})
                    .get("com.widevine.alpha", {})
                    .get("license_url")
                )
                if dash_url and widevine_url:
                    break

        if not dash_url or not widevine_url:
            raise ValueError("No Widevine DASH stream found")

        self.widevine_license_url = widevine_url
        self.log.info(f"Manifest URL: {dash_url}")

        # Fetch tracks
        tracks = Tracks.from_mpd(
            url=dash_url,
            source=self.ALIASES[0],
            session=self.session,
        )
        
        if not tracks.videos and not tracks.audios:
            raise self.log.exit("No tracks could be obtained for this title.")
        
        # Add subtitles from manifest
        for sub in manifest.get("text_tracks", []):
            srclang = sub.get("srclang")
            if not srclang or srclang == "thumbnails":
                continue

            subtitle_track = TextTrack(
                id_=sub["id"],
                source=self.ALIASES[0],
                url=sub["src"],
                codec="vtt",
                language=Language.get(srclang),
                sdh=True,
                forced=False,
                descriptor=Track.Descriptor.URL,
            )
            tracks.add(subtitle_track)
        
        return tracks

    def get_chapters(self, title: Title):
        """Get chapters (markers) from the video."""
        return []

    def certificate(self, challenge: bytes, title: Title, track, session_id: str, **kwargs) -> bytes:
        """Get license certificate (not needed for this service)."""
        return None

    def license(self, challenge: bytes, title: Title, track, session_id: str, **kwargs) -> bytes:
        """Get license for the content."""
        if not self.widevine_license_url:
            raise ValueError("No Widevine license URL available")
        
        try:
            r = self.session.post(
                self.widevine_license_url,
                data=challenge,
                headers={
                    "BCOV-Auth": self.playback_token,
                    "Content-Type": "application/octet-stream",
                    "Origin": "https://www.kocowa.com",
                    "Referer": "https://www.kocowa.com/",
                }
            )
            r.raise_for_status()
            return r.content
            
        except requests.exceptions.Timeout:
            raise self.log.exit("Timeout in license request")
        except requests.exceptions.ConnectionError as e:
            raise self.log.exit(f"Connection error in license: {e}")
        except Exception as e:
            raise self.log.exit(f"License error: {e}")

    def search(self) -> List[object]:
        """Search for content (used for interactive selection)."""
        url = "https://prod-fms.kocowa.com/api/v01/fe/gks/autocomplete"
        params = {
            "search_category": "All",
            "search_input": self.title_id,
            "include_webtoon": "true",
        }

        try:
            r = self.session.get(
                url,
                params=params,
                headers={
                    "Authorization": self.access_token,
                    "Origin": "https://www.kocowa.com",
                    "Referer": "https://www.kocowa.com/",
                }
            )
            r.raise_for_status()
            response = r.json()
            contents = response.get("object", {}).get("contents", [])

            results = []
            for item in contents:
                if item.get("detail_type") != "season":
                    continue

                meta = item["meta"]
                title_en = meta["title"].get("en") or "[No Title]"
                description_en = meta["description"].get("en") or ""
                show_id = str(item["id"])

                result = type('SearchResult', (), {
                    'id': show_id,
                    'title': title_en,
                    'description': description_en,
                    'label': "season",
                    'url': f"https://www.kocowa.com/en_us/season/{show_id}/"
                })()
                results.append(result)
                
            return results
            
        except Exception as e:
            self.log.debug(f"Search failed: {e}")
            return []