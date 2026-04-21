import base64
import json
import re
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import unquote

import click
import requests
from langcodes import Language

from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService


class UniversalPlus(BaseService):
    """
    Service code for Universal+ (https://web.play.universalplus.com).
    
    Authorization: tbx-token from cookies

    Security: 1080p Widevine L3 / PlayReady SL2000

    Ported by @AnotherBigUserHere

    A big Thanks to @mr.movies.club for the unshackle script used in this port!

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["UNVP", "universalplus", "unvp", "uplus"]

    TITLE_RE = r"^(?:https?://web\.play\.universalplus\.com/.*?)?(?P<id>[a-z0-9]{5,})$"

    @staticmethod
    @click.command(name="UniversalPlus", short_help="https://web.play.universalplus.com")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return UniversalPlus(ctx, **kwargs)

    def __init__(self, ctx, title):
        super().__init__(ctx)
        self.title = self.parse_title(ctx, title)
        
        # Load config from YAML (already in self.config)
        self._load_config()
        
        # State variables
        self.tbx_token = None
        self.profile_id = None
        self.country_code = "MX"
        
        # Detect if using PlayReady (from CDM device)
        self._is_playready = self._detect_playready(ctx)
        
        self._configure_session()

    def _detect_playready(self, ctx) -> bool:
        """Detect if we're using a PlayReady device."""
        try:
            # Check if CDM is PlayReady
            if hasattr(ctx.obj, 'cdm'):
                cdm = ctx.obj.cdm
                # PlayReady Cdm has specific attributes
                if hasattr(cdm, 'device') and hasattr(cdm.device, 'type'):
                    if hasattr(cdm.device.type, 'PLAYREADY'):
                        return cdm.device.type == cdm.device.type.PLAYREADY
                # Check for PlayReady specific methods
                if hasattr(cdm, 'get_license_challenge') and hasattr(cdm, 'parse_license'):
                    # Try to detect by checking for PlayReady-specific behavior
                    pass
        except Exception:
            pass
        return False

    def _load_config(self):
        
        # Auth config
        self.auth_config = self.config.get("auth", {})
        
        # Content types
        self.content_types = self.config.get("content_types", {})
        
        # Player defaults
        self.player_defaults = self.config.get("player", {})
        
        # Scraping config
        self.scraping_config = self.config.get("scraping", {})
        
        # Subtitles config
        self.subs_config = self.config.get("subtitles", {})

    def _configure_session(self) -> None:
        """Configure session with authentication."""
        # Apply headers from config
        headers = self.config.get("headers", {})
        self.session.headers.update(headers)
        
        # Load cookies into session
        if self.cookies:
            for cookie in self.cookies:
                # Only add cookies for universalplus domains
                if "universalplus" in (cookie.domain or ""):
                    self.session.cookies.set(cookie.name, cookie.value, domain=cookie.domain)
            
            # Extract authentication from cookies (still needed for tbx_token)
            self._auth_from_cookies()
        
        # Fallback to credentials
        if not self.tbx_token and self.credentials:
            raw = self.credentials.username
            if self.credentials.password:
                raw = self.credentials.password
            self.tbx_token = raw.strip()
        
        if not self.tbx_token:
            raise EnvironmentError(
                "Universal+ requires tbx-token. Export cookies from web.play.universalplus.com"
            )
        
        self._decode_token()
        
        if not self.profile_id:
            raise EnvironmentError("Could not extract profile ID from cookies or token")

    def _auth_from_cookies(self) -> None:
        """Extract tbx-token and profile from cookies using YAML config."""
        auth_cfg = self.auth_config
        token_cookie_name = auth_cfg.get("token_cookie", "token")
        profile_cookie_name = auth_cfg.get("profile_cookie", "profile")
        country_cookie_name = auth_cfg.get("country_cookie", "countryCode")
        token_path = auth_cfg.get("token_path", ["token", "access_token"])
        
        for cookie in self.cookies:
            if cookie.name == token_cookie_name and "universalplus" in cookie.domain:
                token_data = json.loads(unquote(cookie.value))
                
                # Navigate JSON path to get actual token
                access_token = token_data
                for key in token_path:
                    access_token = access_token.get(key, {})
                
                if isinstance(access_token, str) and access_token.startswith("eyJ"):
                    self.tbx_token = access_token
                
                # Try to get country from token cookie
                if not self.country_code:
                    self.country_code = token_data.get("country", "MX")
                    
            elif cookie.name == profile_cookie_name and "universalplus" in cookie.domain:
                profile_data = json.loads(unquote(cookie.value))
                self.profile_id = profile_data.get("id", "")
                
            elif cookie.name == country_cookie_name and "universalplus" in cookie.domain:
                self.country_code = cookie.value

    def _decode_token(self) -> None:
        """Decode JWT token to extract profile and check expiration."""
        import base64
        import time
        
        try:
            payload_b64 = self.tbx_token.split(".")[1]
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.b64decode(payload_b64))
            
            if not self.profile_id:
                self.profile_id = payload.get("profile", "")
            
            if self.country_code == "MX":
                self.country_code = payload.get("country", self.country_code)
            
            exp = payload.get("exp", 0)
            if exp and exp < int(time.time()):
                raise EnvironmentError("tbx-token has expired. Export fresh cookies.")
                
        except Exception as e:
            self.log.debug(f"Token decode failed (non-fatal): {e}")

    def get_titles(self) -> List[Title]:
        """Fetch titles from Universal+."""
        content_id = self._extract_content_id(self.title.get('id', ''))
        
        # First, try to scrape series info from web page
        page_data = self._scrape_page_data(content_id)
        if page_data:
            props = page_data.get("props", {})
            page_props = props.get("pageProps", {})
            details = page_props.get("details", {})
            
            # Check if this is a series with multiple seasons
            seasons = details.get("seasons", [])
            seasons_data = details.get("seasonsData", [])
            
            if seasons and seasons_data:
                series_name = details.get("alternativeTitle") or details.get("title", "")
                self.log.debug(f"Detected series: {series_name} with {len(seasons)} seasons")
                return self._scrape_series_episodes(content_id, series_name)
        
        # If not a series from scrape, try player API
        player_data = self._get_player_data(content_id)
        if not player_data:
            raise ValueError(f"Could not fetch player data for: {content_id}")
        
        content = player_data.get("content", {})
        content_type = content.get("contentType", "").upper()
        title_name = content.get("alternativeTitle") or content.get("title", "")
        
        # Handle EPISODE
        if content_type == "EPISODE":
            serie = content.get("serie", {})
            series_name = serie.get("alternativeTitle") or serie.get("title", title_name)
            serie_id = serie.get("id") or serie.get("shortId", "")
            
            # Check if user passed series ID (not episode)
            if serie_id and content_id in (serie_id, serie.get("shortId", "")):
                return self._get_all_episodes_via_api(serie_id)
            
            # Single episode
            season_num = content.get("season", 1)
            episode_num = content.get("episode", 1)
            year = content.get("release") or content.get("year")
            
            return [Title(
                id_=content.get("id", content_id),
                type_=Title.Types.TV,
                name=series_name,
                year=int(year) if year else None,
                season=int(season_num),
                episode=int(episode_num),
                episode_name=title_name,
                source=self.ALIASES[0] if self.ALIASES else "universalplus",
                service_data=player_data
            )]
        
        # Handle MOVIE
        if content_type == "MOVIE":
            year = content.get("release") or content.get("year") or content.get("releaseYear")
            
            return [Title(
                id_=content.get("id", content_id),
                type_=Title.Types.MOVIE,
                name=title_name,
                year=int(year) if year else None,
                source=self.ALIASES[0] if self.ALIASES else "universalplus",
                service_data=player_data
            )]
        
        # Try as series via API
        return self._get_all_episodes_via_api(content_id)
    
    def _get_series_info(self, content_id: str) -> Optional[dict]:
        """Get series information including seasons."""
        # First try to scrape from the web page (__NEXT_DATA__)
        page_data = self._scrape_page_data(content_id)
        if page_data:
            props = page_data.get("props", {})
            page_props = props.get("pageProps", {})
            details = page_props.get("details", {})
            
            # Check if this is a series (has seasons and seasonsData)
            if details.get("seasons") and details.get("seasonsData"):
                self.log.debug(f"Found series data in page scrape for {content_id}")
                return details
        
        # Fallback: try to get via player API and check if it has series info
        player_data = self._get_player_data(content_id)
        if player_data:
            content = player_data.get("content", {})
            if content.get("contentType") == "SERIES":
                # Build series info from player data
                return {
                    "id": content_id,
                    "title": content.get("alternativeTitle") or content.get("title", ""),
                    "contentType": "SERIES",
                    "seasons": content.get("seasons", []),
                    "seasonsData": content.get("seasonsData", [])
                }
        
        return None
    
    def _get_all_episodes_via_api(self, series_id: str, series_data: dict = None) -> List[Title]:
        """Get all episodes for a series using the API."""
        episodes = []
        
        # Try to get series info from web scrape first (most reliable)
        if not series_data:
            page_data = self._scrape_page_data(series_id)
            if page_data:
                props = page_data.get("props", {})
                page_props = props.get("pageProps", {})
                series_data = page_props.get("details", {})
        
        # If no data from scrape, try API
        if not series_data or not series_data.get("seasons"):
            series_data = self._get_series_info(series_id)
        
        if not series_data:
            self.log.debug(f"No series data found for {series_id}")
            return []
        
        series_name = series_data.get("title") or series_data.get("alternativeTitle", "Unknown Series")
        
        # Get seasons list (array of season numbers)
        seasons = series_data.get("seasons", [])
        
        if not seasons:
            self.log.debug(f"No seasons found for series {series_id}")
            return []
        
        self.log.debug(f"Found {len(seasons)} seasons for {series_name}")
        
        # seasonsData contains episodes organized by season number
        seasons_data = series_data.get("seasonsData", [])
        
        # Create a dict for quick access by season number
        seasons_data_dict = {sd.get("season"): sd for sd in seasons_data if sd.get("season")}
        
        for season_num in seasons:
            season_info = seasons_data_dict.get(season_num)
            if not season_info:
                self.log.debug(f"No data found for season {season_num}")
                continue
            
            season_episodes = season_info.get("episodes", [])
            self.log.debug(f"Season {season_num}: {len(season_episodes)} episodes")
            
            for ep in season_episodes:
                ep_id = ep.get("id")
                ep_title = ep.get("title") or ep.get("alternativeTitle", "")
                ep_number = ep.get("episode", 1)
                ep_year = ep.get("release") or ep.get("year")
                ep_desc = ep.get("description", "")
                ep_short_id = ep.get("shortId", ep_id)
                
                if not ep_id:
                    continue
                
                # Parse year
                year = None
                if ep_year:
                    try:
                        if isinstance(ep_year, (int, float)):
                            year = int(ep_year)
                        elif isinstance(ep_year, str):
                            year_match = re.search(r'\b(19|20)\d{2}\b', ep_year)
                            if year_match:
                                year = int(year_match.group(0))
                    except:
                        pass
                
                episodes.append(Title(
                    id_=str(ep_id),
                    type_=Title.Types.TV,
                    name=series_name,
                    season=int(season_num),
                    episode=int(ep_number),
                    episode_name=ep_title,
                    year=year,
                    source=self.ALIASES[0] if self.ALIASES else "universalplus",
                    service_data={
                        "short_id": ep_short_id,
                        "season": season_num,
                        "episode": ep_number
                    }
                ))
        
        # Sort episodes
        episodes.sort(key=lambda x: (x.season, x.episode))
        
        if episodes:
            seasons_found = set(e.season for e in episodes)
            self.log.info(f"Found {len(episodes)} episodes across {len(seasons_found)} seasons")
            for season in sorted(seasons_found):
                count = len([e for e in episodes if e.season == season])
                self.log.info(f"  Season {season}: {count} episodes")
        
        return episodes

    def _scrape_series_episodes(self, series_id: str, series_name: str) -> List[Title]:
        """Scrape episodes from __NEXT_DATA__."""
        page_data = self._scrape_page_data(series_id)
        if not page_data:
            return []
        
        props = page_data.get("props", {})
        page_props = props.get("pageProps", {})
        details = page_props.get("details", {})
        
        # Get seasons list (array of season numbers)
        seasons = details.get("seasons", [])
        
        if not seasons:
            self.log.debug("No seasons found in page data")
            return []
        
        episodes = []
        
        # seasonsData contains episodes organized by season number
        seasons_data = details.get("seasonsData", [])
        
        # Create a dict for quick access by season number
        seasons_data_dict = {sd.get("season"): sd for sd in seasons_data if sd.get("season")}
        
        for season_num in seasons:
            season_info = seasons_data_dict.get(season_num)
            if not season_info:
                self.log.debug(f"Season {season_num} data not found in seasonsData")
                continue
            
            season_episodes = season_info.get("episodes", [])
            self.log.debug(f"Processing season {season_num} with {len(season_episodes)} episodes")
            
            for ep_data in season_episodes:
                ep_id = ep_data.get("id") or ep_data.get("contentId", "")
                ep_title = ep_data.get("title") or ep_data.get("alternativeTitle", "")
                ep_number = ep_data.get("episode", 1)
                ep_year = ep_data.get("release") or ep_data.get("year")
                ep_desc = ep_data.get("description", "")
                ep_short_id = ep_data.get("shortId", ep_id)
                
                if not ep_id:
                    continue
                
                year = None
                if ep_year:
                    try:
                        if isinstance(ep_year, (int, float)):
                            year = int(ep_year)
                        elif isinstance(ep_year, str):
                            year_match = re.search(r'\b(19|20)\d{2}\b', ep_year)
                            if year_match:
                                year = int(year_match.group(0))
                    except:
                        pass
                
                episodes.append(Title(
                    id_=str(ep_id),
                    type_=Title.Types.TV,
                    name=series_name,
                    season=int(season_num),
                    episode=int(ep_number),
                    episode_name=ep_title,
                    year=year,
                    source=self.ALIASES[0] if self.ALIASES else "universalplus",
                    service_data={"short_id": ep_short_id}
                ))
        
        # Sort by season and episode
        episodes.sort(key=lambda x: (x.season, x.episode))
        
        if episodes:
            seasons_found = set(e.season for e in episodes)
            self.log.info(f"Found {len(episodes)} episodes across {len(seasons_found)} seasons")
            for season in sorted(seasons_found):
                season_eps = [e for e in episodes if e.season == season]
                self.log.debug(f"  Season {season}: {len(season_eps)} episodes")
        
        return episodes

    def _find_all_episodes(self, data, current_depth=0, max_depth=None) -> list:
        """Recursively find all episode-like objects in JSON data."""
        if max_depth is None:
            max_depth = self.scraping_config.get("max_recursion_depth", 15)
        
        if current_depth > max_depth:
            return []
        
        results = []
        episode_type = self.content_types.get("episode", "EPISODE")
        
        if isinstance(data, dict):
            # Check if this dict is an episode
            ct = data.get("contentType", "")
            if ct == episode_type and data.get("id"):
                results.append(data)
            else:
                # Recursively search values
                for value in data.values():
                    results.extend(self._find_all_episodes(value, current_depth + 1, max_depth))
        elif isinstance(data, list):
            for item in data:
                results.extend(self._find_all_episodes(item, current_depth + 1, max_depth))
        
        return results

    def _convert_episodes_to_titles(self, episodes_data: list, series_name: str) -> List[Title]:
        """Convert raw episode data to Title objects."""
        titles = []
        for ep_data in episodes_data:
            ep_id = ep_data.get("id", "")
            ep_title = ep_data.get("title") or ep_data.get("alternativeTitle", "")
            ep_number = ep_data.get("episode", 1)
            season_num = ep_data.get("season", 1)
            ep_year = ep_data.get("release") or ep_data.get("year")
            ep_desc = ep_data.get("description", "")
            
            if not ep_id:
                continue
            
            year = None
            if ep_year:
                try:
                    if isinstance(ep_year, (int, float)):
                        year = int(ep_year)
                    elif isinstance(ep_year, str):
                        year_match = re.search(r'\b(19|20)\d{2}\b', ep_year)
                        if year_match:
                            year = int(year_match.group(0))
                except:
                    pass
            
            titles.append(Title(
                id_=str(ep_id),
                type_=Title.Types.TV,
                name=series_name,
                season=int(season_num),
                episode=int(ep_number),
                episode_name=ep_title,
                year=year,
                source=self.ALIASES[0] if self.ALIASES else "universalplus",
                service_data={"short_id": ep_data.get("shortId", ep_id)}
            ))
        
        return titles

    def _chain_episodes(self, series_id: str, series_name: str, player_data: dict) -> List[Title]:
        """Fallback: follow nextContent chain."""
        max_episodes = self.scraping_config.get("max_episodes_chain", 200)
        episodes = []
        next_content = player_data.get("nextContent")
        seen_ids = set()
        
        while next_content and len(episodes) < max_episodes:
            nc_id = next_content.get("id", "")
            if nc_id in seen_ids:
                break
            seen_ids.add(nc_id)
            
            episodes.append(Title(
                id_=nc_id,
                type_=Title.Types.TV,
                name=series_name,
                season=int(next_content.get("season", 1)),
                episode=int(next_content.get("episode", 1)),
                episode_name=next_content.get("alternativeTitle") or next_content.get("title", ""),
                source=self.ALIASES[0] if self.ALIASES else "universalplus",
                service_data={"short_id": next_content.get("shortId", nc_id)}
            ))
            
            # Fetch next episode
            try:
                nc_player = self._get_player_data(next_content.get("shortId", nc_id))
                next_content = nc_player.get("nextContent") if nc_player else None
            except Exception as e:
                self.log.debug(f"Failed to fetch next episode: {e}")
                break
        
        return episodes

    def get_tracks(self, title: Title) -> Tracks:
        """Fetch tracks for the title."""
        # Get fresh player data
        short_id = title.service_data.get("short_id", title.id)
        player_data = self._get_player_data(short_id)
        
        if not player_data:
            return Tracks()
        
        entitlements = player_data.get("entitlements", [])
        
        # Find DASH entitlement (preferred) or HLS as fallback
        dash_ent = None
        for ent in entitlements:
            if ent.get("extension") == "mpd":
                dash_ent = ent
                break
        
        if not dash_ent:
            raise ValueError("No DASH entitlement found")
        
        manifest_url = dash_ent.get("url")
        
        # Get BOTH license URLs (Widevine and PlayReady)
        widevine_license_url = dash_ent.get("drm", {}).get("widevine", {}).get("licenseAcquisitionUrl", "")
        playready_license_url = dash_ent.get("drm", {}).get("playready", {}).get("licenseAcquisitionUrl", "")
        
        self.log.debug(f"Manifest URL: {manifest_url}")
        self.log.debug(f"Widevine License URL: {widevine_license_url[:100] if widevine_license_url else 'None'}...")
        self.log.debug(f"PlayReady License URL: {playready_license_url[:100] if playready_license_url else 'None'}...")
        
        # Fetch and parse manifest
        response = self.session.get(manifest_url)
        response.raise_for_status()
        manifest_data = response.text
        
        # Parse tracks from MPD
        tracks = Tracks.from_mpd(
            url=manifest_url, 
            data=manifest_data, 
            source=self.ALIASES[0] if self.ALIASES else "universalplus"
        )
        
        # Find the video KID first
        video_kid = None
        for video in tracks.videos:
            # Try to get KID from the video track
            if hasattr(video, 'kid') and video.kid:
                video_kid = video.kid
                self.log.debug(f"Found video KID: {video_kid}")
                break
            # If no kid attribute, try to extract from the video track's extra data
            elif hasattr(video, 'extra') and video.extra:
                rep, adapt_set = video.extra
                for prot in adapt_set.findall("ContentProtection"):
                    kid_val = prot.get("{urn:mpeg:cenc:2013}default_KID")
                    if kid_val:
                        video_kid = uuid.UUID(kid_val).hex.lower()
                        self.log.debug(f"Extracted video KID from AdaptationSet: {video_kid}")
                        video.kid = video_kid
                        break
                if video_kid:
                    break
        
        # If we found a video KID, assign it to all audio tracks that don't have one
        if video_kid:
            for audio in tracks.audios:
                if not hasattr(audio, 'kid') or not audio.kid:
                    audio.kid = video_kid
                    self.log.debug(f"Assigned video KID to audio {audio.language}: {video_kid}")
        
        # Also handle the case where audio tracks have a different KID but we still need the correct one
        # For Universal+, all tracks in a single-period manifest share the same KID
        if not video_kid and tracks.audios:
            # Try to get KID from the first audio track's extra data
            for audio in tracks.audios:
                if hasattr(audio, 'extra') and audio.extra:
                    rep, adapt_set = audio.extra
                    for prot in adapt_set.findall("ContentProtection"):
                        kid_val = prot.get("{urn:mpeg:cenc:2013}default_KID")
                        if kid_val:
                            video_kid = uuid.UUID(kid_val).hex.lower()
                            self.log.debug(f"Found audio KID: {video_kid}")
                            break
                    if video_kid:
                        break
            
            if video_kid:
                for audio in tracks.audios:
                    if not hasattr(audio, 'kid') or not audio.kid:
                        audio.kid = video_kid
        
        # Store BOTH license URLs on encrypted tracks
        for track in [*tracks.videos, *tracks.audios]:
            if track.encrypted:
                track.widevine_license_url = widevine_license_url
                track.playready_license_url = playready_license_url
                # For backward compatibility
                track.license_url = widevine_license_url or playready_license_url
                self.log.debug(f"License URLs stored for {track.__class__.__name__}")
        
        # Handle subtitles based on config
        if self.subs_config.get("external_only", True):
            # Clear DASH subtitles (fragmented, slow)
            tracks.subtitles.clear()
            
            # Add external VTT subtitles
            ext_tracks = self._extract_text_tracks(player_data)
            for sub in ext_tracks:
                url = sub.get("src", "")
                lang = sub.get("srclang", "es")
                label = sub.get("label", "")
                
                if not url:
                    continue
                
                tracks.add(TextTrack(
                    id_=f"sub_{lang}",
                    source=self.ALIASES[0] if self.ALIASES else "universalplus",
                    url=url,
                    codec="vtt",
                    language=lang,
                    forced=label == "Forced",
                    sdh=label == "SDH",
                    external=True
                ))
        
        # Set source for downloader
        for track in tracks:
            track.source = "UniversalPlus"
        
        return tracks

    def certificate(self, challenge: bytes, title: Title, track, session_id, **kwargs) -> Optional[bytes]:
        """
        Get service certificate (not used by Universal+ Widevine).
        PlayReady may need this in some implementations.
        """
        self.log.debug("Certificate requested - returning None (not required)")
        return None

    def license(self, challenge: bytes, title: Title, track, session_id, **kwargs) -> Optional[bytes]:
        """
        Get DRM license - supports both Widevine and PlayReady.
        
        The correct license URL is determined by:
        1. If track has playready_license_url and we're using PlayReady CDM
        2. Otherwise use widevine_license_url
        """
        service_name = kwargs.get('service_name', '')
        if service_name:
            self.log.debug(f"Licensing for service: {service_name}")
        
        # Detect if this is a PlayReady request (from dl.py context)
        is_playready = self._is_playready
        
        # Also check if challenge looks like PlayReady (WRMHEADER)
        try:
            challenge_str = challenge.decode('utf-8', errors='ignore')
            if '<WRMHEADER' in challenge_str or challenge_str.startswith('<'):
                is_playready = True
                self.log.debug("Detected PlayReady challenge by content")
        except:
            pass
        
        # Select the appropriate license URL
        license_url = None
        if is_playready:
            license_url = getattr(track, 'playready_license_url', None)
            if license_url:
                self.log.debug("Using PlayReady license URL")
            else:
                # Fallback to widevine if playready not available
                license_url = getattr(track, 'widevine_license_url', None)
                if license_url:
                    self.log.debug("PlayReady URL not found, falling back to Widevine")
        else:
            license_url = getattr(track, 'widevine_license_url', None)
            if license_url:
                self.log.debug("Using Widevine license URL")
            else:
                # Fallback to playready
                license_url = getattr(track, 'playready_license_url', None)
                if license_url:
                    self.log.debug("Widevine URL not found, falling back to PlayReady")
        
        if not license_url:
            # Try legacy attribute
            license_url = getattr(track, 'license_url', None)
            if license_url:
                self.log.debug("Using legacy license URL")
        
        if not license_url:
            raise ValueError(f"No license URL available for track {track.id} (PlayReady: {is_playready})")
        
        self.log.debug(f"License URL: {license_url[:100]}...")
        self.log.debug(f"Challenge size: {len(challenge)} bytes")
        
        # Prepare headers based on DRM type
        headers = {"Content-Type": "application/octet-stream"}
        
        # For PlayReady, sometimes need different headers
        if is_playready:
            headers.update({
                "Origin": "https://web.play.universalplus.com",
                "Referer": "https://web.play.universalplus.com/",
            })
        
        try:
            response = self.session.post(
                url=license_url,
                headers=headers,
                data=challenge,
                timeout=30
            )
            response.raise_for_status()
            
            self.log.debug(f"License response size: {len(response.content)} bytes")
            
            # For PlayReady, the response might be XML wrapped
            if is_playready and response.content:
                try:
                    # Check if response is a license challenge response
                    response_text = response.content.decode('utf-8', errors='ignore')
                    if '<License>' in response_text or '<ms:license>' in response_text:
                        self.log.debug("PlayReady license received successfully")
                except:
                    pass
            
            return response.content
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"License request failed: {e}")
            if hasattr(e, 'response') and e.response:
                self.log.error(f"Response status: {e.response.status_code}")
                self.log.error(f"Response body: {e.response.text[:500]}")
            raise

    def get_chapters(self, title: Title) -> List:
        """No chapters for Universal+."""
        return []

    # ========== PRIVATE HELPERS ==========

    def _extract_content_id(self, value: str) -> str:
        """Extract content ID from URL or direct input."""
        if not value:
            raise ValueError("No content ID provided")
        
        # Try URL patterns
        match = re.search(r"(?:details|watch|player)/([a-z0-9]+)", value, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Direct shortId
        if re.match(r"^[a-z0-9]{5,}$", value, re.IGNORECASE):
            return value
        
        raise ValueError(f"Could not extract content ID from: {value}")

    def _get_player_data(self, content_id: str) -> Optional[dict]:
        """Get player data including entitlements."""
        if not self.profile_id:
            raise ValueError("No profile ID available")
        
        # Use the player endpoint from config
        player_url_template = self.config.get("endpoints", {}).get("player", "")
        player_url = player_url_template.format(
            content_id=content_id,
            profile_id=self.profile_id
        )
        
        # Build portability payload
        portability = json.dumps({
            "platform": self.player_defaults.get("platform", "web"),
            "countryCode": self.country_code,
            "locale": self.player_defaults.get("locale", "es"),
            "userAccountType": self.player_defaults.get("user_account_type", "-1421992352"),
            "userProfileRating": self.player_defaults.get("user_profile_rating", "18"),
        }, separators=(",", ":"))
        
        try:
            response = self.session.post(
                url=player_url,
                params={"platform": "web"},
                json={"playerCustomId": f"web-{uuid.uuid4()}"},
                headers={
                    "tbx-token": self.tbx_token,
                    "x-portability": portability,
                }
            )
            self.log.debug(f"[PLAYER] POST {player_url}")
            self.log.debug(f"[PLAYER] Status: {response.status_code}")
            
            if response.status_code == 401:
                raise EnvironmentError(
                    "tbx-token is invalid or expired. Get a fresh one from your browser."
                )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            self.log.error(f"Failed to get player data: {e}")
            return None

    def _scrape_page_data(self, content_id: str) -> Optional[dict]:
        """Scrape __NEXT_DATA__ from web page."""
        details_url_template = self.config.get("endpoints", {}).get("details", "")
        url = details_url_template.format(content_id=content_id)
        
        script_id = self.scraping_config.get("next_data_script_id", "__NEXT_DATA__")
        
        # Use the session's cookies directly - don't build a separate cookie header
        # The session already has the cookies from _configure_session
        try:
            response = self.session.get(
                url,
                headers={
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": self.config["headers"]["User-Agent"],
                },
                allow_redirects=True,
                timeout=30
            )
            
            self.log.debug(f"Scrape URL: {url} -> Status: {response.status_code}")
            self.log.debug(f"Final URL after redirects: {response.url}")
            
            if response.status_code != 200:
                self.log.debug(f"Failed to fetch page: {response.status_code}")
                return None
            
            # Check if we got redirected to login
            if "login" in response.url:
                self.log.debug("Redirected to login page - session may be invalid")
                return None
            
            # Find __NEXT_DATA__
            match = re.search(
                rf'<script\s+id="{script_id}"[^>]*>(.*?)</script>',
                response.text,
                re.DOTALL
            )
            if not match:
                self.log.debug("__NEXT_DATA__ script not found")
                return None
            
            raw_json = match.group(1)
            self.log.debug(f"__NEXT_DATA__ size: {len(raw_json)} chars")
            
            next_data = json.loads(raw_json)
            
            # Verify we have the data we need
            page_props = next_data.get("props", {}).get("pageProps", {})
            details = page_props.get("details", {})
            seasons_data = details.get("seasonsData", [])
            
            self.log.debug(f"Found {len(seasons_data)} seasons in __NEXT_DATA__")
            
            # Log first season for debugging
            if seasons_data:
                first_season = seasons_data[0]
                self.log.debug(f"First season: {first_season.get('season')} with {len(first_season.get('episodes', []))} episodes")
            
            return next_data
            
        except Exception as e:
            self.log.debug(f"Scrape failed: {e}")
            import traceback
            self.log.debug(traceback.format_exc())
            return None

    def _build_cookie_header(self) -> str:
        """Build Cookie header for web scraping."""
        parts = []
        
        # Use raw cookies if available
        if hasattr(self, "_raw_cookies") and self._raw_cookies:
            for cookie in self._raw_cookies:
                if "universalplus" in (cookie.domain or ""):
                    parts.append(f"{cookie.name}={cookie.value}")
        
        # If no raw cookies, build from token
        if not parts and self.tbx_token and self.profile_id:
            token_json = json.dumps({
                "country": self.country_code,
                "deviceCountry": self.country_code,
                "language": "es",
                "token": {
                    "access_token": self.tbx_token,
                    "auth_type": "JWT",
                },
            }, separators=(",", ":"))
            
            profile_json = json.dumps({
                "id": self.profile_id,
            }, separators=(",", ":"))
            
            parts = [
                f"token={token_json}",
                f"profile={profile_json}",
                "locale=es",
                f"countryCode={self.country_code}",  # Fixed: added missing closing quote
                "maturity=18",
            ]
        
        cookie_str = "; ".join(parts)
        self.log.debug(f"Cookie header length: {len(cookie_str)}")
        return cookie_str

    def _extract_text_tracks(self, player_data: dict) -> list:
        """Extract external subtitle URLs from entitlements."""
        tracks = []
        seen = set()
        
        for ent in player_data.get("entitlements", []):
            for track in ent.get("tracks", []):
                src = track.get("src", "")
                if src and src not in seen and track.get("kind") == "subtitles":
                    seen.add(src)
                    tracks.append(track)
        
        return tracks