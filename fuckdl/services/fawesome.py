import hashlib
import re
from typing import List, Optional, Union

import click
import requests
from langcodes import Language

from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Tracks, AudioTrack, TextTrack, VideoTrack, MenuTrack
from fuckdl.parsers import m3u8 as m3u8_parser


class Fawesome(BaseService):
    """
    Service code for Fawesome streaming service (https://fawesome.tv/)

    Added by @AnotherBigUserHere
    Original Author: sp4rk.y (ported to unshackle)

    \b
    Version: 1.0.0
    Authorization: None
    Robustness:
        Widevine:
            L3: 1080p, AAC2.0

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    \b
    Tips:
        - Input can be a complete URL or content ID:
            https://fawesome.tv/movies/10686321/the-pledge
            https://fawesome.tv/shows/100946/nash-bridges
        - Movie/show type is auto-detected from URL path
        - Use -m/--movie flag when passing a raw ID for movies
    """

    ALIASES = ["FWSM", "fawesome", "fawesome.tv"]
    GEOFENCE = []  # No specific region required
    TITLE_RE = r"^(?:https?://(?:www\.)?fawesome\.(?:tv|ifood\.tv)/(?P<type>movies|shows|fawesome-topics)/)?(?P<id>\d+)"

    API_BASE = "https://rapi.ifood.tv"
    API_PARAMS = {
        "appId": "7",
        "siteId": "1285",
        "auth-token": "1216525",
        "version": "sv6.0",
    }

    @staticmethod
    @click.command(name="Fawesome", short_help="https://fawesome.tv")
    @click.argument("title", type=str)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Specify if it's a movie")
    @click.pass_context
    def cli(ctx: click.Context, **kwargs: any) -> "Fawesome":
        return Fawesome(ctx, **kwargs)

    def __init__(self, ctx: click.Context, title: str, movie: bool = False) -> None:
        super().__init__(ctx)
        self.title = title
        self.movie = movie

    def authenticate(self) -> None:
        """
        Fawesome doesn't require authentication for free content.
        """
        pass

    def get_titles(self) -> Union[Title, List[Title]]:
        """
        Get titles from Fawesome based on the input.
        """
        match = re.match(self.TITLE_RE, self.title)
        if not match:
            self.log.exit(f"Could not parse title ID from: {self.title}")

        title_id = match.group("id")
        url_type = match.group("type")

        # Override movie flag based on URL type
        if url_type == "movies":
            self.movie = True
        elif url_type == "shows":
            self.movie = False

        if self.movie:
            return self._get_movie(title_id)
        return self._get_series(title_id)

    def _get_movie(self, title_id: str) -> Title:
        """Fetch movie details."""
        node_id = int(title_id)
        full_id = node_id if node_id > 200000000000000 else 200000000000000 + node_id

        info = self.session.get(
            url=f"{self.API_BASE}/recipeInfo.php",
            params={
                **self.API_PARAMS,
                "searchType": "nid",
                "nid": str(full_id),
            },
        ).json()

        node_data = info["node_data"]
        video_id = node_data["video_id"]
        mp4_url = node_data.get("video_url") or node_data.get("video_flv_url") or ""
        m3u8_url = self._construct_m3u8_url(mp4_url, video_id)
        search_data = self._search_by_title(node_data["title"], video_id)

        return Title(
            id_=str(video_id),
            type_=Title.Types.MOVIE,
            name=node_data["title"],
            year=int(search_data.get("release_year", 0)) if search_data.get("release_year") else None,
            original_lang=Language.get(search_data.get("content_language_iso2") or "en"),
            source=self.ALIASES[0],
            service_data={
                "video_url": m3u8_url,
                "cc_path": search_data.get("cc_path", ""),
                "intro_st": search_data.get("intro_st"),
                "intro_et": search_data.get("intro_et"),
                "endcredit_st": search_data.get("endcredit_st"),
                "endcredit_et": search_data.get("endcredit_et"),
            }
        )

    def _get_series(self, show_key: str) -> List[Title]:
        """Fetch all episodes for a series."""
        shows = self.session.get(
            url=f"{self.API_BASE}/shows.php",
            params={
                **self.API_PARAMS,
                "searchType": "listoflist",
                "keys": show_key,
            },
        ).json()

        episodes = []
        channels = shows.get("channels", {})

        for season_num, season_info in channels.items():
            feed_url = season_info.get("feed", "")
            if not feed_url:
                continue

            season_episodes = self._fetch_all_episodes(feed_url)
            series_name = ""

            for ep in season_episodes:
                if not series_name:
                    series_name = ep.get("series_name", "")

                node_id = ep.get("node_id") or ep["id"]
                video_url = ep.get("video_url") or ""

                # Always use HLS â€” m3u8 is universally available and unencrypted
                if not video_url.endswith(".m3u8"):
                    video_url = self._construct_m3u8_url(video_url, int(node_id))

                ep_name = self._parse_episode_name(ep.get("title", ""), series_name)

                episodes.append(
                    Title(
                        id_=str(node_id),
                        type_=Title.Types.TV,
                        name=series_name,
                        season=int(ep.get("season") or season_num),
                        episode=int(ep.get("episode") or 0),
                        episode_name=ep_name,
                        year=int(ep["release_year"]) if ep.get("release_year") else None,
                        original_lang=Language.get(ep.get("content_language_iso2") or "en"),
                        source=self.ALIASES[0],
                        service_data={
                            "video_url": video_url,
                            "cc_path": ep.get("cc_path", ""),
                            "intro_st": ep.get("intro_st"),
                            "intro_et": ep.get("intro_et"),
                            "endcredit_st": ep.get("endcredit_st"),
                            "endcredit_et": ep.get("endcredit_et"),
                        },
                    )
                )

        # Fawesome has inconsistent release_year, use minimum year across episodes
        years = [e.year for e in episodes if e.year]
        if years:
            original_year = min(years)
            for ep in episodes:
                ep.year = original_year

        return episodes

    def _fetch_all_episodes(self, feed_url: str) -> list:
        """Fetch all episodes from a season feed URL."""
        separator = "&" if "?" in feed_url else "?"
        url = f"{feed_url}{separator}max-results=200"
        return self.session.get(url=url).json().get("results", [])

    @staticmethod
    def _parse_episode_name(full_title: str, series_name: str) -> str:
        """Extract episode name from formatted title."""
        name = re.sub(r"^S\d+ E\d+ - ", "", full_title)
        if series_name:
            name = re.sub(r"\s*-\s*" + re.escape(series_name) + r"$", "", name)
        return name

    def _search_by_title(self, title_name: str, node_id: int) -> dict:
        """Search by title name and find matching result by node_id."""
        results = self.session.get(
            url=f"{self.API_BASE}/recipes.php",
            params={
                **self.API_PARAMS,
                "searchType": "search",
                "keys": title_name,
            },
        ).json()

        for result in results.get("results", []):
            if int(result.get("node_id", 0)) == node_id:
                return result

        return {}

    @staticmethod
    def _construct_m3u8_url(video_url: str, node_id: int) -> str:
        """Construct m3u8 URL from an mp4/mpd URL by reusing the CDN path prefix."""
        match = re.search(r"(https?://[^/]+/files/[^/]+/vi/[a-f0-9]{2}/[a-f0-9]{2}/)", video_url)
        if match:
            base = match.group(1)
            return f"{base}{node_id}/s{node_id}.m3u8"
        return video_url

    def get_tracks(self, title: Title) -> Tracks:
        """
        Get tracks for a title from Fawesome.
        """
        video_url = title.service_data.get("video_url", "")
        if not video_url:
            raise ValueError("No video URL found for this title")

        # Parse the HLS manifest
        master = self._get_m3u8_master(video_url)
        
        # Check if it's a variant playlist
        if not master.is_variant or not master.playlists:
            # Handle direct media playlist case
            tracks = Tracks()
            
            # Add video track
            tracks.add(
                VideoTrack(
                    id_=title.id_,
                    source=self.ALIASES[0],
                    url=video_url,
                    codec="h264",
                    bitrate=0,
                    width=1920,
                    height=1080,
                    language=title.original_lang,
                    descriptor=VideoTrack.Descriptor.M3U
                )
            )
            
            # Add audio tracks if available
            if hasattr(master, 'media') and master.media:
                for media in master.media:
                    if media.type == "AUDIO" and media.uri:
                        media_url = media.uri
                        if not media_url.startswith(('http://', 'https://')):
                            media_url = (media.base_uri or '') + media_url
                        
                        tracks.add(
                            AudioTrack(
                                id_=hashlib.md5(media_url.encode()).hexdigest()[:6],
                                source=self.ALIASES[0],
                                url=media_url,
                                codec="aac",
                                bitrate=0,
                                language=Language.get(media.language or "en"),
                                descriptor=AudioTrack.Descriptor.M3U
                            )
                        )
            
            # Add closed captions
            cc_path = title.service_data.get("cc_path", "")
            if cc_path:
                tracks.add(
                    TextTrack(
                        id_=hashlib.md5(cc_path.encode()).hexdigest()[:6],
                        source=self.ALIASES[0],
                        url=cc_path,
                        codec="vtt",
                        language=title.original_lang,
                        sdh=True,
                    )
                )
            
            return tracks
        
        # Use the patched parser
        tracks = m3u8_parser.parse(master, source=self.ALIASES[0])
        
        # Fawesome-specific FPS detection from URL patterns
        for video in tracks.videos:
            if hasattr(video, 'url') and video.url:
                # Check if FPS is still unknown (0 or None)
                if not video.fps or video.fps == 0:
                    # For Fawesome, most content uses standard framerates
                    if video.height >= 720:
                        video.fps = 23.976  # Common for HD content
                    else:
                        video.fps = 29.97   # Common for SD content
                    
                    self.log.debug(f"Set FPS for {video.width}x{video.height} to {video.fps}")
        
        # Enhance video tracks with resolution information for Fawesome-specific URLs
        for video in tracks.videos:
            if hasattr(video, 'url') and video.url:
                if "/hls-hi/" in video.url:
                    video.width, video.height = 1920, 1080
                elif "/hls-xlo/" in video.url:
                    video.width, video.height = 640, 360
                elif "/hls-med/" in video.url:
                    video.width, video.height = 1280, 720
        
        # Add closed captions if available and not already present
        cc_path = title.service_data.get("cc_path", "")
        if cc_path:
            existing = any(
                hasattr(st, 'url') and st.url == cc_path 
                for st in tracks.subtitles
            )
            if not existing:
                tracks.add(
                    TextTrack(
                        id_=hashlib.md5(cc_path.encode()).hexdigest()[:6],
                        source=self.ALIASES[0],
                        url=cc_path,
                        codec="vtt",
                        language=title.original_lang,
                        sdh=True,
                    )
                )
        
        return tracks

    def _get_m3u8_master(self, url: str):
        """Fetch and parse M3U8 master playlist."""
        import m3u8
        
        response = self.session.get(url)
        response.raise_for_status()
        
        # Parse with error handling for malformed playlists
        try:
            master = m3u8.loads(response.text, uri=url)
        except Exception as e:
            self.log.warning(f"Failed to parse M3U8 playlist: {e}")
            # Create a minimal master playlist
            master = m3u8.M3U8()
            master.is_variant = False
            master.playlists = []
            master.segments = []
            master.uri = url
            master.base_uri = url[:url.rfind('/') + 1] if '/' in url else ''
            
            # Try to add segments if possible
            lines = response.text.splitlines()
            segment_urls = [line for line in lines if line and not line.startswith('#')]
            if segment_urls:
                from m3u8.model import Segment
                master.segments = [
                    Segment(uri=seg_uri, base_uri=master.base_uri)
                    for seg_uri in segment_urls
                ]
        
        return master

    def get_chapters(self, title: Title) -> List[MenuTrack]:
        """
        Get chapters for a title based on intro/credits timestamps.
        """
        chapters = []
        service_data = title.service_data

        intro_st = service_data.get("intro_st")
        intro_et = service_data.get("intro_et")
        if intro_st and intro_et and int(intro_st) > 0:
            chapters.append(
                MenuTrack(
                    number=len(chapters) + 1,
                    title="Intro Start",
                    timecode=self._format_time(int(intro_st))
                )
            )
            chapters.append(
                MenuTrack(
                    number=len(chapters) + 1,
                    title="Intro End",
                    timecode=self._format_time(int(intro_et))
                )
            )

        endcredit_st = service_data.get("endcredit_st")
        if endcredit_st and int(endcredit_st) > 0:
            chapters.append(
                MenuTrack(
                    number=len(chapters) + 1,
                    title="Credits",
                    timecode=self._format_time(int(endcredit_st))
                )
            )

        return chapters

    @staticmethod
    def _format_time(seconds: int) -> str:
        """Convert seconds to HH:MM:SS.mmm format."""
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hrs:02d}:{mins:02d}:{secs:02d}.000"

    def license(self, challenge: bytes, title: Title, track, session_id: bytes) -> Optional[bytes]:
        """
        Fawesome content is unencrypted, so no license needed.
        """
        return None

    def certificate(self, challenge: bytes, title: Title, track, session_id: bytes) -> Optional[bytes]:
        """
        No certificate needed for unencrypted content.
        """
        return None