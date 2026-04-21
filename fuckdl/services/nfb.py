# Authors: Kiro
# Created on: 03-04-2026
# Version: 1.0
# Service: ONF/NFB (https://www.onf.ca / https://www.nfb.ca)

import re
import json
import m3u8
import click
from hashlib import md5
from langcodes import Language

from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService


class NFB(BaseService):
    """
    Service code for ONF/NFB (https://www.onf.ca / https://www.nfb.ca).

    \b
    Authorization: None (free content)
    Security: Non-DRM (HLS)

    \b
    Examples:
      Film FR: poetry run fuckdl dl nfb https://www.onf.ca/film/mal-du-siecle/
      Film EN: poetry run fuckdl dl nfb https://www.nfb.ca/film/trafficopter/
      Series:  poetry run fuckdl dl nfb https://www.nfb.ca/series/true-north-inside-the-rise-of-toronto-basketball/season1/episode9/

    Original Author: @Kiro - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["NFB", "ONF"]
    GEOFENCE = ["ca"]

    TITLE_RE = [
        r"^https?://(?:www\.)?(?:nfb|onf)\.ca/(?:film|serie|series)/(?P<id>[^/?#]+)",
        r"^(?P<id>[a-z0-9-]+)$",
    ]

    @staticmethod
    @click.command(name="NFB", short_help="https://www.onf.ca / https://www.nfb.ca")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return NFB(ctx, **kwargs)

    def __init__(self, ctx, title):
        self.parse_title(ctx, title)
        self.url = title
        super().__init__(ctx)

        # Detect site (onf.ca vs nfb.ca)
        self.site = "onf" if title and "onf.ca" in title else "nfb"
        self.lang = "fr" if self.site == "onf" else "en"

        # Detect type (film vs series)
        self.content_type = "film"
        if title and re.search(r"/serie[s]?/", title):
            self.content_type = "series"

        self.configure()

    def get_titles(self):
        # Build the page URL
        slug = self.title
        page_url = self.url or f"https://www.{self.site}.ca/{self.content_type}/{slug}/"

        resp = self.session.get(page_url, allow_redirects=True)
        html = resp.text

        # Extract player data
        player_match = re.search(
            r"window\.PLAYER_OPTIONS\[(['\"])[^\]]+\1\]\s*=\s*(\{.+?\})\s*$",
            html, re.MULTILINE,
        )
        if not player_match:
            self.log.exit(" - Could not find player data on page")

        player_data = json.loads(player_match.group(2))

        # Extract metadata from page
        title_name = self._extract_regex(
            r'["\']nfb_version_title["\']\s*:\s*["\']([^"\']+)', html, "Unknown"
        )
        year = self._extract_regex(
            r'["\']nfb_version_year["\']\s*:\s*["\']([^"\']+)', html, None
        )
        description = self._extract_regex(
            r'<[^>]+\bid=["\']tabSynopsis["\'][^>]*>\s*<p[^>]*>\s*([^<]+)', html, ""
        )
        director = self._extract_regex(
            r'<[^>]+\bitemprop=["\']director["\'][^>]*>([^<]+)', html, None
        )

        return Title(
            id_=slug,
            type_=Title.Types.MOVIE,
            name=title_name,
            year=int(year) if year and year.isdigit() else None,
            original_lang=self.lang,
            source=self.ALIASES[0],
            service_data={
                "player": player_data,
                "description": description.strip() if description else "",
                "director": director,
            },
        )

    def get_tracks(self, title):
        player = title.service_data.get("player", {})
        m3u8_url = player.get("source")

        if not m3u8_url:
            self.log.exit(" - No HLS source found in player data")

        self.log.info(f" + HLS: {m3u8_url}")

        master = m3u8.load(m3u8_url)
        tracks = Tracks.from_m3u8(
            master=master,
            source=self.ALIASES[0],
        )

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        return None

    def configure(self):
        self.log.info(" + Starting ONF/NFB...")

    @staticmethod
    def _extract_regex(pattern, text, default=None):
        m = re.search(pattern, text)
        return m.group(1) if m else default
