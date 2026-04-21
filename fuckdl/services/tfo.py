import re
import json
import m3u8
import click
from langcodes import Language

from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService


class TFO(BaseService):
    """
    Service code for TFO (https://www.tfo.org).

    \b
    Authorization: None (free content)
    Security: Non-DRM (HLS via JW Player)

    Original Author: Kiro - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    \b
    Examples:
      Film:   poetry run fuckdl dl tfo https://www.tfo.org/regarder/retour-en-bourgogne/GP187969
      Series: poetry run fuckdl dl tfo https://www.tfo.org/regarder/electricien/GP181532
    """

    ALIASES = ["TFO"]
    GEOFENCE = ["ca"]

    TITLE_RE = [
        r"^https?://(?:www\.)?tfo\.org/(?:regarder|film|serie)/[^/]+/(?P<id>GP\d+)",
        r"^(?P<id>GP\d+)$",
    ]

    @staticmethod
    @click.command(name="TFO", short_help="https://www.tfo.org")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return TFO(ctx, **kwargs)

    def __init__(self, ctx, title):
        self.parse_title(ctx, title)
        self.url = title
        super().__init__(ctx)
        self.configure()

    def get_titles(self):
        # Fetch the watch page
        page_url = self.url or f"https://www.tfo.org/regarder/video/{self.title}"
        resp = self.session.get(page_url, allow_redirects=True)
        html = resp.text

        # Extract JW Player manifest URL
        m3u8_match = re.search(r'data-video-playlist="([^"]+)"', html)
        if not m3u8_match:
            self.log.exit(" - Could not find video manifest on page")

        self._manifest_url = m3u8_match.group(1)

        # Extract title from page
        title_match = re.search(r'<title>([^<]+)</title>', html)
        title_name = title_match.group(1).split("|")[0].strip() if title_match else "Unknown"
        title_name = re.sub(r'\s*-\s*TFO\s*$', '', title_name)

        # Try to extract metadata from page
        og_title = re.search(r'property="og:title"\s+content="([^"]+)"', html)
        if og_title:
            title_name = og_title.group(1)

        og_desc = re.search(r'property="og:description"\s+content="([^"]+)"', html)
        description = og_desc.group(1) if og_desc else ""

        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title_name,
            original_lang="fr",
            source=self.ALIASES[0],
            service_data={"manifest_url": self._manifest_url, "description": description},
        )

    def get_tracks(self, title):
        manifest_url = title.service_data.get("manifest_url")
        if not manifest_url:
            self.log.exit(" - No manifest URL")

        self.log.info(f" + HLS: {manifest_url}")

        resp = self.session.get(manifest_url)
        master = m3u8.loads(resp.text, uri=manifest_url)

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
        self.log.info(" + Starting TFO...")
