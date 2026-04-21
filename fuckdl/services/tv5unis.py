# Authors: Kiro
# Created on: 03-04-2026
# Version: 1.0
# Service: TV5Unis (https://www.tv5unis.ca)

import re
import json
import m3u8
import click
import requests
from langcodes import Language

from fuckdl.objects import Title, Tracks
from fuckdl.services.BaseService import BaseService

API_URL = "https://api.tv5unis.ca/graphql"


class TV5Unis(BaseService):
    """
    Service code for TV5Unis (https://www.tv5unis.ca).

    \b
    Authorization: None (free content)
    Security: Non-DRM (HLS via Uplynk)

    \b
    Examples:
      Series: poetry run fuckdl dl tv5unis https://www.tv5unis.ca/vivre-en-foret/saisons/1
      Episode: poetry run fuckdl dl tv5unis https://www.tv5unis.ca/videos/vivre-en-foret/saisons/1/episodes/1

    Original Author: Kiro - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["TV5U", "TV5Unis"]
    GEOFENCE = ["ca"]

    TITLE_RE = [
        r"^https?://(?:www\.)?tv5unis\.ca/(?:videos/)?(?P<id>[a-z0-9-]+)",
        r"^(?P<id>[a-z0-9-]+)$",
    ]

    @staticmethod
    @click.command(name="TV5Unis", short_help="https://www.tv5unis.ca")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return TV5Unis(ctx, **kwargs)

    def __init__(self, ctx, title):
        self.parse_title(ctx, title)
        self.url = title
        self.show_slug = None
        self.season_num = None
        self.episode_num = None
        super().__init__(ctx)

        # Parse URL for season/episode
        if title:
            m = re.search(r"tv5unis\.ca/(?:videos/)?([a-z0-9-]+)(?:/saisons/(\d+))?(?:/episodes/(\d+))?", title)
            if m:
                self.show_slug = m.group(1)
                self.season_num = int(m.group(2)) if m.group(2) else None
                self.episode_num = int(m.group(3)) if m.group(3) else None

        self.configure()

    def get_titles(self):
        if self.episode_num:
            return self._get_single_episode()
        return self._get_season_episodes()

    def _get_single_episode(self):
        data = self._graphql("""
            query($slug: String!, $s: Int, $e: Int) {
              productByRootProductSlug(rootProductSlug: $slug, seasonNumber: $s, episodeNumber: $e) {
                id title duration seasonNumber episodeNumber productionYear summary contentId
                videoElement { ... on Video { id mediaId drmProtected encodings { hls { url } } } }
              }
            }
        """, {"slug": self.show_slug, "s": self.season_num or 1, "e": self.episode_num})

        ep = data.get("productByRootProductSlug")
        if not ep:
            self.log.exit(" - Episode not found")

        return Title(
            id_=ep["id"],
            type_=Title.Types.TV,
            name=self.show_slug.replace("-", " ").title(),
            season=ep.get("seasonNumber", 1),
            episode=ep.get("episodeNumber"),
            episode_name=ep.get("title", ""),
            year=ep.get("productionYear"),
            original_lang="fr",
            source=self.ALIASES[0],
            service_data=ep,
        )

    def _get_season_episodes(self):
        # If no season specified, default to season 1
        season = self.season_num or 1

        # Use GraphQL to get all episodes for this season
        # First get the show info
        show_title = self.show_slug.replace("-", " ").title()

        # Try to get episode count from the page Apollo State
        page_url = self.url or f"https://www.tv5unis.ca/{self.show_slug}/saisons/{season}"
        resp = self.session.get(page_url)
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text, re.DOTALL)
        if m:
            next_data = json.loads(m.group(1))
            apollo = next_data.get("props", {}).get("apolloState", {})
            for key, val in apollo.items():
                if isinstance(val, dict) and val.get("slug") == self.show_slug and val.get("title"):
                    show_title = val["title"]
                    break

        # Fetch episodes one by one until we get null
        titles = []
        seen_ids = set()
        for ep_num in range(1, 50):
            data = self._graphql("""
                query($slug: String!, $s: Int, $e: Int) {
                  productByRootProductSlug(rootProductSlug: $slug, seasonNumber: $s, episodeNumber: $e) {
                    id title duration seasonNumber episodeNumber productionYear summary contentId
                    videoElement { ... on Video { id mediaId drmProtected encodings { hls { url } } } }
                  }
                }
            """, {"slug": self.show_slug, "s": season, "e": ep_num})

            product = data.get("productByRootProductSlug")
            if not product:
                break

            # Skip duplicates and wrong season
            pid = product["id"]
            if pid in seen_ids:
                break
            if product.get("seasonNumber") != season:
                break
            if product.get("episodeNumber") != ep_num:
                break
            seen_ids.add(pid)

            titles.append(Title(
                id_=product["id"],
                type_=Title.Types.TV,
                name=show_title,
                season=season,
                episode=ep_num,
                episode_name=product.get("title", ""),
                year=product.get("productionYear"),
                original_lang="fr",
                source=self.ALIASES[0],
                service_data=product,
            ))

        if not titles:
            self.log.exit(" - No episodes found")

        return titles

    def get_tracks(self, title):
        # Re-fetch fresh HLS URL (tokens expire quickly)
        data = self._graphql("""
            query($slug: String!, $s: Int, $e: Int) {
              productByRootProductSlug(rootProductSlug: $slug, seasonNumber: $s, episodeNumber: $e) {
                videoElement { ... on Video { encodings { hls { url } } } }
              }
            }
        """, {"slug": self.show_slug, "s": title.season, "e": title.episode})

        product = data.get("productByRootProductSlug") or {}
        video = product.get("videoElement") or {}
        hls_url = (video.get("encodings") or {}).get("hls", {}).get("url")

        if not hls_url:
            self.log.warning(f" - No HLS URL for this episode (not yet available?)")
            return Tracks()

        self.log.info(f" + HLS: {hls_url}")

        try:
            from curl_cffi.requests import Session as CffiSession
            cffi = CffiSession(impersonate="chrome")
            resp = cffi.get(hls_url)
            resp_text = resp.text
            if resp.status_code == 403 or '#EXTM3U' not in resp_text:
                self.log.warning(f" - Manifest returned {resp.status_code}, retrying...")
                # Retry with fresh URL
                data2 = self._graphql("""
                    query($slug: String!, $s: Int, $e: Int) {
                      productByRootProductSlug(rootProductSlug: $slug, seasonNumber: $s, episodeNumber: $e) {
                        videoElement { ... on Video { encodings { hls { url } } } }
                      }
                    }
                """, {"slug": self.show_slug, "s": title.season, "e": title.episode})
                hls_url = (data2.get("productByRootProductSlug") or {}).get("videoElement", {}).get("encodings", {}).get("hls", {}).get("url", hls_url)
                resp = cffi.get(hls_url)
                resp_text = resp.text
        except ImportError:
            resp_text = self.session.get(hls_url).text

        if '#EXTM3U' not in resp_text:
            self.log.warning(f" - Could not load manifest (expired token?)")
            return Tracks()

        master = m3u8.loads(resp_text, uri=hls_url)
        tracks = Tracks.from_m3u8(master=master, source=self.ALIASES[0])

        # TV5Unis is non-DRM but uses AES-128, mark not encrypted and store manifest URL
        for track in tracks:
            track.encrypted = False
            track.extra = hls_url

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        return None

    def configure(self):
        self.log.info(" + Starting TV5Unis...")

    def _graphql(self, query, variables=None):
        r = self.session.post(
            API_URL,
            json={"query": query, "variables": variables or {}},
            headers={"Content-Type": "application/json"},
        )
        data = r.json()
        if "errors" in data:
            self.log.warning(f" - GraphQL errors: {data['errors'][0].get('message', '')[:100]}")
        return data.get("data", {})
