import json
import re
import os
import m3u8
import click
from pathlib import Path

from fuckdl.objects import Title, Tracks
from fuckdl.services.BaseService import BaseService

os.system('')
GREEN = '\033[32m'
MAGENTA = '\033[35m'
YELLOW = '\033[33m'
RESET = '\033[0m'


class Tenk(BaseService):
    """
    Service code for TÃ«nk (https://www.tenk.ca).

    \b
    Authorization: Credentials (GraphQL login)
    Security: Non-DRM (HLS with token auth)

    \b
    Examples:
      poetry run fuckdl dl tenk https://www.tenk.ca/fr/documentaires/antoine
      poetry run fuckdl dl tenk antoine

    Fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["TENK", "tenk", "tÃ«nk"]
    GEOFENCE = ["ca"]

    TITLE_RE = [
        r"^https?://(?:www\.)?tenk\.ca/(?:fr|en)/documentaires/(?P<id>[a-z0-9-]+)",
        r"^(?P<id>[a-z0-9-]+)$",
    ]

    GRAPHQL_URL = "https://platform-359.kinow.io/graphql"
    PLAYER_URL = "https://media.kinow.video/video-player"

    @staticmethod
    @click.command(name="Tenk", short_help="https://www.tenk.ca")
    @click.argument("title", type=str, required=False)
    @click.option("--lang", type=click.Choice(["fr", "en"], case_sensitive=False),
                  default="fr", help="Audio language: fr or en")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Tenk(ctx, **kwargs)

    def __init__(self, ctx, title, lang):
        self.parse_title(ctx, title)
        self.lang = lang.upper()
        super().__init__(ctx)
        self.jwt = None
        self.configure()

    def configure(self):
        self.log.info(" + Starting TÃ«nk...")
        self.jwt = self.get_auth_token()
        self.session.headers.update({
            "Authorization": f"Bearer {self.jwt}",
            "Origin": "https://www.tenk.ca",
            "Referer": "https://www.tenk.ca/",
        })

    # â”€â”€ Auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_auth_token(self):
        cache_path = self.get_cache("token.json")

        if cache_path.is_file():
            cached = json.loads(cache_path.read_text(encoding="utf8"))
            token = cached.get("token")
            if token:
                self.log.info(" + Using cached JWT token")
                return token

        if not self.credentials:
            self.log.exit(" x No credentials provided for TÃ«nk")

        self.log.info(" + Logging in to TÃ«nk...")

        query = """
        mutation SignIn($credentials: SignInInput!) {
            signin(credentials: $credentials) {
                accessToken
                refreshToken
                tokenType
                expiresIn
            }
        }
        """
        resp = self.session.post(self.GRAPHQL_URL, json={
            "query": query,
            "variables": {
                "credentials": {
                    "email": self.credentials.username,
                    "password": self.credentials.password,
                }
            }
        }).json()

        token = resp.get("data", {}).get("signin", {}).get("accessToken")
        if not token:
            errors = resp.get("errors", [{}])
            msg = errors[0].get("message", "Unknown error") if errors else str(resp)
            self.log.exit(f" x Login failed: {msg}")

        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        cache_path.write_text(json.dumps({"token": token}), encoding="utf8")

        self.log.info(f" + {GREEN}Login successful{RESET}")
        return token

    # â”€â”€ GraphQL helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def graphql(self, query, variables=None):
        resp = self.session.post(self.GRAPHQL_URL, json={
            "query": query,
            "variables": variables or {}
        })
        data = resp.json()
        if "errors" in data:
            self.log.exit(f" x GraphQL error: {data['errors'][0]['message']}")
        return data["data"]

    # â”€â”€ Titles â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_titles(self):
        query = """
        query GetProduct($query: String!) {
            cms {
                products(query: $query, perPage: 5) {
                    items {
                        id
                        name
                        linkRewrite
                        descriptionShort
                        images { source type }
                        metadata { name value }
                        type
                        extension {
                            ... on ProductTVOD {
                                videos {
                                    items {
                                        id
                                        name
                                        language { name isoCode }
                                    }
                                }
                                directors {
                                    items {
                                        director { name }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        data = self.graphql(query, {"query": self.title})
        items = data["cms"]["products"]["items"]

        if not items:
            self.log.exit(f" x No product found for: {self.title}")

        # Try to match by linkRewrite first, fallback to first result
        product = None
        for item in items:
            if item.get("linkRewrite") == self.title:
                product = item
                break
        if not product:
            product = items[0]
            self.log.info(f" + No exact slug match, using: {product['name']}")

        meta = {m["name"]: m["value"] for m in product.get("metadata", [])}
        ext = product.get("extension", {}) or {}

        # Find the video matching requested language
        videos = ext.get("videos", {}).get("items", [])
        video = None
        for v in videos:
            if v.get("language", {}).get("isoCode", "").upper() == self.lang:
                video = v
                break
        if not video and videos:
            video = videos[0]
            self.log.info(f" + Language '{self.lang}' not found, using: {video.get('name')}")

        if not video:
            self.log.exit(" x No video found for this product")

        directors = [
            d["director"]["name"]
            for d in ext.get("directors", {}).get("items", [])
        ]

        self.log.info(f" + {GREEN}{product['name']}{RESET} ({meta.get('AnnÃ©e', '?')})")
        self.log.info(f" + Director: {', '.join(directors) if directors else 'N/A'}")
        self.log.info(f" + Duration: {meta.get('DurÃ©e', '?')} min")
        self.log.info(f" + Video ID: {video['id']} ({video.get('language', {}).get('isoCode', '?')})")

        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=product["name"],
            year=int(meta["AnnÃ©e"]) if meta.get("AnnÃ©e") else None,
            source=self.ALIASES[0],
            service_data={"product": product, "video": video},
        )

        meta = {m["name"]: m["value"] for m in product.get("metadata", [])}

        # Find the video matching requested language
        videos = product.get("videos", {}).get("items", [])
        video = None
        for v in videos:
            if v.get("language", {}).get("isoCode", "").upper() == self.lang:
                video = v
                break
        if not video and videos:
            video = videos[0]
            self.log.info(f" + Language '{self.lang}' not found, using: {video.get('name')}")

        if not video:
            self.log.exit(" x No video found for this product")

        directors = [
            d["director"]["name"]
            for d in product.get("extension", {}).get("directors", {}).get("items", [])
        ]

        self.log.info(f" + {GREEN}{product['name']}{RESET} ({meta.get('AnnÃ©e', '?')})")
        self.log.info(f" + Director: {', '.join(directors) if directors else 'N/A'}")
        self.log.info(f" + Duration: {meta.get('DurÃ©e', '?')} min")
        self.log.info(f" + Video ID: {video['id']} ({video.get('language', {}).get('isoCode', '?')})")

        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=product["name"],
            year=int(meta["AnnÃ©e"]) if meta.get("AnnÃ©e") else None,
            source=self.ALIASES[0],
            service_data={"product": product, "video": video},
        )

    # â”€â”€ Tracks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_tracks(self, title):
        video = title.service_data["video"]
        video_id = int(video["id"])

        # Step 1: Get player URL via GraphQL
        query = """
        query GetVideoPlayer($includeIds: [ID!]) {
            cms {
                videos(includeIds: $includeIds) {
                    items {
                        player { url }
                        accessInfo { streaming }
                    }
                }
            }
        }
        """
        data = self.graphql(query, {"includeIds": [str(video_id)]})
        video_items = data["cms"]["videos"]["items"]

        if not video_items:
            self.log.exit(f" x No video player data for video ID {video_id}")

        player_url = video_items[0].get("player", {}).get("url")
        if not player_url:
            self.log.exit(" x No player URL returned. Check your subscription/access.")

        self.log.info(f" + Player URL: {player_url}")

        # Step 2: Fetch the player HTML page and extract playerConfiguration
        resp = self.session.get(player_url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        })

        match = re.search(r"var\s+playerConfiguration\s*=\s*({.+?});\s*//", resp.text, re.DOTALL)
        if not match:
            # Try alternative pattern without trailing comment
            match = re.search(r"var\s+playerConfiguration\s*=\s*({.+?});\s*\(function", resp.text, re.DOTALL)
        if not match:
            self.log.exit(" x Could not extract playerConfiguration from player page")

        player_config = json.loads(match.group(1))

        # Step 3: Extract HLS URL and auth token
        hls_url = player_config.get("source", {}).get("hls", {}).get("src")
        if not hls_url:
            self.log.exit(" x No HLS source found in player configuration")

        # The CDN requires x-km-custom-data for segment auth
        session_id = player_config.get("sessionId")
        signed_url = player_config.get("signedUrlQuery")

        if session_id:
            separator = "&" if "?" in hls_url else "?"
            hls_url = f"{hls_url}{separator}x-km-custom-data={session_id}"
        elif signed_url:
            separator = "&" if "?" in hls_url else "?"
            hls_url = f"{hls_url}{separator}{signed_url}"

        self.log.info(f" + HLS Master: {hls_url[:120]}...")

        # Step 4: Parse the m3u8 master playlist
        master = m3u8.load(hls_url)
        tracks = Tracks.from_m3u8(
            master=master,
            source=self.ALIASES[0],
        )

        # Step 5: Append the auth token to each track's variant m3u8 URL
        # so that when segments are resolved, the base_uri carries the token
        token_query = ""
        if session_id:
            token_query = f"x-km-custom-data={session_id}"
        elif signed_url:
            token_query = signed_url

        if token_query:
            for track in tracks:
                # Store the full master URL with token in extra for N_m3u8DL-RE
                track.extra = hls_url
                if isinstance(track.url, str) and "?" not in track.url:
                    track.url = track.url + "?" + token_query
                elif isinstance(track.url, list):
                    track.url = [
                        (u + ("&" if "?" in u else "?") + token_query)
                        for u in track.url
                    ]

        return tracks

    # â”€â”€ Chapters / DRM (none) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        return None
