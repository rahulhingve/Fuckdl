import json
import os
import re
import sys
from hashlib import md5
from urllib.parse import parse_qs, unquote

import click
import requests
from langcodes import Language

from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Track, Tracks, AudioTrack, TextTrack, MenuTrack
from fuckdl.utils import Cdm
from fuckdl.utils.widevine.device import LocalDevice


class Meliplay(BaseService):
    """
    Service code for Mercado Libre Play (https://play.mercadolibre.com.mx/).
    Thanks to @limaalef for the original vt script which inspired this rewrite.

    \b
    Version: 20260417
    Author: rd
    Authorization: Cookies / None
    Security:
        L1/SL3000: 1080p, AAC2.0
        L3/SL2000: 480p, AAC2.0

    \b
    Tips:
        - Input can be complete title URL or just the ID:
            https://play.mercadolibre.com.mx/ver/shaft/1a32175c9ff5478b8f795899a12f99b5
            1a32175c9ff5478b8f795899a12f99b5
        - Individual season or episode:
            set --mode to season or episode
        - Available in these countries:
            "AR", "BR", "CL", "CO", "EC", "MX", "PE", "UY"
        - Cookies are optional and can be disabled by either,
            - adding 'Meliplay: false' to the profiles section in fuckdl.yml
            - or creating a meliplay.yml service config file with just 'needs_auth: false' in it
        - In Brazil mature content is age-restricted and requires cookies from an authenticated account to access.

    \b
    Notes:
        - Security levels changed in September 2025; before then, HD/FHD was L3/SL2000.
        - Meliplay downmixes the audio to mono for /some/ titles.
        - April 2026, Meliplay killed some of their older endpoints.
    """

    ALIASES = ["MELI", "meli", "MELIPLAY", "meliplay"]
    TITLE_RE = r"(?:https?://)?(?:play\.(?:mercadolivre|mercadolibre).*?/)?(?P<id>[a-f0-9]{32})(?:[/?].*)?"

    @staticmethod
    @click.command(name="Meliplay", short_help="https://play.mercadolibre.com.mx/")
    @click.argument("title", type=str, required=True)
    @click.option("-m", "--mode", default="all",
                  type=click.Choice(["episode", "season", "all"], case_sensitive=False),
                  help="Season mode: 'episode' = single episode, 'season' = all episodes from the same season as the input episode, 'all' = all seasons.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Meliplay(ctx, **kwargs)

    def __init__(self, ctx, title, mode):
        super().__init__(ctx)
        self.title = title
        self.mode = mode
        self.playready = ctx.obj.cdm.device.type == LocalDevice.Types.PLAYREADY
        self.configure()

    def get_titles(self):
        try:
            match = re.match(self.TITLE_RE, self.title)
            content_id = match.group("id")
        except Exception:
            self.log.exit(" - Unable to extract ID from title - is the URL/Path correct?")

        playback_content = self.get_playback_content(content_id)

        if not playback_content:
            self.log.exit(" - Unable to get playback content. The title is not be available or the ID is not valid.")

        kind = playback_content["type"].upper()
        name = playback_content["originalName"] or playback_content["title"]
        year = playback_content["year"]

        if kind == "MOVIE":
            return Title(
                    id_=content_id,
                    type_=Title.Types.MOVIE,
                    name=name,
                    year=year,
                    source=self.ALIASES[0],
                    service_data=playback_content,
                )

        elif kind in ["SEASON", "EPISODE"]:
            episodes = self.get_episodes(playback_content)
            titles = []

            for episode in episodes:
                titles.append(
                    Title(
                        id_=episode["id"],
                        type_=Title.Types.TV,
                        name=name,
                        year=year,
                        season=episode["seasonIndex"],
                        episode=episode["episodeLabel"],
                        episode_name=episode["name"],
                        source=self.ALIASES[0],
                        service_data=episode,
                    )
                )
            return titles

        else:
            self.log.exit(f" - Unsupported content type: {kind}")

    def get_tracks(self, title):
        if not title.service_data.get("source"):
            playback_content = self.get_playback_content(title.id)
            title.service_data = playback_content

            if not playback_content:
                self.log.error(" - Unable to get playback content. Title may not not be available.")
                return []

        source = title.service_data["source"]
        audios = title.service_data["audios"]
        subtitles = title.service_data["subtitles"]
        restriction = title.service_data.get("restrictedBy")
        under_age = {"under_age", "not_atp_user_guest_mlb"}

        if not source["dash"]:
            if restriction in under_age:
                self.log.error(f" - Title is age-restricted, cookies from an authenticated account are required.")
            elif restriction:
                self.log.error(f" - Title is restricted: {restriction}")
            else:
                self.log.error(" - DASH manifest is not available.")
            return []

        drm_type = "playready" if self.playready else "widevine"
        self.license_url = source["drm"][drm_type]["LA_URL"]
        self.auth_token = source["drm"][drm_type]["headers"]["x-dt-auth-token"]

        mpd_url = source["dash"].split('?')[0]
        self.log.debug(f" + Manifest URL: {mpd_url}")

        tracks = Tracks.from_mpd(
            url=mpd_url,
            session=self.session,
            source=self.ALIASES[0],
        )

        # remove mpd subs (if any)
        tracks.subtitles.clear()

        # add subtitles
        for sub in subtitles or []:
            if sub.get("url"):
                tracks.add(TextTrack(
                    id_=md5(sub["url"].encode()).hexdigest(),
                    source=self.ALIASES[0],
                    url=sub["url"],
                    codec="vtt",
                    language=sub["lang"],
                ), warn_only=True)

        # set original lang
        lang_map = {"es": "es-419", "pt": "pt-BR"}
        original = next((x["language"] for x in audios if x["original"]), "es-419")
        title.original_lang = lang_map.get(original, original)

        # normalize lang tags
        for track in tracks:
            tag = track.language.to_tag()
            if tag.startswith(("es", "pt")):
                track.language = Language.get("es-419" if tag.startswith("es") else "pt-BR")

        # dl tracks w/o proxy
        for track in tracks:
            track.needs_proxy = False

        return tracks

    def get_chapters(self, title):
        chapters = []
        timemarks = title.service_data.get("timeMarks", {})
        intro_start = timemarks.get("startIntro")
        intro_end = timemarks.get("endIntro")
        credits = timemarks.get("end")
        name = "Episode" if title.type == Title.Types.TV else "Feature"

        def add_chapter(title, timecode):
            chapters.append(MenuTrack(number=len(chapters) + 1, title=title, timecode=timecode))

        if intro_start and intro_end:
            if intro_start > 30:
                add_chapter("Prologue", "00:00:00.000")
                add_chapter("Intro", self.format_time(intro_start))
            else:
                add_chapter("Intro", "00:00:00.000")

            add_chapter(name, self.format_time(intro_end))

        if credits:
            add_chapter("Credits", self.format_time(credits))

        return chapters

    def certificate(self, **_):
        return None  # will use common privacy cert

    def license(self, challenge, **_):
        try:
            res = self.session.post(
                url=self.license_url,
                headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.5',
                    'origin': self.base_url,
                    'user-agent': self.headers["user-agent"],
                    'x-dt-auth-token': self.auth_token,
                },
                data=challenge
            )
        except requests.exceptions.HTTPError as e:
            self.log_http_error(f"License request failed", e)

        return res.content

    # Service specific functions

    def configure(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0'
        }

        try:
            country_code = self.session.get("https://api.country.is/").json()["country"]
        except Exception:
            country_code = self.session.get("https://ipapi.co/json/").json()["country"]

        if country_code not in {"AR", "BR", "CL", "CO", "EC", "MX", "PE", "UY"}:
            self.log.exit(f" - Mercado Play is not available in the current region ({country_code}).")

        self.base_url = self.get_base_url(country_code)
        self.action_key = "assistir" if self.base_url.endswith(".br") else "ver"
        self.player_url = f"{self.base_url}/{self.action_key}/{{id}}/player"
        self.season_api = f"{self.base_url}/api/season/{{sid}}"


    def get_episodes(self, manifest):
        # single episode
        if self.mode == "episode":
            snum, epnum, epname = self.get_episode_details(manifest["subtitle"])

            ep = manifest.copy()
            ep["episodeLabel"] = epnum
            ep["name"] = epname

            return [ep]

        # seasons (all/single)
        all_episodes = []
        seasons = manifest["show"]["seasons"]

        if self.mode == "season":
            seasons = [s for s in seasons if s["id"] == manifest["seasonId"]]

        for season in seasons:
            sid = season["id"]
            idx = season["index"]

            res = self.session.get(self.season_api.format(sid=sid)).json()
            epds = res["content"]["episodes"]

            for ep in epds:
                ep["seasonIndex"] = idx

            all_episodes.extend(epds)

        return all_episodes


    def get_playback_content(self, content_id: str):
        try:
            response = self.session.get(
                url=self.player_url.format(id=content_id),
                headers = {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.5',
                    'referer': f'{self.base_url}/{self.action_key}/{content_id}',
                    'user-agent': self.headers["user-agent"],
                    'viewport-width': '1920',
                },
                params = {
                    'origin': 'organic',
                }
            )
        except requests.exceptions.HTTPError as e:
            self.log_http_error(f"Playback content request failed", e, log_body=False)

        html_text = response.text
        page_props = self.extract_page_props(html_text) or {}

        queries = page_props.get("pageProps", {}).get("dehydratedState", {}).get("queries") or []
        first_query = queries[0] if queries else {}
        playback_content = first_query.get("state", {}).get("data", {}).get("playbackContent") or {}

        if playback_content:
            cust_params = playback_content.get("source", {}).get("bodyPayload", {}).get("adsParams", {}).get("cust_params")
            show_year = page_props.get("nextEpisode", {}).get("show", {}).get("year")
            year = show_year or self.get_release_year(cust_params)
            playback_content["year"] = year or None

        return playback_content


    def log_http_error(self, message: str, e: requests.exceptions.HTTPError, log_body: bool = True):
        try:
            response_details = json.dumps(e.response.json(), indent=None, ensure_ascii=False)
        except ValueError:
            response_details = e.response.text

        if log_body:
            self.log.error(f" - {message}: {response_details}", exc_info=False)
        else:
            self.log.error(f" - {message}", exc_info=False)

        self.log.error(f" - HTTP Error {e.response.status_code}: {e.response.reason}")
        sys.exit(1)


    def extract_page_props(self, html_text: str):
        payload_match = re.search(r'_n\.ctx\.s\.q\("(.*?)"\);?\s*</script>', html_text, re.DOTALL)

        if payload_match:
            raw_payload = payload_match.group(1).strip()

            try:
                # Remove newlines and unescape
                raw_payload = raw_payload.replace('\n', '').replace('\r', '')
                unescaped_payload = json.loads('"' + raw_payload + '"')

                # Sanitize artifacts
                artifacts_pattern = r'(?<=[\[:,])\s*(u|\$[\d]+|@[\d]+)(?=\s*[\]},])'
                sanitized_payload = re.sub(artifacts_pattern, 'null', unescaped_payload)

                return json.loads(sanitized_payload)

            except json.JSONDecodeError as e:
                snippet = repr(f"{e.doc[max(0, e.pos-20):e.pos]} [ERROR HERE] {e.doc[e.pos:e.pos+20]}")
                self.log.error(f" - Page props JSON parsing failed: {e}. Snippet: {snippet}")

        else:
            self.log.error(" - Page props payload not found in the HTML.")

        return None


    def get_base_url(self, country_code: str):
        if self.cookies:
            match_candidates = (
                re.search(r'(?:^|\.)mercadoli[bv]re(?:\.com)?\.([a-z]{2})$', x.domain)
                for x in self.cookies if x.domain_specified
            )
            if match := next((m for m in match_candidates if m), None):
                country_code = match.group(1)

        country_code = country_code.lower()
        url_map = {
            "br": "https://play.mercadolivre.com.br",
            "cl": "https://play.mercadolibre.cl"
        }

        return url_map.get(country_code, f"https://play.mercadolibre.com.{country_code}")


    @staticmethod
    def get_episode_details(label: str):
        match = re.match(r"T(\d+):E(\d+)\s*[-|]\s*(.*)", label)
        if match:
            return int(match[1]), int(match[2]), match[3].strip()
        return 0, 0, label.strip()


    @staticmethod
    def format_time(seconds: int):
        """Converts seconds to a HH:MM:SS.mmm timecode string."""
        total_ms = int(round(seconds * 1000))
        secs, ms = divmod(total_ms, 1000)
        mins, secs = divmod(secs, 60)
        hrs, mins = divmod(mins, 60)
        timecode = f'{hrs:02d}:{mins:02d}:{secs:02d}.{ms:03d}'
        return timecode


    @staticmethod
    def get_release_year(encoded_str: str):
        try:
            decoded = unquote(encoded_str)
            parsed = parse_qs(decoded)

            year = parsed.get("release_year")
            if year:
                return int(year[0])
        except (ValueError, TypeError):
            pass

        return None
