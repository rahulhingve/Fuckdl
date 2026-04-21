import re
import m3u8
import click
from hashlib import md5
from langcodes import Language

from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService


class Arte(BaseService):
    """
    Service code for ARTE (https://www.arte.tv).

    \b
    Authorization: None (free content)
    Security: Non-DRM (HLS)

    \b
    Examples:
      poetry run fuckdl dl arte https://www.arte.tv/fr/videos/109067-000-A/la-loi-de-teheran/

    Author: Kiro - fixed by rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["ARTE"]
    GEOFENCE = []

    TITLE_RE = [
        r"^https?://(?:www\.)?arte\.tv/(?P<lang>[a-z]{2})/videos/(?P<id>\d{6}-\d{3}-[AF])",
        r"^(?P<id>\d{6}-\d{3}-[AF])$",
    ]

    API_BASE = "https://api.arte.tv/api/player/v2"

    @staticmethod
    @click.command(name="Arte", short_help="https://www.arte.tv")
    @click.argument("title", type=str, required=False)
    @click.option("-l", "--lang", type=str, default="fr", help="Language: fr, de, en, es, it, pl")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Arte(ctx, **kwargs)

    def __init__(self, ctx, title, lang):
        self.parse_title(ctx, title)
        self.lang = lang
        super().__init__(ctx)

        # Extract lang from URL if present
        if title:
            m = re.search(r"arte\.tv/([a-z]{2})/videos/", title)
            if m:
                self.lang = m.group(1)

        self.configure()

    def get_titles(self):
        config = self.session.get(
            url=f"{self.API_BASE}/config/{self.lang}/{self.title}",
            headers={"x-validated-age": "18"},
        ).json()

        metadata = config["data"]["attributes"]["metadata"]
        title_name = metadata.get("subtitle") or metadata.get("title", "")
        description = metadata.get("description", "")
        duration = metadata.get("duration", {}).get("seconds")

        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title_name,
            source=self.ALIASES[0],
            original_lang=metadata.get("language", self.lang),
            service_data=config,
        )

    def get_tracks(self, title):
        config = title.service_data
        streams = config["data"]["attributes"]["streams"]

        tracks = Tracks()
        best_m3u8_url = None
        best_lang_pref = -1

        self.log.debug(f" + Found {len(streams)} streams")

        # Find the best HLS stream (original voice preferred)
        for stream in streams:
            protocol = stream.get("protocol", "")
            self.log.debug(f" + Stream: {protocol} - {stream.get('url', '')[:80]}")
            if "HLS" not in protocol:
                continue

            version = stream.get("versions", [{}])[0]
            code = version.get("eStat", {}).get("ml5", "")

            # Prefer VO (original voice) without subtitles
            lang_pref = 0
            if "VO" in code:
                lang_pref += 10
            if "-ST" not in code:
                lang_pref += 5
            if "AUD" not in code:
                lang_pref += 3

            if lang_pref > best_lang_pref:
                best_lang_pref = lang_pref
                best_m3u8_url = stream["url"]

        if not best_m3u8_url:
            # Fallback: take first HLS stream
            for stream in streams:
                if "HLS" in stream.get("protocol", ""):
                    best_m3u8_url = stream["url"]
                    break

        if not best_m3u8_url:
            # Try HTTPS direct streams
            for stream in streams:
                if stream.get("protocol") in ("HTTPS", "RTMP"):
                    best_m3u8_url = stream["url"]
                    self.log.info(f" + Using direct {stream['protocol']} stream")
                    break

        if not best_m3u8_url:
            self.log.exit(f" - No stream found. Available protocols: {[s.get('protocol') for s in streams]}")

        self.log.info(f" + HLS: {best_m3u8_url}")

        master = m3u8.load(best_m3u8_url)
        tracks = Tracks.from_m3u8(
            master=master,
            source=self.ALIASES[0],
        )

        # Add subtitles from all streams
        for stream in streams:
            if "HLS" not in stream.get("protocol", ""):
                continue
            version = stream.get("versions", [{}])[0]
            code = version.get("eStat", {}).get("ml5", "")
            if "-ST" in code:
                # This stream has subtitles embedded in HLS
                try:
                    sub_master = m3u8.load(stream["url"])
                    for media in sub_master.media:
                        if media.type == "SUBTITLES" and media.uri:
                            sub_url = ("" if re.match("^https?://", media.uri) else media.base_uri) + media.uri
                            tracks.add(TextTrack(
                                id_=md5(sub_url.encode()).hexdigest()[:6],
                                url=sub_url,
                                codec="vtt",
                                language=Language.get(media.language or self.lang),
                                source=self.ALIASES[0],
                                forced=media.forced == "YES",
                            ))
                except Exception:
                    pass

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        return None

    def configure(self):
        self.log.info(" + Starting ARTE...")
