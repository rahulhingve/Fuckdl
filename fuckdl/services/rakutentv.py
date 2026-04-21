from __future__ import annotations

import os
import base64
import datetime
import hashlib
import hmac
import re
import urllib.parse
import click
from requests.exceptions import HTTPError

from fuckdl.config import config, directories
from fuckdl.objects import TextTrack, Title, Tracks
from fuckdl.services.BaseService import BaseService
from copy import copy
from langcodes import *
from pymediainfo import MediaInfo

import requests
from requests.adapters import HTTPAdapter, Retry
from fuckdl.utils.widevine.device import LocalDevice

os.system('')
GREEN = '\033[32m'
RED = '\033[31m'
RESET = '\033[0m'


class RakutenTV(BaseService):
    """
    Service code for Rakuten TV (https://rakuten.tv).

    \b
    Authorization: Credentials
    Security: FHD-UHD@L1, SD-FHD@L3

    \b
    Examples:
      poetry run fuckdl dl rktn https://rakuten.tv/fr/movies/le-diable-a-ma-porte
      poetry run fuckdl dl rktn le-diable-a-ma-porte

    Fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["RKTN", "rakuten", "rakutentv"]
    TITLE_RE = r"^(?:https?://(?:www\.)?rakuten\.tv/([a-z]+/|)movies(?:/[a-z]{2})?/)(?P<id>[a-z0-9-]+)"

    @staticmethod
    @click.command(name="RakutenTV", short_help="rakuten.tv")
    @click.argument("title", type=str, required=False)
    @click.option("-dev", "--device", default=None,
                  type=click.Choice(["web", "android", "atvui40", "lgui40", "smui40"], case_sensitive=True),
                  help="Device to use for requests.")
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return RakutenTV(ctx, **kwargs)

    def __init__(self, ctx, title, device, movie):
        cdm = ctx.obj.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.vcodec = ctx.parent.params["vcodec"] or "H264"
        self.resolution = "UHD" if self.vcodec.lower() == "h265" else "FHD"
        self.device = device
        super().__init__(ctx)

        self.device = "lgui40" if self.playready else "android"
        self.parse_title(ctx, title)
        self.movie = movie or "movies" in (title or "")
        self.range = ctx.parent.params["range_"]
        self.wv_pssh = None
        self.configure()

    def _create_wv_pssh(self, kid_hex):
        """Build a Widevine PSSH box from a KID hex string."""
        import struct
        WV_SYSTEM_ID = bytes.fromhex("edef8ba979d64acea3c827dcd51d21ed")
        kid_bytes = bytes.fromhex(kid_hex.replace("-", ""))
        init_data = b'\x08\x01\x12\x10' + kid_bytes
        box_size = 4 + 4 + 4 + 16 + 4 + len(init_data)
        pssh_box = struct.pack(">I", box_size)
        pssh_box += b'pssh'
        pssh_box += b'\x00\x00\x00\x00'
        pssh_box += WV_SYSTEM_ID
        pssh_box += struct.pack(">I", len(init_data))
        pssh_box += init_data
        return base64.b64encode(pssh_box).decode("utf-8")

    def configure(self):
        self.session.headers.update({
            "Origin": "https://rakuten.tv/",
            "User-Agent": "Mozilla/5.0 (Linux; Android 11; SHIELD Android TV Build/RQ1A.210105.003; wv) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/99.0.4844.88 Mobile Safari/537.36",
        })

    def generate_signature(self, url):
        up = urllib.parse.urlparse(url)
        digester = hmac.new(
            self.access_token.encode(),
            f"POST{up.path}{up.query}".encode(),
            hashlib.sha1,
        )
        return base64.b64encode(digester.digest()).decode("utf-8").replace("+", "-").replace("/", "_")

    def pair_device(self):
        if not self.credentials:
            self.log.exit(" x No credentials provided, unable to log in.")
        try:
            res = self.session.post(
                url=self.config["endpoints"]["auth"],
                params={"device_identifier": self.config["clients"][self.device]["device_identifier"]},
                data={
                    "app_version": self.config["clients"][self.device]["app_version"],
                    "device_metadata[uid]": self.config["clients"][self.device]["device_serial"],
                    "device_metadata[os]": self.config["clients"][self.device]["device_os"],
                    "device_metadata[model]": self.config["clients"][self.device]["device_model"],
                    "device_metadata[year]": self.config["clients"][self.device]["device_year"],
                    "device_serial": self.config["clients"][self.device]["device_serial"],
                    "device_metadata[trusted_uid]": False,
                    "device_metadata[brand]": self.config["clients"][self.device]["device_brand"],
                    "classification_id": 69,
                    "user[password]": self.credentials.password,
                    "device_metadata[app_version]": self.config["clients"][self.device]["app_version"],
                    "user[username]": self.credentials.username,
                    "device_metadata[serial_number]": self.config["clients"][self.device]["device_serial"],
                },
            ).json()
        except HTTPError as e:
            if e.response.status_code == 403:
                self.log.exit(" x Rakuten returned 403. IP may be detected as proxy.")
        if "errors" in res:
            error = res["errors"][0]
            if "exception.forbidden_vpn" in error["code"]:
                self.log.exit(" x RakutenTV is detecting this VPN or Proxy")
            else:
                self.log.exit(f" x Login failed: {error['message']} [{error['code']}]")

        self.access_token = res["data"]["user"]["access_token"]
        self.ifa_subscriber_id = res["data"]["user"]["avod_profile"]["ifa_subscriber_id"]
        self.session_uuid = res["data"]["user"]["session_uuid"]
        self.classification_id = res["data"]["user"]["profile"]["classification"]["id"]
        self.locale = res["data"]["market"]["locale"]
        self.market_code = res["data"]["market"]["code"]
        self.log.info(f" + {GREEN}Logged in to Rakuten TV{RESET}")

    def get_info(self, title):
        self.kind = title["labels"]["purchase_types"][0]["kind"]
        self.available_hdr_types = [x for x in title["labels"]["hdr_types"]]

        if any(x["abbr"] == "HDR10" for x in self.available_hdr_types) and any(
            x["abbr"] == "HDR10" for x in title["view_options"]["support"]["hdr_types"]
        ):
            self.hdr_type = "HDR10"
        else:
            self.hdr_type = "NONE"

        if len(title["view_options"]["private"]["offline_streams"]) == 1:
            self.audio_languages = [
                x["abbr"] for x in title["view_options"]["private"]["streams"][0]["audio_languages"]
            ]
        else:
            self.audio_languages = [
                x["abbr"] for x in [
                    x["audio_languages"][0] for x in title["view_options"]["private"]["streams"]
                ]
            ]

        # Prioritize market language
        market_lang_map = {"fr": "FRA", "es": "SPA", "de": "DEU", "it": "ITA", "gb": "ENG", "uk": "ENG"}
        preferred = market_lang_map.get(self.market_code, "ENG")
        if preferred in self.audio_languages:
            self.audio_languages.remove(preferred)
            self.audio_languages.insert(0, preferred)

        return title

    # â”€â”€ Titles â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_titles(self):
        self.pair_device()

        title_url = self.config["endpoints"]["title"].format(
            title_id=self.title
        ) + urllib.parse.urlencode({
            "classification_id": self.classification_id,
            "device_identifier": self.config["clients"][self.device]["device_identifier"],
            "device_serial": self.config["clients"][self.device]["device_serial"],
            "locale": self.locale,
            "market_code": self.market_code,
            "session_uuid": self.session_uuid,
            "timestamp": f"{int(datetime.datetime.now().timestamp())}005",
        })

        title = self.session.get(url=title_url).json()
        if "errors" in title:
            error = title["errors"][0]
            self.log.exit(f" x {error['message']} [{error['code']}]")

        title = self.get_info(title["data"])

        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title["title"],
            year=title["year"],
            original_lang="en",
            source=self.ALIASES[0],
            service_data=title,
        )

    # â”€â”€ Tracks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_tracks(self, title):
        stream_info = self.get_avod() if self.kind == "avod" else self.get_me()
        if "errors" in stream_info:
            error = stream_info["errors"][0]
            self.log.exit(f" x {error['message']} [{error['code']}]")

        stream_info = stream_info["data"]["stream_infos"][0]
        self.license_url = stream_info["license_url"]

        # Extract KID from media_key and build WV PSSH
        media_key = stream_info.get("media_key", "")
        kid_hex = media_key.split("-")[0] if media_key and len(media_key.split("-")[0]) == 32 else None
        if kid_hex:
            self.wv_pssh = self._create_wv_pssh(kid_hex)
            self.log.info(f" + Built WV PSSH from KID: {kid_hex}")

        self.log.info(f" + MPD: {stream_info['url']}")
        self.log.info(f" + License: {self.license_url}")

        tracks = Tracks.from_mpd(
            url=stream_info["url"],
            session=self.session,
            source=self.ALIASES[0],
        )

        # Add subtitles (prefer SRT, fallback to VTT)
        added_langs = set()
        for subtitle in stream_info.get("all_subtitles", []):
            if subtitle["format"] == "srt":
                lang_key = f"{subtitle['locale']}_{subtitle.get('subtitle_type', 'full')}"
                added_langs.add(lang_key)
                tracks.add(TextTrack(
                    id_=hashlib.md5(subtitle["url"].encode()).hexdigest()[0:6],
                    source=self.ALIASES[0],
                    url=subtitle["url"],
                    codec="srt",
                    forced=subtitle.get("subtitle_type") == "forced",
                    language=subtitle["locale"],
                ))
        # Add VTT for any language not already covered by SRT
        for subtitle in stream_info.get("all_subtitles", []):
            if subtitle["format"] == "vtt":
                lang_key = f"{subtitle['locale']}_{subtitle.get('subtitle_type', 'full')}"
                if lang_key not in added_langs:
                    tracks.add(TextTrack(
                        id_=hashlib.md5(subtitle["url"].encode()).hexdigest()[0:6],
                        source=self.ALIASES[0],
                        url=subtitle["url"],
                        codec="vtt",
                        forced=subtitle.get("subtitle_type") == "forced",
                        language=subtitle["locale"],
                    ))

        for video in tracks.videos:
            if "HDR10" in video.url:
                video.hdr10 = True

        # Discover additional audio languages by probing URLs
        self._append_audio_tracks(tracks)

        return tracks

    def _append_audio_tracks(self, tracks):
        """Probe for additional audio languages and codecs not in the MPD."""
        if not tracks.audios or not self.audio_languages:
            return
        base_audio = tracks.audios[0]
        base_url = base_audio.url

        # Find the language and codec pattern in the base URL
        # Pattern: audio-{lang}-{codec}-{n}
        match = re.search(r'audio-([a-z]{2,3})-([a-z0-9-]+)-(\d+)', base_url)
        if not match:
            self.log.info(" + Could not parse audio URL pattern, skipping audio discovery")
            return

        base_lang = match.group(1)
        base_codec = match.group(2)
        base_n = match.group(3)

        existing = {(str(a.language), a.codec) for a in tracks.audios}

        for language in self.audio_languages:
            lang_lower = language.lower()
            for codec in ["ec-3", "ac-3", "mp4a", "dts"]:
                if (lang_lower, codec) in existing:
                    continue
                isma = base_url.replace(
                    f"audio-{base_lang}-{base_codec}-{base_n}",
                    f"audio-{lang_lower}-{codec}-1"
                )
                if isma == base_url:
                    continue
                try:
                    if self.session.head(isma, timeout=5).status_code != 200:
                        continue
                except Exception:
                    continue
                audio = copy(base_audio)
                audio.codec = codec
                audio.url = isma
                audio.id = hashlib.md5(isma.encode()).hexdigest()
                audio.language = Language.get(lang_lower)
                tracks.audios.append(audio)
                existing.add((lang_lower, codec))

    def get_avod(self):
        stream_info_url = self.config["endpoints"]["manifest"].format(
            kind="avod"
        ) + urllib.parse.urlencode({
            "device_stream_video_quality": self.resolution,
            "device_identifier": self.config["clients"][self.device]["device_identifier"],
            "market_code": self.market_code,
            "session_uuid": self.session_uuid,
            "timestamp": f"{int(datetime.datetime.now().timestamp())}122",
        })
        stream_info_url += "&signature=" + self.generate_signature(stream_info_url)
        return self.session.post(
            url=stream_info_url,
            data={
                "hdr_type": self.hdr_type,
                "audio_quality": "5.1",
                "app_version": self.config["clients"][self.device]["app_version"],
                "content_id": self.title,
                "video_quality": self.resolution,
                "audio_language": self.audio_languages[0],
                "video_type": "stream",
                "device_serial": self.config["clients"][self.device]["device_serial"],
                "content_type": "movies" if self.movie else "episodes",
                "classification_id": self.classification_id,
                "subtitle_language": "MIS",
                "player": self.config["clients"][self.device]["player"],
            },
        ).json()

    def get_me(self):
        stream_info_url = self.config["endpoints"]["manifest"].format(
            kind="me"
        ) + urllib.parse.urlencode({
            "audio_language": self.audio_languages[0],
            "audio_quality": "5.1",
            "classification_id": self.classification_id,
            "content_id": self.title,
            "content_type": "movies" if self.movie else "episodes",
            "device_identifier": self.config["clients"][self.device]["device_identifier"],
            "device_serial": "not_implemented",
            "device_stream_audio_quality": "5.1",
            "device_stream_hdr_type": self.hdr_type,
            "device_stream_video_quality": self.resolution,
            "device_uid": "affa434b-8b7c-4ff3-a15e-df1fe500e71e",
            "device_year": self.config["clients"][self.device]["device_year"],
            "disable_dash_legacy_packages": "false",
            "gdpr_consent": self.config["gdpr_consent"],
            "gdpr_consent_opt_out": 0,
            "hdr_type": self.hdr_type,
            "ifa_subscriber_id": self.ifa_subscriber_id,
            "locale": self.locale,
            "market_code": self.market_code,
            "player": self.config["clients"][self.device]["player"],
            "session_uuid": self.session_uuid,
            "subtitle_language": "MIS",
            "timestamp": f"{int(datetime.datetime.now().timestamp())}122",
            "video_type": "stream",
        })
        stream_info_url += "&signature=" + self.generate_signature(stream_info_url)
        return self.session.post(url=stream_info_url).json()

    # â”€â”€ DRM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    def get_chapters(self, title):
        return []

    def certificate(self, **kwargs):
        return self.config.get("certificate")

    def license(self, challenge, **_):
        res = self.session.post(url=self.license_url, data=challenge)
        if "errors" in res.text:
            data = res.json()
            msg = data["errors"][0]["message"]
            if "Forbidden" in msg:
                self.log.exit(" x CDM not eligible or blacklisted by RakutenTV")
            elif "An error happened" in msg:
                self.log.exit(" x CDM seems revoked, can't decrypt this content")
        return res.content

    def get_session(self):
        session = requests.Session()
        session.mount("https://", HTTPAdapter(
            max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        ))
        session.headers.update(config.headers)
        session.cookies.update(self.cookies or {})
        return session
