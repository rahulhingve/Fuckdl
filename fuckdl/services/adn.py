import base64
import binascii
import json
import os
import random
import re
import tempfile
import time
from hashlib import md5, sha1
from pathlib import Path
from typing import Union

import click
import m3u8
import requests
from langcodes import Language

from fuckdl.objects import Title, Tracks, MenuTrack, TextTrack
from fuckdl.services.BaseService import BaseService


def pkcs1pad(data, length):
    """PKCS#1 v1.5 padding for RSA encryption."""
    if len(data) > length - 11:
        raise ValueError("Message too long for RSA key size")
    padding_length = length - len(data) - 3
    padding = bytes(random.randint(1, 255) for _ in range(padding_length))
    return [0, 2] + list(padding) + [0] + data


def bytes_to_long(b):
    return int.from_bytes(b, byteorder='big')


def long_to_bytes(n, blocksize=0):
    byte_length = max((n.bit_length() + 7) // 8, blocksize)
    return n.to_bytes(byte_length, byteorder='big')


def aes_cbc_decrypt(data, key, iv):
    """AES-CBC decryption with PKCS7 unpadding."""
    from Crypto.Cipher import AES
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(data)
    # PKCS7 unpad
    pad_len = decrypted[-1]
    if pad_len < 1 or pad_len > 16:
        return decrypted
    if decrypted[-pad_len:] != bytes([pad_len]) * pad_len:
        return decrypted
    return decrypted[:-pad_len]


class ADN(BaseService):
    """
    Service code for ADN - Animation Digital Network (https://animationdigitalnetwork.com).

    \b
    Authorization: Credentials
    Security: HLS (non-DRM / clear key)

    \b
    Notes:
    - ADN uses HLS streaming with AES-128 encryption on some content.
    - Subtitles are encrypted with AES-CBC and need decryption.
    - Geofenced to France and Germany.

    \b
    Examples:
      Series:  poetry run fuckdl dl adn https://animationdigitalnetwork.com/video/911-tokyo-mew-mew-new
      Episode: poetry run fuckdl dl adn https://animationdigitalnetwork.com/video/558-fruits-basket/9841-episode-1-a-ce-soir 

    Original Author: Kiro - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["ADN", "AnimationDigitalNetwork"]
    GEOFENCE = ["fr"]

    TITLE_RE = [
        r"^https?://(?:www\.)?animationdigitalnetwork\.com/(?:(?P<lang>de)/)?video/(?P<show_slug>[^/]+)/(?P<id>\d+)",
        r"^https?://(?:www\.)?animationdigitalnetwork\.com/(?:(?P<lang>de)/)?video/(?P<id>\d+[^/]*?)/?$",
        r"^(?P<id>\d+)$",
    ]

    _RSA_KEY = (
        0x9B42B08905199A5CCE2026274399CA560ECB209EE9878A708B1C0812E1BB8CB5D1FB7441861147C1A1F2F3A0476DD63A9CAC20D3E983613346850AA6CB38F16DC7D720FD7D86FC6E5B3D5BBC72E14CD0BF9E869F2CEA2CCAD648F1DCE38F1FF916CEFB2D339B64AA0264372344BC775E265E8A852F88144AB0BD9AA06C1A4ABB,
        65537,
    )

    @staticmethod
    @click.command(name="ADN", short_help="https://animationdigitalnetwork.com")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Treat as movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return ADN(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        self.parse_title(ctx, title)
        self.movie_mode = movie
        super().__init__(ctx)

        self.lang = "fr"
        self.access_token = None
        self.show_slug = None
        self.show_id = None
        self.video_id = None

        # Parse URL to extract show_slug vs video_id
        if title:
            m = re.search(
                r"animationdigitalnetwork\.com/(?:de/)?video/(?P<show_slug>[^/]+?)(?:/(?P<vid>\d+))?/?$",
                title,
            )
            if m:
                self.show_slug = m.group("show_slug")
                self.video_id = m.group("vid")
                if "/de/" in title:
                    self.lang = "de"
                # Extract numeric show ID from slug (e.g. "911-tokyo-mew-mew-new" -> "911")
                slug_match = re.match(r"^(\d+)", self.show_slug)
                if slug_match:
                    self.show_id = slug_match.group(1)
                else:
                    self.show_id = self.show_slug
            else:
                # Might be just a numeric ID
                self.show_id = title

        self.configure()

    def get_titles(self) -> Union[list, Title]:
        headers = {
            "X-Target-Distribution": self.lang,
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        # If we have a video_id, it's a single episode
        if self.video_id:
            video = self.session.get(
                url=self.config["endpoints"]["video_metadata"].format(video_id=self.video_id),
                headers=headers,
            ).json().get("video", {})

            show = video.get("show", {})
            show_title = show.get("title", "")
            episode_name = video.get("name", "")
            season = video.get("season")
            episode_number = video.get("shortNumber")
            year = (video.get("releaseDate") or "")[:4] or None

            if self.movie_mode:
                return Title(
                    id_=self.video_id,
                    type_=Title.Types.MOVIE,
                    name=show_title or episode_name,
                    year=year,
                    source=self.ALIASES[0],
                    original_lang="ja",
                    service_data=video,
                )

            return Title(
                id_=self.video_id,
                type_=Title.Types.TV,
                name=show_title,
                season=int(season) if season else 1,
                episode=int(episode_number) if episode_number else None,
                episode_name=episode_name,
                year=year,
                source=self.ALIASES[0],
                original_lang="ja",
                service_data=video,
            )

        # Otherwise it's a show URL, get all episodes
        if not self.show_id:
            self.log.exit(" - Could not determine show or episode from URL")

        # Get show info
        show_data = self.session.get(
            url=self.config["endpoints"]["show"].format(show_id=self.show_id),
            headers=headers,
        ).json().get("show", {})

        show_id = str(show_data.get("id", self.show_id))
        show_title = show_data.get("title", "")

        # Get all episodes
        episodes_data = self.session.get(
            url=self.config["endpoints"]["episodes"].format(show_id=show_id),
            headers=headers,
            params={"order": "asc", "limit": "-1"},
        ).json()

        videos = episodes_data.get("videos", [])
        titles = []

        for video in videos:
            vid = str(video.get("id", ""))
            episode_name = video.get("name", "")
            season = video.get("season")
            episode_number = video.get("shortNumber")
            year = (video.get("releaseDate") or "")[:4] or None

            titles.append(
                Title(
                    id_=vid,
                    type_=Title.Types.TV,
                    name=show_title,
                    season=int(season) if season else 1,
                    episode=int(episode_number) if episode_number else None,
                    episode_name=episode_name,
                    year=year,
                    source=self.ALIASES[0],
                    original_lang="ja",
                    service_data=video,
                )
            )

        return titles

    def get_tracks(self, title: Title) -> Tracks:
        headers = {
            "X-Target-Distribution": self.lang,
        }
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        video_id = title.id

        # Step 1: Get player configuration
        player_config = self.session.get(
            url=self.config["endpoints"]["player_config"].format(video_id=video_id),
            headers=headers,
        ).json()

        player = player_config.get("player", {})
        options = player.get("options", {})
        user = options.get("user", {})

        if not user.get("hasAccess"):
            self.log.exit(" - No access to this video. A subscription may be required.")

        # Step 2: Get refresh token
        refresh_token_url = user.get("refreshTokenUrl") or self.config["endpoints"]["refresh_token"]
        self.log.debug(f" + Refresh token URL: {refresh_token_url}")
        self.log.debug(f" + Refresh token: {user.get('refreshToken')}")
        token_response = self.session.post(
            url=refresh_token_url,
            headers={
                "X-Player-Refresh-Token": user["refreshToken"],
                **headers,
            },
            data=b"",
        ).json()
        token = token_response.get("token")
        self.log.debug(f" + Player token: {token}")

        if not token:
            self.log.warning(f" - Token response: {token_response}")
            self.log.exit(" - Failed to get player token")

        # Step 3: Build encrypted authorization
        links_url = options.get("video", {}).get("url") or self.config["endpoints"]["video_link"].format(video_id=video_id)
        self.log.debug(f" + Links URL: {links_url}")

        k = "".join(random.choices("0123456789abcdef", k=16))
        self._k = k  # Store for subtitle decryption
        message = list(json.dumps({"k": k, "t": token}).encode())

        # RSA encrypt with PKCS#1 v1.5 padding (retry up to 3 times)
        # Sometimes authentication fails for no good reason, retry with
        # a different random padding
        links_data = None
        for attempt in range(3):
            padded_message = bytes(pkcs1pad(message, 128))
            n, e = self._RSA_KEY
            encrypted = long_to_bytes(pow(bytes_to_long(padded_message), e, n), 128)
            authorization = base64.b64encode(encrypted).decode()

            try:
                resp = self.session.get(
                    url=links_url,
                    headers={
                        "X-Player-Token": authorization,
                        **headers,
                    },
                    params={
                        "freeWithAds": "true",
                        "adaptive": "false",
                        "withMetadata": "true",
                        "source": "Web",
                    },
                )
                if resp.status_code == 401:
                    self.log.debug(f" - 401 on attempt {attempt + 1}, retrying with new padding...")
                    continue
                if resp.status_code == 403:
                    error = resp.json()
                    if error.get("code") == "player-bad-geolocation-country":
                        self.log.exit(f" - Geo-blocked: {error.get('message')}")
                    self.log.exit(f" - Forbidden: {error.get('message')}")

                links_data = resp.json()
                if "links" in links_data:
                    break
                else:
                    self.log.debug(f" - Unexpected response: {links_data}")
                    continue
            except Exception as ex:
                self.log.warning(f" - Link request failed on attempt {attempt + 1}: {ex}")
                continue

        if not links_data:
            self.log.exit(" - Failed to get video links after retries")

        self.log.debug(f" + Links data keys: {list(links_data.keys()) if links_data else 'None'}")

        links = links_data.get("links", {})
        streaming = links.get("streaming", {})

        self.log.debug(f" + Streaming formats: {list(streaming.keys()) if streaming else 'empty'}")
        if not streaming:
            self.log.debug(f" + Full links_data: {json.dumps(links_data, indent=2)[:2000]}")

        # Step 4: Extract HLS streams
        # ADN provides sd/hd/fhd (single-variant) + auto (adaptive master with all).
        # We use "auto" only to avoid duplicates. Fallback to individual qualities.
        # ADN muxes audio into video segments, so no separate audio tracks.
        tracks = Tracks()

        for format_id, qualities in streaming.items():
            if not isinstance(qualities, dict):
                continue

            # Prefer "auto" (adaptive master m3u8 with all resolutions)
            if "auto" in qualities:
                quality_list = [("auto", qualities["auto"])]
            else:
                quality_list = list(qualities.items())

            for quality, lb_url in quality_list:
                self.log.debug(f" + Loading {format_id}/{quality}: {lb_url}")
                try:
                    lb_data = self.session.get(url=lb_url, headers=headers).json()
                except Exception as ex:
                    self.log.warning(f" - Failed to get load balancer data: {ex}")
                    continue

                m3u8_url = lb_data.get("location")
                if not m3u8_url:
                    continue

                lang_code = "fr" if format_id == "vf" else ("de" if format_id == "vde" else "ja")

                try:
                    master_playlist = m3u8.load(m3u8_url)
                except Exception as ex:
                    self.log.warning(f" - Failed to load m3u8: {ex}")
                    continue

                new_tracks = Tracks.from_m3u8(
                    master=master_playlist,
                    source=self.ALIASES[0],
                )

                for t in new_tracks:
                    t.language = Language.get(lang_code)
                    t.note = format_id

                if not tracks.videos:
                    tracks = new_tracks
                else:
                    for t in new_tracks.videos:
                        tracks.videos.append(t)
                    for t in new_tracks.audios:
                        tracks.audios.append(t)

        # Step 5: Subtitles â€” download, decrypt AES-CBC, parse JSON, write SRT
        sub_url = (links.get("subtitles") or {}).get("all")
        if sub_url:
            decrypted_subs = self._decrypt_subtitles(sub_url, video_id, headers)
            if decrypted_subs:
                for sub_lang, sub_entries in decrypted_subs.items():
                    # Map ADN sub lang codes to standard
                    lang_code = sub_lang
                    if sub_lang == "vostf":
                        lang_code = "fr"
                    elif sub_lang == "vostde":
                        lang_code = "de"

                    # Build SRT content from the subtitle entries
                    srt_content = self._build_srt(sub_entries)

                    # Write to a temp file so the track can reference it
                    tmp_dir = Path(tempfile.gettempdir()) / "adn_subs"
                    tmp_dir.mkdir(parents=True, exist_ok=True)
                    srt_path = tmp_dir / f"{video_id}_{lang_code}.srt"
                    srt_path.write_text(srt_content, encoding="utf-8")

                    tracks.add(
                        TextTrack(
                            id_=md5(f"{video_id}_{lang_code}".encode()).hexdigest()[:6],
                            url=str(srt_path),
                            codec="srt",
                            language=Language.get(lang_code),
                            source=self.ALIASES[0],
                        )
                    )

        return tracks

    def get_chapters(self, title: Title) -> list:
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        # ADN uses HLS with AES-128, not Widevine/PlayReady DRM
        return None

    # -- Service-specific functions --

    def configure(self):
        self.log.info(" + Starting ADN...")

        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://animationdigitalnetwork.com",
            "Referer": "https://animationdigitalnetwork.com/",
            "X-Target-Distribution": self.lang,
        })

        if self.credentials:
            self._login()

    def _login(self):
        cache_path = Path(
            self.get_cache(
                "tokens_{hash}.json".format(
                    hash=sha1(f"{self.credentials.username}".encode()).hexdigest()
                )
            )
        )

        if cache_path.is_file():
            tokens = json.loads(cache_path.read_text(encoding="utf-8"))
            self.access_token = tokens.get("accessToken")
            self.log.info(" + Using cached ADN tokens")
        else:
            cache_path.parent.mkdir(exist_ok=True, parents=True)
            self.log.info(" + Logging in to ADN...")

            try:
                res = self.session.post(
                    url=self.config["endpoints"]["login"],
                    data=json.dumps({
                        "username": self.credentials.username,
                        "password": self.credentials.password,
                        "rememberMe": False,
                        "source": "Web",
                    }),
                    headers={
                        "Content-Type": "application/json",
                    },
                ).json()

                self.access_token = res.get("accessToken")
                if self.access_token:
                    cache_path.write_text(json.dumps(res), encoding="utf-8")
                    self.log.info(" + Login successful")
                else:
                    self.log.warning(" - Login failed, continuing without auth")
            except Exception as ex:
                self.log.warning(f" - Login error: {ex}")

        if self.access_token:
            self.session.headers["Authorization"] = f"Bearer {self.access_token}"

    def _decrypt_subtitles(self, sub_url, video_id, headers):
        """
        Download and decrypt ADN subtitles.
        ADN encrypts subtitles with AES-CBC:
        - The first 24 bytes (base64) of the payload are the IV
        - The rest is the AES-CBC encrypted data (base64)
        - Key = self._k (16 hex chars) + '7fac1178830cfe0c' (hardcoded suffix from ADN player JS)
        Returns a dict of {lang: [subtitle_entries]} or None on failure.
        """
        try:
            # Step 1: Get the subtitle location URL
            enc_subtitles_raw = self.session.get(url=sub_url, headers=headers).text
            sub_location_data = json.loads(enc_subtitles_raw) if enc_subtitles_raw.strip().startswith("{") else {}
            sub_location = sub_location_data.get("location")

            # Step 2: Download the actual encrypted subtitle data
            if sub_location:
                enc_subtitles = self.session.get(
                    url=sub_location,
                    headers={"Origin": "https://animationdigitalnetwork.com"},
                ).text
            else:
                enc_subtitles = enc_subtitles_raw

            if not enc_subtitles or len(enc_subtitles) < 24:
                self.log.warning(" - Subtitle data too short or empty")
                return None

            # Step 3: Decrypt
            # IV = first 24 chars base64-decoded
            # Ciphertext = remaining chars base64-decoded
            # Key = self._k + hardcoded suffix, hex-decoded to bytes
            iv = base64.b64decode(enc_subtitles[:24])
            ciphertext = base64.b64decode(enc_subtitles[24:])
            key = binascii.unhexlify(self._k + "7fac1178830cfe0c")

            decrypted = aes_cbc_decrypt(ciphertext, key, iv)
            subtitles_json = json.loads(decrypted.decode("utf-8"))

            self.log.info(f" + Subtitles decrypted: {list(subtitles_json.keys())}")
            return subtitles_json

        except Exception as ex:
            self.log.warning(f" - Failed to decrypt subtitles: {ex}")
            return None

    @staticmethod
    def _build_srt(entries):
        """Convert a list of ADN subtitle entries to SRT format."""
        srt_lines = []
        for i, entry in enumerate(entries, 1):
            start = entry.get("startTime")
            end = entry.get("endTime")
            text = entry.get("text", "")
            if start is None or end is None:
                continue
            srt_lines.append(str(i))
            srt_lines.append(f"{ADN._format_srt_time(start)} --> {ADN._format_srt_time(end)}")
            # Clean HTML tags (<i>, </i>) to SRT italic tags
            clean_text = text.replace("<i>", "<i>").replace("</i>", "</i>")
            srt_lines.append(clean_text)
            srt_lines.append("")
        return "\n".join(srt_lines)

    @staticmethod
    def _format_srt_time(seconds):
        """Convert seconds (float) to SRT timecode HH:MM:SS,mmm."""
        ms = int((seconds % 1) * 1000)
        s = int(seconds)
        h = s // 3600
        m = (s % 3600) // 60
        s = s % 60
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"
