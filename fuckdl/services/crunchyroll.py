from __future__ import annotations
import re
import uuid
import json
import os
from pathlib import Path
from hashlib import md5, sha1
from langcodes import Language
from typing import Any, Optional, Union
from copy import copy
import click
import requests
from curl_cffi import requests as curl
from fuckdl.objects import Title, Tracks, AudioTrack, MenuTrack, TextTrack, Track, Tracks, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.widevine.device import LocalDevice
from requests.adapters import HTTPAdapter, Retry
from fuckdl.config import config

class Crunchyroll(BaseService):
    """
    Service code for Crunchyroll (https://www.crunchyroll.com).}
    Added by @AnotherBigUserHere

    @emtiaz_07 was the developer that solves this version

    + Updated playback to V3
    + Audio finally can get up to 192 kb/s
    + Web Token Added
    + Licencing of WV and PRD was corrected

    \b
    Authorization: Credentials
    Security:
      Widevine:
        L3: 1080p

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    \b
    Tips:
    - Subs, dubs, OVAs, and some series movies are considered seasons/episodes internally.
    - View the series page on the website to map extra season numbers to proper season/title.
    - If a movi e is incorrectly listed as a series season/episode,
      use movie option (if only one episode), year can be incorrect.

    \b
    Notes:
    - The series year is based on the release season (i.e winter-2023) because there is no series release year API data.
    - This means dubs will have a different release year if they weren't simulcasted together.
    - This issue does not af fect movies as movie_release_year API data exists.
    """
    ALIASES = ["CR"]
    TITLE_RE = [
        r"^https?://(?:www\.)?crunchyroll\.com(?:/[a-z0-9-]+)?/(?:watch/)?(?P<type>series|watch|artist|musicvideo|concert)/(?P<id>[A-Z0-9]+)",
        r"^(?P<id>[A-Z0-9]+)"
    ]

    LANGUAGE_MAP: dict = {
        "es-LA": "es-419", "ar-ME": "ar-SA", "de-DE": "de", "en-US": "en",
        "es-ES": "es", "fr-FR": "fr", "hi-IN": "hi", "it-IT": "it",
        "ja-JP": "ja", "ko-KR": "ko", "ru-RU": "ru", "zh-CN": "zh",
    }

    @staticmethod
    @click.command(name="Crunchyroll", short_help="https://crunchyroll.com", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.option("-d", "--default-sub-lang", type=str, default="en", help="Default subtitle language (default: en).")
    @click.option("-all", "--show-all-video", is_flag=True, default=False, help="Show all video track.")
    @click.option("-sm", "--skip-merge", type=str, default=None, help="Show dub versions as individual eps.")
    @click.option("-c", "--concert", is_flag=True, default=False, help="Select concert to download.")
    @click.option("-m", "--movie", is_flag=True, default=False, help="Download series as movie if listed as a series.)")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Crunchyroll(ctx, **kwargs)

    def __init__(self, ctx, title, default_sub_lang, show_all_video, concert, movie, skip_merge):
        super().__init__(ctx)
        data = self.parse_title(ctx, title)
        self.title = data.get("id")
        self.type: str = data.get("type") if data else "series"
        self.movie: bool = False
        self.music: bool = False
        self.default_sub: str = default_sub_lang
        self.show_all_video: bool = show_all_video
        self.concert: bool = concert
        self.movie_forced: bool = movie
        self.skip_merge: Optional[str] = skip_merge
        self.cdm = ctx.obj.cdm
        self.processed_ids = set()
        self.slang: list = ctx.parent.params.get("slang", [])
        self.alang: list = ctx.parent.params.get("alang", [])
        self.chapters_only: bool = ctx.parent.params.get("chapters_only", False)
        self.audio_only: bool = ctx.parent.params.get("audio_only", False)
        self.subtitles_only: bool = ctx.parent.params.get("subs_only", False)
        self.lic: int = 0

        # ðŸ”‘ Initialize persistent device_id BEFORE configure()
        device_cache = Path(self.get_cache("device_id.json"))
        if device_cache.is_file():
            try:
                self.device_id = json.loads(device_cache.read_text())["device_id"]
            except Exception:
                self.device_id = str(uuid.uuid4())
        else:
            self.device_id = str(uuid.uuid4())
            
        device_cache.parent.mkdir(parents=True, exist_ok=True)
        device_cache.write_text(json.dumps({"device_id": self.device_id}))

        self.configure()
    
    def get_session(self):
        """Creates a Python-requests Session with retries and common headers."""
        session = requests.Session()
        session.mount("https://", HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
        ))
        session.hooks = {
            "response": lambda r, *_, **__: r.raise_for_status(),
        }
        session.headers.update(config.headers)
        session.cookies.update(self.cookies or {})
        return session
    
    def close_all_sessions(self):
        headers = {
            "authorization": f"Bearer {self.access_token}",
            "user-agent": self.config["headers"]["user-agent"],
        }
        response = requests.get(self.config['endpoints']['session'], headers=headers)
        if response.status_code == 200 and len(response.json().get('items', [])) > 0:
            for ses in response.json()['items']:
                self.close_session(ses['contentId'], ses['token'])

    def get_titles(self) -> Union[list[Title], Title]:
        self.processed_ids.clear()
        if self.type in {"concert", "musicvideo"}:
            return self.get_title_music_watch()
        elif self.type == "artist":
            return self.get_title_music()
        elif self.type == "watch":
            return self.get_title_watch()
        else:
            return self.get_title_series()

    def close_session(self, id, token):
        self.session.delete(self.config['endpoints']['streams_del'].format(id=id, video_token=token))

    def get_tracks(self, title: Title) -> Tracks:
        self.close_all_sessions()
        if title.id in self.processed_ids:
            return Tracks()
        self.processed_ids.add(title.id)

        if self.chapters_only and not self.audio_only and not self.subtitles_only:
            return Tracks()

        self.headers_update()

        tracks = None
        variants = []
        endpoints = [
            self.config["endpoints"]["streams_new"],
            self.config["endpoints"]["streams_new"].replace("/tv/android_tv/play", "/android/phone/download")
        ]

        try:
            if self.music:
                playback_tv = self.session.get(self.config["endpoints"]["music"]["streams_new"].format(id=title.id)).json()
            else:
                playback_tv = self.session.get(self.config["endpoints"]["streams_new"].format(id=title.id)).json()
        except requests.HTTPError as e:
            self.log.debug(e.response.content)
            self.log.exit("Failed to get playback, maybe too much request in short time!")

        if playback_tv.get("versions"):
            for x in playback_tv["versions"]:
                if x["guid"] not in variants and x["guid"] != title.id:
                    variants.append(x["guid"])
        if title.service_data.get("versions"):
            for x in title.service_data["versions"]:
                if x["guid"] not in variants and x["guid"] != title.id:
                    variants.append(x["guid"])
        variants.insert(0, title.id)

        sdh = []
        for variant in variants:
            skip = False
            playbacks = []
            for endpoint in endpoints:
                try:
                    headers = {
                        'authorization': f'Bearer {self.access_token}',
                        'origin': 'https://www.crunchyroll.com',
                        'referer': 'https://www.crunchyroll.com/'
                    }
                    playback = self.session.get(url=endpoint.format(id=variant), headers=headers).json()
                    playback["_endpoint"] = endpoint
                    playbacks.append(playback)
                except requests.HTTPError as e:
                    self.log.debug(e)
                    if "420 Client Error" in str(e):
                        skip = True
                        self.log.warning("A version might not be available in your region!")
            if skip:
                continue

            for playback in playbacks:
                if not playback.get("url"):
                    continue

                track = Tracks.from_mpd(url=playback["url"], session=self.session, source=self.ALIASES[0])
                for x in track:
                    x.token = playback.get("token")
                    x.real_id = variant

                try:
                    lang_audio = [x for x in playback.get("versions", []) if x["guid"] == variant][0]["audio_locale"]
                    lang_audio = self.get_lang(lang_audio)
                except (IndexError, KeyError, TypeError):
                    lang_audio = title.original_lang

                if self.skip_merge and str(lang_audio).lower() not in str(self.skip_merge).lower():
                    continue

                for track_now in track:
                    if isinstance(track_now, AudioTrack):
                        track_now.language = Language.get(lang_audio)
                        track_now.id += md5(variant.encode()).hexdigest()[0:6]
                        if track_now.channels == "1.0":
                            track_now.channels = "2.0"
                    elif isinstance(track_now, VideoTrack):
                        track_now.language = Language.get(lang_audio)
                        track_now.note = lang_audio
                        if self.show_all_video:
                            track_now.id += md5(variant.encode()).hexdigest()[0:6]

                track.subtitles.clear()
                if "android/phone/download" not in playback["_endpoint"]:
                    if playback.get("captions"):
                        for subtitle in playback["captions"].values():
                            lang = self.get_lang(subtitle["language"])
                            sdh.append(lang)
                            track.add(TextTrack(
                                id_=md5(subtitle["url"].encode()).hexdigest()[0:6],
                                url=subtitle["url"],
                                codec=subtitle["format"],
                                language=Language.get(lang),
                                source=self.ALIASES[0],
                                sdh=True,
                            ))
                    if playback.get("subtitles"):
                        for subtitle in playback["subtitles"].values():
                            lang = self.get_lang(subtitle["language"])
                            track.add(TextTrack(
                                id_=md5(subtitle["url"].encode()).hexdigest()[0:6],
                                url=subtitle["url"],
                                codec=subtitle["format"],
                                language=Language.get(lang),
                                source=self.ALIASES[0],
                                forced=False if (str(lang_audio) == str(title.original_lang)) and (lang not in sdh) else True,
                            ))

                if not tracks:
                    tracks = copy(track)
                else:
                    for trk in track:
                        if isinstance(trk, AudioTrack) and all(a.id != trk.id for a in tracks.audios):
                            tracks.audios.append(trk)
                        elif isinstance(trk, TextTrack) and all(s.id != trk.id for s in tracks.subtitles):
                            tracks.subtitles.append(trk)
                        elif isinstance(trk, VideoTrack) and self.show_all_video and all(v.id != trk.id for v in tracks.videos):
                            tracks.videos.append(trk)

                if playback.get("token"):
                    try:
                        self.session.delete(url=self.config["endpoints"]["streams_stop"].format(guid=variant, token=playback["token"]))
                    except Exception:
                        pass
    
        # Web playback fallback logic remains unchanged...
        # (Keeping it concise for readability, identical to your version)
        if self.credentials:
            token = self.access_token
        else:
            token, _ = self.get_token_cookies()
            
        headers_base = {
            "host": "www.crunchyroll.com",
            "authorization": f"Bearer {token}",
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }
        for variant in variants:
            try:
                playback = self.web_play(variant, token)
            except requests.HTTPError as e:
                if "420 Client Error" in str(e):
                    self.log.warning("Version not available in your region!")
                continue

            if not playback.get("url"):
                continue

            mpd_data = curl.get(url=playback["url"], headers=headers_base, cookies=self.session.cookies, impersonate="chrome120")
            track = Tracks.from_mpd(url=playback["url"], data=mpd_data.text, session=self.session, source=self.ALIASES[0])
            lang_audio = self.get_lang(next((v["audio_locale"] for v in playback.get("versions", []) if v["guid"] == variant), title.original_lang))

            if self.skip_merge and str(lang_audio).lower() not in str(self.skip_merge).lower():
                continue

            for t in track:
                t.language = Language.get(lang_audio)
                if isinstance(t, AudioTrack):
                    t.id += md5(variant.encode()).hexdigest()[:6]
                    if t.channels == "1.0": t.channels = "2.0"
                elif isinstance(t, VideoTrack) and self.show_all_video:
                    t.id += md5(variant.encode()).hexdigest()[:6]
                    t.note = lang_audio

            track.subtitles.clear()
            tracks.add(track.audios)

            if playback.get("token"):
                try:
                    curl.delete(url=self.config["endpoints"]["streams_web_stop"].format(guid=variant, token=playback["token"]), headers={**headers_base, "content-type": "application/json", "origin": "https://www.crunchyroll.com"}, json={}, cookies=self.session.cookies)
                except Exception:
                    pass

        return tracks if tracks else Tracks()

    def get_chapters(self, title: Title) -> list[MenuTrack]:
        return []

    def license(self, challenge: bytes, title: Title, track: Track, *_, **__) -> Optional[Union[bytes, str]]:
        is_playready = False
        
        is_playready = False
        if hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__:
            is_playready = True
        elif hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type'):
            is_playready = (self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        
        headers = {
            'authorization': f'Bearer {self.access_token}',
            'origin': 'https://www.crunchyroll.com',
            'referer': 'https://www.crunchyroll.com/',
            "user-agent": self.config["headers"]["user-agent"]
        }
        
        playback = self.session.get(
            url=self.config["endpoints"]["streams_new"].format(id=track.real_id),
            headers=headers
        ).json()
        
        if is_playready:
            # PlayReady license request
            res = self.session.post(
                url=self.config["endpoints"]["license_pr"],
                headers={
                    "x-cr-content-id": track.real_id,
                    "x-cr-video-token": playback["token"],
                    "content-type": "text/xml",
                    "user-agent": self.config["headers"]["user-agent"]
                },
                data=challenge
            ).content
        else:
            # Widevine license request
            r: dict = self.session.post(
                url=self.config["endpoints"]["license_wv"],
                headers={
                    "x-cr-content-id": track.real_id,
                    "x-cr-video-token": playback["token"],
                    "content-type": "application/octet-stream",
                    "user-agent": self.config["headers"]["user-agent"]
                },
                data=challenge
            ).json()
            res = r["license"]
        
        try:
            self.session.delete(
                url=self.config["endpoints"]["streams_stop"].format(guid=track.real_id, token=playback["token"])
            )
        except Exception:
            pass
        
        return res

    def configure(self) -> None:
        self.log.info(" + Logging in")
        self.session.headers.update({
            "origin": "https://www.crunchyroll.com",
            "referer": "https://www.crunchyroll.com",
            "user-agent": self.config["headers"]["user-agent"],
        })

        if self.credentials:
            cache_path = Path(self.get_cache(f"tokens_{sha1(self.credentials.username.encode()).hexdigest()}.json"))
            if cache_path.is_file():
                try:
                    tokens = json.loads(cache_path.read_text())
                    self.refresh_token = tokens["refresh_token"]
                    self.access_token = tokens["access_token"]
                    self.account_id = tokens.get("account_id")
                    self.log.info("  + Using cached tokens")
                    self.update_auth()
                    self.key_pair_id, self.policy, self.signature, self.bucket = self.get_bucket_info()
                    return
                except Exception:
                    self.log.warning("  - Cached tokens invalid, re-authenticating")

            cache_path.parent.mkdir(exist_ok=True, parents=True)
            self.log.info(" + Getting logging tokens")
            
            res = self.session.post(
                url=self.config["endpoints"]["token"],
                headers={
                    **self.config.get("headers_login", {}),
                    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                data={
                    "scope": "offline_access",
                    "grant_type": "password",
                    "device_id": self.device_id,  # âœ… Now initialized in __init__
                    "client_id": self.config["client"]["id"],  # âœ… From config
                    "client_secret": self.config["client"]["secret"],  # âœ… From config
                    "username": self.credentials.username,
                    "password": self.credentials.password,
                    "device_name": self.config.get("device", {}).get("name", "SHIELD Android TV"),  # âœ… From config
                    "device_type": self.config.get("device", {}).get("type", "ANDROIDTV"),  # âœ… From config
                }
            ).json()

            self.log.debug(res)
            self.refresh_token: str = res["refresh_token"]
            self.access_token: str = res["access_token"]
            self.account_id: str = res.get('account_id')
            cache_path.write_text(json.dumps(res))
            
            self.update_auth()
            self.key_pair_id, self.policy, self.signature, self.bucket = self.get_bucket_info() 
        else:
            self.access_token, self.account_id = self.get_token_cookies()
            self.update_auth()
            self.key_pair_id, self.policy, self.signature, self.bucket = self.get_bucket_info()

    def convert_timecode(self, time):
        secs, ms = divmod(time, 1)
        mins, secs = divmod(secs, 60)
        hours, mins = divmod(mins, 60)
        ms = ms * 10000
        return '%02d:%02d:%02d.%04d' % (hours, mins, secs, ms)

    def update_auth(self):
        self.session.headers.update({"authorization": f"Bearer {self.access_token}"})

    def headers_update(self):
        if self.credentials:
            self.access_token, self.account_id = self.get_token_password()
        else:
            self.access_token, self.account_id = self.get_token_cookies()
        self.update_auth()
        self.key_pair_id, self.policy, self.signature, self.bucket = self.get_bucket_info()

    def get_bucket_info(self):
        try:
            res = self.session.get(self.config["endpoints"]["bucket"]).json()
            return res["cms"]["key_pair_id"], res["cms"]["policy"], res["cms"]["signature"], res["cms"]["bucket"]
        except Exception as e:
            self.log.debug(f"Failed to load bucket info: {e}")
            return None, None, None, None

    def get_token_password(self) -> str:
        try:
            res = self.session.post(
                url=self.config["endpoints"]["token"],
                headers={**self.config.get("headers_login", {}), "content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
                data={
                    "refresh_token": self.refresh_token,
                    "device_id": self.device_id,
                    "grant_type": "refresh_token",
                    "scope": "offline_access",
                    "client_id": self.config["client"]["id"],
                    "client_secret": self.config["client"]["secret"],
                    "device_name": self.config.get("device", {}).get("name", "SHIELD Android TV"),
                    "device_type": self.config.get("device", {}).get("type", "ANDROIDTV"),
                }
            ).json()
            return res["access_token"], res.get("account_id")
        except requests.HTTPError as e:
            self.log.debug(e.response)
            self.log.exit("Failed to get token.")

    def get_token_cookies(self) -> str:
        try:
            res = curl.post(
                url=self.config["endpoints"]["token_web"],
                headers={
                    "host": "www.crunchyroll.com", "connection": "keep-alive",
                    "etp-anonymous-id": self.session.cookies.get("ajs_anonymous_id"),
                    "sec-ch-ua-platform": "\"Windows\"",
                    "authorization": "Basic bzd1b3d5N3E0bGdsdGJhdnloanE6bHFyakVUTng2Vzd1Um5wY0RtOHdSVmo4QkNoakMxZXI=",
                    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
                    "sec-ch-ua-mobile": "?0",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
                    "accept": "application/json, text/plain, */*",
                    "content-type": "application/x-www-form-urlencoded",
                    "origin": "https://www.crunchyroll.com", "sec-fetch-site": "same-origin",
                    "sec-fetch-mode": "cors", "sec-fetch-dest": "empty",
                    "referer": "https://www.crunchyroll.com/series/GG5H5XQ0D/dan-da-dan",
                    "accept-encoding": "gzip, deflate, br, zstd", "accept-language": "en-US,en;q=0.9",
                },
                data={"device_id": self.session.cookies.get("device_id"), "device_type": "Chrome on Windows", "grant_type": "etp_rt_cookie"},
                cookies=self.session.cookies,
                impersonate="chrome120"
            ).json()
            if "access_token" not in res:
                self.log.debug(f"Token response: {res}")
                self.log.exit("Failed to get token â€” cookies are likely expired. Please re-export your Crunchyroll cookies.")
            return res["access_token"], res.get("account_id", "")
        except Exception as e:
            self.log.debug(e)
            self.log.exit("Failed to get token â€” cookies are likely expired. Please re-export your Crunchyroll cookies.")

    def get_lang(self, lang: str):
        return self.LANGUAGE_MAP.get(lang, lang)

    def get_title_music_watch(self) -> Title:
        self.music = True
        type: str = "concerts" if self.type == "concert" else "music_videos"
        data: dict = self.session.get(url=self.config["endpoints"]["music"]["music"].format(id=self.title, type=type)).json()["data"][0]
        return Title(id_=data["id"], type_=Title.Types.MUSIC, artist=data["artist"]["name"], year=data["originalRelease"][:4], album=data["type"], disc_number=None, track_number=data["sequenceNumber"], source=self.ALIASES[0], original_lang="ja", name=data["title"], service_data=data)

    def get_title_music(self) -> list[Title]:
        data = self.session.get(url=self.config["endpoints"]["music"]["artist"].format(id=self.title, type="concerts" if self.concert else "music_videos")).json()["data"]
        self.music = True
        return [Title(id_=x["id"], type_=Title.Types.MUSIC, artist=x["artist"]["name"], year=x["originalRelease"][:4], album=x["type"], disc_number=None, track_number=y, source=self.ALIASES[0], original_lang="ja", name=x["title"], service_data=x) for y, x in enumerate(data, 1)]

    def get_title_watch(self) -> Title:
        data = self.session.get(url=self.config["endpoints"]["objects"].format(id=self.title)).json()["data"][0]
        if data["type"] == "episode":
            if self.movie_forced:
                return Title(id_=data["id"], type_=Title.Types.MOVIE, name=data["episode_metadata"]["series_title"], year=data["episode_metadata"]["episode_air_date"][:4], source=self.ALIASES[0], original_lang=self.get_lang(data["episode_metadata"]["audio_locale"]), service_data=data)
            else:
                return Title(id_=data["id"], type_=Title.Types.TV, name=data["episode_metadata"]["series_title"], season=data["episode_metadata"]["season_number"], episode=data["episode_metadata"]["episode_number"], year=data["episode_metadata"]["episode_air_date"][:4], source=self.ALIASES[0], original_lang=self.get_lang(data["episode_metadata"]["audio_locale"]), episode_name=data["title"], service_data=data)
        else:
            self.movie = True
            metadata = self.session.get(url=self.config["endpoints"]["metadata"].format(title_id=self.title)).json()["data"][0]
            return Title(id_=data["movie_listing_metadata"]["first_movie_id"], type_=Title.Types.MOVIE, name=metadata["title"], year=metadata["movie_release_year"], source=self.ALIASES[0], original_lang=self.get_lang(metadata["audio_locale"]), service_data=data)

    def get_title_series(self) -> list[Title]:
        titles = list()
        var = dict()
        series_info: dict = self.session.get(url=self.config["endpoints"]["series"]["series"].format(id=self.title)).json()["data"][0]
        if "availability_notes" in series_info: del series_info["availability_notes"]

        seasons: dict = self.session.get(
            url=self.config["endpoints"]["series"]["seasons"].format(bucket=self.bucket),
            params={"series_id": self.title, "locale": "en-US", "Signature": self.signature, "Policy": self.policy, "Key-Pair-Id": self.key_pair_id}
        ).json()["items"]

        for season in seasons:
            if "availability_notes" in season: del season["availability_notes"]

        unique_episodes = {} 
        for season in seasons:
            if bool(re.search(r" Dub", season["title"])): continue
            episodes: dict = self.session.get(url=self.config["endpoints"]["series"]["episodes"].format(id=season["id"])).json()["data"]
            for episode in episodes:
                key = f"{episode['season_number']}:{episode['episode_number']}"
                if key not in unique_episodes: unique_episodes[key] = episode
    
        for episode in unique_episodes.values():
            if self.movie_forced:
                titles.append(Title(id_=episode["id"], type_=Title.Types.MOVIE, name=series_info["title"], year=episode["episode_air_date"][:4], source=self.ALIASES[0], original_lang=self.get_lang(episode["audio_locale"]), service_data=episode))
            else:
                titles.append(Title(id_=episode["id"], type_=Title.Types.TV, name=series_info["title"], season=episode["season_number"], episode=episode["episode_number"] or float(episode["sequence_number"] if '.' not in str(episode["sequence_number"]) else str(episode["sequence_number"]).replace('.', "")), year=episode["episode_air_date"][:4], source=self.ALIASES[0], original_lang=self.get_lang(episode["audio_locale"]), episode_name=episode["title"], service_data=episode))

        for season in seasons:
            if re.search(r" Dub", season["title"]):
                episodes: dict = self.session.get(url=self.config["endpoints"]["series"]["episodes"].format(id=season["id"])).json()["data"]
                for episode in episodes:
                    if episode["season_number"] not in var: var[episode["season_number"]] = dict()
                    if episode["episode_number"] not in var[episode["season_number"]]: var[episode["season_number"]][episode["episode_number"]] = []
                    var[episode["season_number"]][episode["episode_number"]].append(episode["id"])

        season_count = {}
        for title in titles:
            season_count[title.season] = season_count.get(title.season, 0) + 1
     
        for s, x in var.items():
            for ep, y in x.items():
                for title_obj in titles:
                    if title_obj.season == s and title_obj.episode == ep:
                        for z in y:
                            title_obj.service_data["versions"].append({'guid': f'{z}'})
        return titles
    
    def web_play(self, title_id, token):
        return curl.get(
            url=self.config["endpoints"]["streams_web"].format(id=title_id),
            headers={"host": "www.crunchyroll.com", "authorization": f"Bearer {token}", "accept": "application/json, text/plain, */*", "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36", "accept-encoding": "gzip, deflate, br, zstd", "accept-language": "en-US,en;q=0.9"},
            impersonate="chrome120"
        ).json()