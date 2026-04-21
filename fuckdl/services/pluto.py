from __future__ import annotations

import re
import uuid
from collections.abc import Generator
from http.cookiejar import CookieJar
from typing import Any, Optional
import m3u8
import click

from fuckdl.utils.widevine.device import LocalDevice
from fuckdl.services.BaseService import BaseService
from fuckdl.objects import AudioTrack, Track, Tracks, VideoTrack, Title

class PLUTO(BaseService):
    """
    \b
    Service code for Pluto TV on demand streaming service (https://pluto.tv/)
    Credit to @wks_uwu for providing an alternative API, making the codebase much cleaner

    \b
    Version: 1.0.1
    Author: stabbedbybrick
    Authorization: None
    Robustness:
      Widevine:
        L3: 1080p, AAC2.0

    \b
    Tips:
        - Input can be complete title URL or just the path:
           SERIES: /series/65ce4e5003fa740013793127/details
           EPISODE: /series/65ce4e5003fa740013793127/season/1/episode/662c2af0a9f2d200131ba731
           MOVIE: /movies/635c1e430888bc001ad01a9b/details
        - Use --lang LANG_RANGE option to request non-English tracks
        - Use --hls to request HLS instead of DASH:
           devine dl pluto URL --hls

    \b
    Notes:
        - Both DASH(widevine) and HLS(AES) are looked for in the API.
        - DASH is prioritized over HLS since the latter doesn't have 1080p. If DASH has audio/subtitle issues,
          you can try using HLS with the --hls flag.
        - Pluto use transport streams for HLS, meaning the video and audio are a part of the same stream
          As a result, only videos are listed as tracks. But the audio will be included as well.
        - With the variations in manifests, and the inconsistency in the API, the language is set as "en" by default
          for all tracks, no matter what region you're in.
          You can manually set the language in the get_titles() function if you want to change it.

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    """

    ALIASES = ["PlutoTV", "plu", "plutotv"]
    TITLE_RE = (
        r"^"
        r"(?:https?://(?:www\.)?pluto\.tv(?:/[a-z]{2})?)?"
        r"(?:https?://(?:www\.)?pluto\.tv(?:/latam)?)?"
        r"(?:/details)?"
        r"(?:/on-demand)?"
        r"/(?P<type>movies|series)"
        r"/(?P<id>[a-z0-9-]+)"
        r"(?:(?:/season/(\d+)/episode/(?P<episode>[a-z0-9-]+)))?"
    )

    @staticmethod
    @click.command(name="PLUTO", short_help="https://pluto.tv/", help=__doc__)
    @click.option("--hls", is_flag=True, help="Request HLS instead of DASH")
    @click.argument("title", type=str)
    @click.pass_context
    def cli(ctx, **kwargs):
        return PLUTO(ctx, **kwargs)

    def __init__(self, ctx, title, hls=False):
        super().__init__(ctx)
        self.title = title
        self.force_hls = hls
        cdm = ctx.obj.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.configure()

    def configure(self):

        self.session.params = {
            "appName": "web",
            "appVersion": "na",
            "clientID": str(uuid.uuid1()),
            "deviceDNT": 0,
            "deviceId": "unknown",
            "clientModelNumber": "na",
            "serverSideAds": "false",
            "deviceMake": "unknown",
            "deviceModel": "web",
            "deviceType": "web",
            "deviceVersion": "unknown",
            "sid": str(uuid.uuid1()),
            "drmCapabilities": "widevine:L3",
        }

        info = self.session.get(self.config["endpoints"]["auth"]).json()
        self.token = info["sessionToken"]
        self.region = info["session"].get("activeRegion", "").lower()
        self.lang = self.region if self.region != "US" and self.region != "UK" else "EN"


    def get_titles(self):
        try:
            kind, content_id, episode_id = (
                re.match(self.TITLE_RE, self.title).group(i) for i in ("type", "id", "episode")
            )
        except Exception:
            raise ValueError("Could not parse ID from title - is the URL correct?")

        if kind == "series" and episode_id:
            r = self.session.get(self.config["endpoints"]["series"].format(season_id=content_id))
            if not r.ok:
                raise ConnectionError(f"{r.json().get('message')}")

            data = r.json()           
            return [Title(
                        id_=episode.get("_id"),
                        type_=Title.Types.TV,
                        name=data.get("name"),
                        season=int(episode.get("season")),
                        episode=int(episode.get("number")),
                        episode_name=episode.get("name"),
                        year=None,
                        source=self.ALIASES[0],
                        original_lang=self.lang,  # TODO: language detection
                        service_data=episode,
                    )
                    for series in data["seasons"]
                    for episode in series["episodes"]
                    if episode.get("_id") == episode_id
                ]

        elif kind == "series":
            r = self.session.get(self.config["endpoints"]["series"].format(season_id=content_id))
            if not r.ok:
                raise ConnectionError(f"{r.json().get('message')}")

            data = r.json()           
            return [Title(
                        id_=episode.get("_id"),
                        type_=Title.Types.TV,
                        name=data.get("name"),
                        season=int(episode.get("season")),
                        episode=int(episode.get("number")),
                        episode_name=episode.get("name"),
                        year=self.year(episode),
                        source=self.ALIASES[0],
                        original_lang=self.lang,  # TODO: language detection
                        service_data=episode,
                    )
                    for series in data["seasons"]
                    for episode in series["episodes"]
                ]

        elif kind == "movies":
            url = self.config["endpoints"]["movie"].format(video_id=content_id)
            r = self.session.get(url, headers={"Authorization": f"Bearer {self.token}"})
            if not r.ok:
                raise ConnectionError(f"{r.json().get('message')}")

            data = r.json()            
            return [Title(
                        id_=movie.get("_id"),
                        type_=Title.Types.MOVIE,
                        name=movie.get("name"),
                        year=self.year(movie),
                        original_lang=self.lang,  # TODO: language detection
                        source=self.ALIASES[0],
                        service_data=movie,
                    )
                    for movie in data
                ]

    def get_tracks(self, title):

        url = self.config["endpoints"]["episodes"].format(episode_id=title.id)
        episode = self.session.get(url).json()

        sources = next((item.get("sources") for item in episode if not self.bumpers(item.get("name", ""))), None)

        if not sources:
            raise ValueError("Unable to find manifest for this title")

        hls = next((x.get("file") for x in sources if x.get("type").lower() == "hls"), None)
        dash = next((x.get("file") for x in sources if x.get("type").lower() == "dash"), None)

        if dash and not self.force_hls:            
            self.license_url = self.config["endpoints"]["license_pr"] if self.playready else self.config["endpoints"]["license_wv"]
            manifest = dash.replace("https://siloh.pluto.tv", "http://silo-hybrik.pluto.tv.s3.amazonaws.com")
            tracks = Tracks.from_mpd(
                url=manifest,
                session=self.session,
                source=self.ALIASES[0]
            )

            for track in tracks.audios:
                role = track.extra[1].find("Role")
                if role is not None and role.get("value") == "description":
                    track.descriptive = True

        else:      

            self.license_url = None
            m3u8_url = hls.replace("https://siloh.pluto.tv", "http://silo-hybrik.pluto.tv.s3.amazonaws.com")
            manifest = self.clean_manifest(self.session.get(m3u8_url).text)
            tracks = Tracks.from_m3u8(master=m3u8.loads(manifest, m3u8_url), source=self.ALIASES[0])           

            

            # Remove separate AD audio tracks
            for track in tracks.audios:
                tracks.audios.remove(track)

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, **_):
        if not self.license_url:
            return None

        r = self.session.post(url=self.license_url, data=challenge)
        if r.status_code != 200:
            raise ConnectionError(r.text)

        return r.content

    # service specific functions

    @staticmethod
    def clean_manifest(text: str) -> str:
        # Remove fairplay entries
        index = text.find('#PLUTO-DRM:ID="fairplay')
        if index == -1:
            return text
        else:
            end_of_previous_line = text.rfind("\n", 0, index)
            if end_of_previous_line == -1:
                return ""
            else:
                return text[:end_of_previous_line]

    @staticmethod
    def bumpers(text: str) -> bool:
        ads = (
            "Pluto_TV_OandO",
            "_ad",
            "creative",
            "Bumper",
            "Promo",
            "WarningCard",
        )

        return any(ad in text for ad in ads)

    @staticmethod
    def year(data: dict):
        title_year = (int(match.group(1)) if (match := re.search(r"\((\d{4})\)", data.get("name", ""))) else None)
        slug_year = (int(match.group(1)) if (match := re.search(r"\b(\d{4})\b", data.get("slug", ""))) else None)
        return None if title_year else slug_year
        
