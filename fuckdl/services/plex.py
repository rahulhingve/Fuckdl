import sys
import uuid
import os
import click
import requests
import re
import json

from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Track, Tracks, AudioTrack, TextTrack, MenuTrack
from fuckdl.utils import Cdm
from fuckdl.utils.widevine.device import LocalDevice


class Plex(BaseService):
    """
    Service code for Plex (https://watch.plex.tv/).

    \b
    Version: 20250913
    Author: rd
    Authorization: None
    Security: HD@L3/SL2000

    \b
    Tips:
        - Input can be complete title URL or just the path:
            https://watch.plex.tv/movie/the-men-who-stare-at-goats
            /movie/the-men-who-stare-at-goats
        - Individual season or episode
            https://watch.plex.tv/show/mad-men/season/1
            /show/mad-men/season/1
            https://watch.plex.tv/show/mad-men/season/1/episode/1
            /show/mad-men/season/1/episode/1

    \b
    TODO:
        - Plex returns grouped playback data for series.

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["PLEX", "Plex"]
    GEOFENCE = []
    TITLE_RE = r'^(?:https?://watch\.plex\.tv(?:/[^/?#]+)*)?/?(?P<path>(?:movie|show)/[^?#]*)'

    @staticmethod
    @click.command(name="Plex", short_help="https://watch.plex.tv/", help=__doc__)
    @click.argument("title", type=str)
    @click.pass_context
    def cli(ctx, **kwargs):
        return Plex(ctx, **kwargs)

    def __init__(self, ctx, title: str):
        super().__init__(ctx)
        self.title = title
        self.cdm = ctx.obj.cdm
        cdm = self.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.drm_type = "playready" if self.playready else "widevine"
        self.auth_token = None

    def get_titles(self):
        try:
            match = re.match(self.TITLE_RE, self.title)
            path = "/" + match.group("path")
        except Exception:
            self.log.exit(" - Unable to extract path from title - is the URL/Path correct?")

        data = self.get_details(path)
        metadata = data["actions"][0]["metrics"]["click"]["properties"]["metadataItem"]
        kind = metadata["type"]

        if not data["ui"]["playableAVOD"]:
            self.log.exit(f" - This {kind} is not playable.")

        if kind == "movie":
            return Title(
                    id_=metadata["id"],
                    type_=Title.Types.MOVIE,
                    name=metadata["title"],
                    year=metadata["releaseDate"][:4],
                    source=self.ALIASES[0],
                    service_data=data,
                )

        if kind in ["episode", "season", "show"]:
            titles = []
            episodes = []

            if kind == "episode":
                content = data["ui"]["siblings"]["content"]
                episode = next((item for item in content if item["id"] == metadata["id"]), None)
                episodes = [episode] if episode else []

            if kind == "season":
                episodes = data["ui"]["children"]["content"]

            if kind == "show":
                for season in data["ui"]["children"]["content"]:
                    season_data = self.get_details(season["link"]["url"])
                    if not season_data["ui"]["playableAVOD"]:
                        continue
                    episodes.extend(season_data["ui"]["children"]["content"])

            for episode in episodes:
                match = re.search(r"season/(\d+)/episode/(\d+)", episode["link"]["url"])
                season_number, episode_number = match.groups() if match else (0, 0)

                titles.append(
                    Title(
                        id_=episode["id"],
                        type_=Title.Types.TV,
                        name=data["ui"]["metadata"]["title"],
                        year=episode["subtitle"][-4:],
                        season=season_number,
                        episode=episode_number,
                        episode_name=episode["title"],
                        source=self.ALIASES[0],
                        service_data=episode,
                    )
                )

            return titles

    def get_tracks(self, title):
        try:
            data = self.get_playback(title.id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "Cannot create an empty playQueue!" in e.response.text:
                self.log.warning("This episode is not playable, skipping...")
                return []
            else:
                self.log_http_error("Playback request failed", e)

        play_queue = data["MediaContainer"]["Metadata"]
        queue_item = next((item for item in play_queue if item["ratingKey"] == title.id), None)
        title.service_data = queue_item # for chapters

        if queue_item is None:
            self.log.exit(" - Content ID not found in playback queue.")

        media = queue_item["Media"]
        dash = next((item for item in media if item["protocol"].lower() == "dash"), None)
        if not dash:
            self.log.exit(f" - DASH manifest is not available for this title.")

        key = dash["Part"][0]["key"]
        has_drm = dash.get("drm")
        if has_drm:
            license = dash["Part"][0]["license"]
            self.license_url = self.config["endpoints"]["license"].format(license=license, token=self.auth_token, drm=self.drm_type)
            manifest_url = self.config["endpoints"]["manifest_enc"].format(key=key, token=self.auth_token, drm=self.drm_type)
        else:
            self.license_url = None
            manifest_url = self.config["endpoints"]["manifest_dec"].format(key=key, token=self.auth_token)

        tracks = Tracks.from_mpd(
            url=manifest_url,
            session=self.session,
            source=self.ALIASES[0],
        )

        # set default lang
        streams = dash["Part"][0]["Stream"]
        title.original_lang = next((x["languageCode"] for x in streams if x["streamType"] == 2 and x["selected"]), "en")

        # set descriptive audio
        for track in tracks.audios:
            role = track.extra[1].find("Role")
            if role is not None and role.get("value") == "description":
                track.descriptive = True

        # dl tracks w/o proxy
        for track in tracks:
            track.needs_proxy = False

        self.log.debug(f" + Manifest URL: {manifest_url}")
        return tracks

    def get_chapters(self, title):
        markers = title.service_data.get("Marker") or []

        for marker in markers:
            if marker['type'] == "credits":
                return [MenuTrack(
                    number=1,
                    title="Credits",
                    timecode=self.format_time(marker["startTimeOffset"] / 1000)
                )]

        return []

    def certificate(self, **_):
        return None  # will use common privacy cert

    def license(self, challenge, **_):
        if not self.license_url:
            return None

        try:
            res = self.session.post(url=self.license_url, data=challenge)
        except requests.exceptions.HTTPError as e:
            self.log_http_error("License request failed", e)

        return res.content

    # Service specific functions

    def get_details(self, path: str):
        try:
            response = self.session.get(
                url=f'{self.config["endpoints"]["screen"]}{path}',
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                    "x-plex-product": "Plex for Android (Mobile)",
                    "x-plex-version": "2025.22.0"
                }
            )
        except requests.exceptions.HTTPError as e:
            self.log_http_error("Title details request failed", e)

        return response.json()

    def get_playback(self, content_id: str):
        headers = {
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/json',
            'Origin': 'https://watch.plex.tv',
            'Referer': 'https://watch.plex.tv/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0',
            'X-Plex-Client-Identifier': str(uuid.uuid4()),
            'X-Plex-Device': 'Windows',
            'X-Plex-Drm': self.drm_type,
            'X-Plex-Language': 'en',
            'X-Plex-Platform': 'Edge',
            'X-Plex-Platform-Version': '137.0.0.0',
            'X-Plex-Playback-Session-Id': str(uuid.uuid4()),
            'X-Plex-Product': 'Plex Mediaverse',
            'X-Plex-Provider-Version': '6.5.0',
        }

        # get authToken
        if self.auth_token is None:
            res_auth = self.session.post(url=self.config["endpoints"]["auth"], headers=headers)
            self.auth_token = res_auth.json()["authToken"]

        # get manifest
        headers['X-Plex-Token'] = self.auth_token
        params = {
            'uri': f'provider://tv.plex.provider.vod/library/metadata/{content_id}',
            'type': 'video',
            'continuous': '1',
        }

        res_play = self.session.post(url=self.config["endpoints"]["play"], params=params, headers=headers)
        return res_play.json()

    def log_http_error(self, message: str, e: requests.exceptions.HTTPError):
        try:
            error_msg = json.dumps(e.response.json(), indent=None, ensure_ascii=False)
        except ValueError:
            error_msg = e.response.text
        self.log.error(f" - {message}: {error_msg}", exc_info=False)
        self.log.error(f" - HTTP Error {e.response.status_code}: {e.response.reason}")
        sys.exit(1)

    @staticmethod
    def format_time(seconds):
        """Converts seconds to a HH:MM:SS.mmm timecode string."""
        total_ms = int(round(seconds * 1000))
        secs, ms = divmod(total_ms, 1000)
        mins, secs = divmod(secs, 60)
        hrs, mins = divmod(mins, 60)
        timecode = f'{hrs:02d}:{mins:02d}:{secs:02d}.{ms:03d}'
        return timecode
