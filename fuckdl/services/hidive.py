from __future__ import annotations

import re
import json
import click
import time
from copy import copy
from pathlib import Path
from hashlib import md5, sha1
from typing import Any, Optional, Union
from urllib.parse import urlparse, parse_qs, unquote

from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Tracks, TextTrack, Tracks, MenuTrack


class Hidive(BaseService):
    """
    Service code for Hidive (https://www.hidive.com/).

    \b
    Added by @AnotherBigUserHere
  
    Original Author: scirocco
 
    + Fist login retrieving fixed
    + Profile information
    + Web Login
    
    \b

    Authorization: Credentials
    Security:
      Widevine:
        L3: 1080p, AAC2.0

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
   
    \b
    Usage:
        - Title inputs and slugs:
            - Series:
                https://www.hidive.com/season/19078?seriesId=1049
                https://www.hidive.com/season/19078
                season/19078
            - Movies:
                https://www.hidive.com/playlist/29749
                playlist/29749
    """
    ALIASES  = ["HIDI"]
    GEOFENCE = ["us"]
    TITLE_RE = [
        r"^https?://(?:www\.)?hidive\.com/(?P<type>season)/(?P<id>\d+)(?:/)?(?:\?.*)?$",
        r"^(?P<type>season)/(?P<id>\d+)",
        r"^(?P<type>playlist)/(?P<id>\d+)",
    ]

    @staticmethod
    @click.command(name="Hidive", short_help="https://www.hidive.com", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return Hidive(ctx, **kwargs)

    def __init__(self, ctx, title):
        super().__init__(ctx)

        data = self.parse_title(ctx, title)
        self.title = data.get("id")
        self.type: str = data.get("type") if data else "season"
        self.token_expiry = None

        self.configure()
    
    def get_titles(self):
        titles = []
        if self.type == "season":
            res = self.session.get(
                self.config["endpoints"]["title_data"],
                params={
                    'type': 'season',
                    'id': self.title,
                }
            ).json()
            current_season = res["metadata"]["currentSeason"]["seasonId"]
            self.series_id = res["metadata"]["series"]["seriesId"]

            data = self.session.get(
                self.config["endpoints"]["series_data"].format(
                    series_id=self.series_id, season_id=current_season
                ),
                params = {
                    'size': '25',
                }
            ).json()
            
            multi_season = bool(data.get("precedingSeasons") or data.get("followingSeasons"))
            if multi_season:
                seasons = self.organize_seasons(data, current_season)
            else:
                seasons = [current_season]
            
            for season in seasons:
                res = self.session.get(
                    self.config["endpoints"]["title_data"],
                    params={
                        'type': 'season',
                        'id': season,
                    }
                ).json()
                
                for episode in res["elements"][3]["attributes"]["items"]:
                    ep_num, ep_name = self.parse_episode_title(episode["title"])
                    titles.append(Title(
                        id_=episode["id"],
                        type_=Title.Types.TV,
                        name=res["metadata"]["series"]["title"],
                        season=res["elements"][2]["attributes"]["seasons"]["items"][0]["seasonNumber"],
                        episode=ep_num,
                        source=self.ALIASES[0],
                        episode_name=ep_name,
                        service_data=episode,
                    ))
        elif self.type == "playlist":
            res = self.session.get(
                self.config["endpoints"]["title_data"],
                params={
                    'type': 'playlist',
                    'id': self.title,
                }
            ).json()

            # TODO: safer approach

            title = (
                res["elements"][0]
                ["attributes"]["actions"][0]
                ["attributes"]["elements"][0]
                ["attributes"]["action"]["data"]["title"]
            )
            videoID = (
                res["elements"][0]
                ["attributes"]["actions"][0]
                ["attributes"]["elements"][0]
                ["attributes"]["action"]["data"]["videoId"]
            )
            raw_tags = (
                res["elements"][0]
                ["attributes"]["content"][1]
                ["attributes"]["tags"]
            )
            tags = [
                tag["attributes"]["text"]
                for tag in raw_tags
                if tag.get("$type") == "textblock"
            ]

            for t in tags:
                match = re.search(r"\b(19|20)\d{2}\b", t)
                if match:
                    year = int(match.group())
                    break
            
            return Title(
                id_=videoID,
                type_=Title.Types.MOVIE,
                name=title,
                year=year,
                source=self.ALIASES[0],
                service_data=res,
            )
        
        return titles

    def get_tracks(self, title):
        tracks = []
        playback_details = self.session.get(
            self.config["endpoints"]["playbackDetails"].format(id=title.id),
            params = {'includePlaybackDetails': 'URL',}
        ).json()
        
        if playback_details["accessLevel"] != "GRANTED":
            self.log.warning(" - You're not allowed to play this video.")
            return []
        
        self.extra = {
            "duration": playback_details.get("duration", 0),
            "skipMarkers": []
        }
        
        for lang in playback_details["onlinePlaybackMetadata"]["audioTracks"]:
            if lang["languageCode"] == "jpn":
                title.original_lang = "ja-JP"
                break
            elif lang["languageCode"] == "eng" and title.original_lang is None:
                title.original_lang = "en-US"
        
        query = parse_qs(urlparse(playback_details["playerUrlCallback"]).query)
        params = {k: unquote(v[0]) for k, v in query.items()}

        playback = self.session.get(
            self.config["endpoints"]["playback"].format(id=title.id),
            params=params,
        ).json()

        skip_markers = playback.get("skipMarkers")
        if isinstance(skip_markers, list) and skip_markers:
            self.extra["skipMarkers"] = skip_markers

        track = Tracks.from_mpd(
            url=playback["dash"][0]["url"],
            session=self.session,
            source=self.ALIASES[0]
        )

        for subtitle in playback["dash"][0]["subtitles"]:
            if subtitle["format"] == "srt":
                track.add(TextTrack(
                    id_=md5(subtitle["url"].encode()).hexdigest()[0:6],
                    url=subtitle["url"],
                    codec=subtitle["format"],
                    language=subtitle["language"],
                    source=self.ALIASES[0],
                ))
        
        if not tracks:
            tracks = copy(track)
        
        self.license_auth = playback["dash"][0]["drm"]["jwtToken"]

        return tracks
    
    def get_chapters(self, title):
        if not getattr(self, "extra", {}).get("skipMarkers"):
            return []
        chapters = []

        chapters += [MenuTrack(
            number=len(chapters) + 1,
            title=f"Scene {len(chapters) + 1}",
            timecode="00:00:00.000",
        )]

        for marker in self.extra.get("skipMarkers"):
            t = marker["skipMarkerType"]

            if t == "SKIP_INTRO":
                chapters += [MenuTrack(
                    number=len(chapters) + 1,
                    title="Intro",
                    timecode=self.convert_time(marker.get("startTimeMs")),
                )]
                chapters += [MenuTrack(
                    number=len(chapters) + 1,
                    title=f"Scene {len(chapters) + 1}",
                    timecode=self.convert_time(marker.get("stopTimeMs")),
                )]

            if t == "SKIP_CREDITS":
                chapters += [MenuTrack(
                    number=len(chapters) + 1,
                    title="Credits",
                    timecode=self.convert_time(marker.get("startTimeMs")),
                )]
                chapters += [MenuTrack(
                    number=len(chapters) + 1,
                    title=f"Scene {len(chapters) + 1}",
                    timecode=self.convert_time(marker.get("stopTimeMs")),
                )]
        
        return chapters

    
    def certificate(self, **kwargs: Any) -> bytes:
        return self.license(**kwargs)
    
    def license(self, challenge: bytes, **_: Any) -> bytes:
        headers = {
            'authorization': f'Bearer {self.license_auth}',
            'x-drm-info': 'eyJzeXN0ZW0iOiJjb20ud2lkZXZpbmUuYWxwaGEifQ==',
        }
        r = self.session.post(self.config["endpoints"]["license"], headers=headers, data=challenge)
        if r.status_code != 200:
            raise ConnectionError(r.text)
        return r.content


    # Service-specific functions

    def convert_time(self, ms: int) -> str:
        h, r = divmod(ms, 3_600_000)
        m, r = divmod(r, 60_000)
        s, ms = divmod(r, 1_000)
        return f"{h:02}:{m:02}:{s:02}.{ms:03}"

    def configure(self):
        self.log.info("Logging in")

        self.session.headers.update({
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US',
            'app': 'dice',
            'content-type': 'application/json',
            'origin': 'https://www.hidive.com',
            'referer': 'https://www.hidive.com/',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'realm': 'dce.hidive',
            'x-api-key': '857a1e5d-e35e-4fdf-805b-a87b6f8364bf',
            'x-app-var': '6.58.1.502e001',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        })

        if self.credentials:
            cache_path = Path(self.get_cache(
                "tokens_{hash}.json".format(
                    hash=sha1(f"{self.credentials.username}".encode()).hexdigest(),
                ),
            ))

            if cache_path.is_file():
                # cached
                tokens = json.loads(cache_path.read_text())
                self.refresh_token = tokens["refreshToken"]
                self.auth_token = tokens["authorisationToken"]
                self.token_expiry = tokens["expires_at"]
                self.log.info("Using cached tokens")
                self.session.headers.update({
                    'authorization': f'Bearer {self.auth_token}'
                })

                # expired, refresh
                if self.token_expiry < time.time():
                    self.log.info("Token expired. Refreshing...")
                    res = self.session.post(
                        self.config["endpoints"]["refresh"],
                        json = {
                            'refreshToken': tokens["refreshToken"]
                        }
                    ).json()
                    if "authorisationToken" in res:
                        self.log.info(" + Refreshed")
                        res["expires_at"] = time.time() + 600
                        res["refreshToken"] = self.refresh_token
                        self.auth_token = res["authorisationToken"]
                        self.token_expiry = res["expires_at"]
                        self.session.headers.update({
                            'authorization': f'Bearer {self.auth_token}'
                        })
                    cache_path.write_text(json.dumps(res))
                self.get_profile_info()
                
                time.sleep(1.5)

                self.get_profile_info(silent=True)
            else:
                # new
                cache_path.parent.mkdir(exist_ok=True, parents=True)
                self.log.info(" + Getting logging tokens")
                
                # 1. guest
                guest_token = self.get_guest_token()
                self.session.headers.update({
                    'authorization': f'Bearer {guest_token}'
                })
                
                # 2. login
                res = self.session.post(
                    self.config["endpoints"]["login"],
                    json={
                        'id': self.credentials.username,
                        'secret': self.credentials.password,
                    },
                ).json()
                
                self.refresh_token = res["refreshToken"]
                self.auth_token = res["authorisationToken"]
                
                self.session.headers.update({
                    'authorization': f'Bearer {self.auth_token}'
                })
                
                # 5. warmup
                self.get_profile_info()
                
                time.sleep(1)
                self.get_profile_info(silent=True)
                
                # 6. cache
                res["expires_at"] = time.time() + 600
                cache_path.write_text(json.dumps(res))
    
    def parse_episode_title(self, title):
        """Extract episode number and name."""
        match = re.match(r"E(\d+)\s*-\s*(.*)", title)
        if match:
            ep_num = int(match.group(1))
            ep_name = match.group(2).strip()
            return ep_num, ep_name
        return None, title.strip()

    def organize_seasons(self, data, current_season_id=None):
        """Organize season IDs in watch order."""
        seasons = []

        for s in data.get("precedingSeasons", []):
            seasons.append(s["id"])

        if current_season_id is not None:
            seasons.append(current_season_id)

        for s in data.get("followingSeasons", []):
            seasons.append(s["id"])

        return seasons

    def get_profile_info(self, silent: bool = False):
        res = self.session.get(
            self.config["endpoints"]["profile"],
        ).json()
    
        profile = res["items"][0]
    
        self.profile_id = profile["profileId"]
        self.profile_name = (
            profile.get("profileName")
            or profile.get("name")
            or "Default"
        )
    
        if not silent:
            self.log.info(f" + Profile: {self.profile_name} ({self.profile_id})")
    
    def get_guest_token(self):
        res = self.session.post(
            self.config["endpoints"]["guest_token"],
        ).json()["authorisationToken"]
        return res