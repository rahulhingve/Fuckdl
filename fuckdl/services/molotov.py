import base64
import json
import time
import re
import uuid
import subprocess
import shutil
import hashlib
import os
import m3u8
import requests
import click

from pathlib import Path
from typing import Any, Union
from hashlib import md5
from bs4 import BeautifulSoup
from fuckdl.objects import MenuTrack, Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService
from langcodes import Language

os.system('')
GREEN = '\033[32m'
MAGENTA = '\033[35m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
RED = '\033[31m'
RESET = '\033[0m'


class Molotov(BaseService):
    """
    \b
    Authorization: Cookies
    Security: FHD@L3, doesn't seem to care about releases.
    Example input: 
    SERIES: poetry run fuckdl dl mltv https://app.molotov.tv/channel/2426/program/56177
    MOVIE: poetry run fuckdl dl mltv -m https://app.molotov.tv/channel/2426/program/6545557

    \b

    Original Author: TANZ - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["MLTV"]
    GEOFENCE = []

    @staticmethod
    @click.command(name="Molotov", short_help="https://app.molotov.tv/")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a Movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Molotov(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        self.title = title
        self.movie = movie

        self.channel = title.split('/')[-3]
        self.program = title.split('/')[-1]
        super().__init__(ctx)

        self.configure()

    def get_titles(self):
        season_api = self.get_api_url(self.channel, self.program)
        
        r = self.session.get(
            url=season_api,
            params = {
                'trkCp': 'category',
                'trkCs': 'vods-mango',
                'trkOcr': '1',
                'trkOsp': '6',
                'access_token': self.token,
            }
        )

        try:
            data = r.json()
        except (json.JSONDecodeError, KeyError):
            raise ValueError(f"Failed to getting info: {r.text}")

        # Auto-detect movie vs series
        if not self.movie:
            program = data.get('program', {})
            if program:
                has_seasons = bool(data.get('seasons_selector'))
                has_video = bool(program.get('video'))
                has_season_ep = bool(data.get('season_episode_sections'))
                
                if has_video and not has_seasons and not has_season_ep:
                    self.movie = True
                    self.log.info(" + Auto-detected as Movie")

        if self.movie:
            try:
                get_year = match = re.search(r"\b\d{4}\b", data['program']['subtitle'])
            except (KeyError, TypeError):
                get_year = None
            
            if get_year:
                year = int(match.group())
            else:
                year = None
            
            return Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name = data['program']['title'].title() if 'program' in data and data['program'] else data['page_title'],
                year=year,
                original_lang='fr', #TODO: Don't assume
                source=self.ALIASES[0],
                service_data=data['program'] if 'program' in data and data['program'] else data['season'],
            )
        else:
            titles = []
            seen = set()
            
            def process_episodes(episode_list, name, titles):
                for e in episode_list:
                    subtitle = e.get('subtitle')
                    
                    if not isinstance(subtitle, str):
                        continue

                    match = re.match(r"S(\d{2})E(\d{2})", subtitle)
                    if not match:
                        continue

                    season = int(match.group(1))
                    episode = int(match.group(2))

                    eid = e.get('id')
                    if eid:
                        key = ('id', eid)
                    else:
                        key = ('se', name.lower(), season, episode)

                    if key in seen:
                        continue

                    seen.add(key)
                    titles.append(Title(
                        id_=e["id"],
                        type_=Title.Types.TV,
                        name=name.title(),
                        season=season,
                        episode=episode,
                        episode_name=e['title'].title(),
                        original_lang=e.get('language', 'fr'),  # Default to 'fr' if not specified
                        source=self.ALIASES[0],
                        service_data=e
                    ))

            # First try: Extract from 'program_episode_sections'
            try:
                ep_sections = data.get('program_episode_sections', [])
                for episode in ep_sections:
                    if episode.get('slug') == 'episodes-futures':
                        episode_list = episode.get('items', [])
                        process_episodes(episode_list, data['program']['title'], titles)
                        return titles

            except KeyError as e:
                self.log.warning(f"KeyError in program_episode_sections: {e}")
            except Exception as e:
                self.log.error(f"Unexpected error in program_episode_sections: {e}")

            # Second try: Extract from 'seasons_selector'
            try:
                season_data = data.get('seasons_selector', {}).get('items', [])
                for season in season_data:
                    season_detail = season.get('actions', {}).get('detail', {}).get('url')
                    if not season_detail:
                        continue
                    
                    res = self.session.get(
                        url=season_detail,
                        params={'access_token': self.token}
                    ).json()

                    ep_sections = res.get('season_episode_sections', [])
                    for episode in ep_sections:
                        if episode.get('slug') == 'vod' or episode.get('slug') == 'episodes-replays':
                            episode_list = episode.get('items', [])
                            process_episodes(episode_list, data['season']['title'], titles)
            
            except KeyError as e:
                self.log.warning(f"KeyError in seasons_selector: {e}")
            except Exception as e:
                self.log.error(f"Unexpected error in seasons_selector: {e}")

            # third try: Extract from 'channel_episode_sections'
            try:
                ep_sections = data.get('channel_episode_sections', [])
                for episode in ep_sections:
                    if episode.get('slug') == 'future':
                        episode_list = episode.get('items', [])
                        process_episodes(episode_list, data.get('page', {}).get('metadata', {}).get('program_title', 'Unknown'), titles)
            except KeyError as e:
                self.log.warning(f"KeyError in channel_episode_sections: {e}")
            except Exception as e:
                self.log.error(f"Unexpected error in channel_episode_sections: {e}")

            # Final fallback: Attempt to use 'season_episode_sections' directly
            try:
                ep_sections = data.get('season_episode_sections', [])
                for episode in ep_sections:
                    if episode.get('slug') == 'vod':
                        episode_list = episode.get('items', [])
                        process_episodes(episode_list, data['season']['title'], titles)
            except KeyError as e:
                self.log.warning(f"KeyError in season_episode_sections: {e}")
            except Exception as e:
                self.log.error(f"Unexpected error in season_episode_sections: {e}")

            return titles

   
    def get_tracks(self, title):
        svdata = title.service_data
        
        if self.movie:           
            res = self.session.get(
                url='https://fapi.molotov.tv/v2/me/assets',
                params = {
                    'id': svdata['video']['id'],
                    'trkCp': 'program',
                    'trkCs': 'django-unchained',
                    'trkOcr': '1',
                    'trkOp': 'category',
                    'trkOs': 'trade_marketing',
                    'trkOsp': '1',
                    'type': 'vod',
                    'position': 'NaN',
                    'start_over': 'false',
                    'embedded': 'false',
                    'skip_dialogs': 'false',
                    'access_token': self.token,
                },
                headers = {
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://app.molotov.tv',
                    'priority': 'u=1, i',
                    'referer': 'https://app.molotov.tv/',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0',
                    'x-molotov-agent': '{"app_id":"browser_app","app_build":4,"app_version_name":"4.4.4","browser_name":"edge","type":"desktop","os_version":"5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0","electron_version":"0.0.0","os":"Win32","manufacturer":"","serial":"1e6f4bec-5927-4c87-a9cf-b14f9bf31ee3","model":"Edge - Windows","hasTouchbar":false,"brand":"Windows NT 10.0; Win64; x64","api_version":8,"features_supported":["social","new_button_conversion","paywall","channel_separator","empty_view_v2","store_offer_v2","player_mplus_teasing","embedded_player","channels_classification","new-post-registration","appstart-d0-full-image","payment_v2","armageddon","user_favorite","parental_control_v3","emptyview_v2","before_pay_periodicity_selection","player_midrolls","cookie_wall","reverse_epg"],"inner_app_version_name":"6.2.1","qa":false}',
                    'x-tcf-string': 'CQevS8AQevS8AAHABBENCPFsAP_gAEPgAAqILPtX_G__bWlr8X73aftkeY1P9_h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3DBIQNlGJDURVCgaogFryDMaEyUoTNKJ6BkiFMRM2dYCFxvmwtD-QCY4vr99lcx2B-t7dr83dzyy4xHn3a5_2S0WJCcA5-tDfv9ZROb-9IOd_x8v4t4_EfpE2_eT1l_pGvp7Dd-cls__XW59_fff_9Pn_-uB_-_3_vc_EQAAAAAAAAAAAgqmASYaFRBGWRISEWgYQQIAVBWEBFAgCAABIGiAgBMGBTkDABdYSIAQAoABggBAACDIAEAAAkACEQAQAFAgAAgECgADAAgGAgAIGAAEAFiIBAACA6BCkBBAIFgAkZkVCmBCEAkEBLZUIJAECCuEIRZ4BEAiJgoAAAAACkAAQFgsDiSQEqEggC4g2gAAIAEAggQKEElJgACgM2WoPBg2jK0wDB8wSIaYBkARBCQgAAA.f_wACHwAAAAA',
                }
            )

        else:
            res = self.session.get(
                url='https://fapi.molotov.tv/v2/me/assets',
                params = {
                    'id': svdata['video']['id'],
                    'trkCp': 'season',
                    'trkOcr': '2',
                    'trkOp': 'category',
                    'trkOs': 'category_most_popular_vods',
                    'trkOsp': '3',
                    'type': 'vod',
                    'position': 'NaN',
                    'start_over': 'false',
                    'embedded': 'false',
                    'skip_dialogs': 'false',
                    'access_token': self.token,
                },
                headers = {
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://app.molotov.tv',
                    'priority': 'u=1, i',
                    'referer': 'https://app.molotov.tv/',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Microsoft Edge";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0',
                    'x-molotov-agent': '{"app_id":"browser_app","app_build":4,"app_version_name":"4.4.4","browser_name":"edge","type":"desktop","os_version":"5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0","electron_version":"0.0.0","os":"Win32","manufacturer":"","serial":"1e6f4bec-5927-4c87-a9cf-b14f9bf31ee3","model":"Edge - Windows","hasTouchbar":false,"brand":"Windows NT 10.0; Win64; x64","api_version":8,"features_supported":["social","new_button_conversion","paywall","channel_separator","empty_view_v2","store_offer_v2","player_mplus_teasing","embedded_player","channels_classification","new-post-registration","appstart-d0-full-image","payment_v2","armageddon","user_favorite","parental_control_v3","emptyview_v2","before_pay_periodicity_selection","player_midrolls","cookie_wall","reverse_epg"],"inner_app_version_name":"6.2.1","qa":false}',
                    'x-tcf-string': 'CQevS8AQevS8AAHABBENCPFsAP_gAEPgAAqILPtX_G__bWlr8X73aftkeY1P9_h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3DBIQNlGJDURVCgaogFryDMaEyUoTNKJ6BkiFMRM2dYCFxvmwtD-QCY4vr99lcx2B-t7dr83dzyy4xHn3a5_2S0WJCcA5-tDfv9ZROb-9IOd_x8v4t4_EfpE2_eT1l_pGvp7Dd-cls__XW59_fff_9Pn_-uB_-_3_vc_EQAAAAAAAAAAAgqmASYaFRBGWRISEWgYQQIAVBWEBFAgCAABIGiAgBMGBTkDABdYSIAQAoABggBAACDIAEAAAkACEQAQAFAgAAgECgADAAgGAgAIGAAEAFiIBAACA6BCkBBAIFgAkZkVCmBCEAkEBLZUIJAECCuEIRZ4BEAiJgoAAAAACkAAQFgsDiSQEqEggC4g2gAAIAEAggQKEElJgACgM2WoPBg2jK0wDB8wSIaYBkARBCQgAAA.f_wACHwAAAAA',
                }
            )
        
        try:
            data = res.json()
        except Exception:
            raise ValueError(f"Failed to load manifest url: {res.text}")

        manifest_url = data['stream']['url']
        self.license_url = data['drm']['license_url']
        self.license_token = data['drm']['token']

        tracks = Tracks.from_mpd(
            url=manifest_url,
            session=self.session,
            source=self.ALIASES[0]
        )

        self.log.debug(f" + MPD: {manifest_url}")
        self.log.debug(f" + LICENSE: {self.license_url}")

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **kwargs):
        # TODO: Hardcode the certificate
        # return self.license(**kwargs)
        return None

    def license(self, challenge, **_):     
        r = self.session.post(
            url=self.license_url,
            data=challenge,
            headers={
                'x-dt-auth-token': self.license_token
            }
        )
        if not r.content:
            self.log.exit(" - No license returned!")
        else:
            return r.content
   
    def configure(self):
        self.log.info(" + Starting Molotov TV...")
        self.auth_grant = self.get_auth_grant()
        self.token = self.auth_grant['access_token']
        
        self.session.headers.update({
            'authority': 'fapi.molotov.tv',
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://app.molotov.tv',
            'referer': 'https://app.molotov.tv/',
            'x-molotov-agent': json.dumps(self.config['molotov_agent']),
            'access_token': self.token
        })

    def get_auth_grant(self):
        if not self.credentials:
            self.log.exit(" x No credentials provided, unable to log in.")

        tokens_cache_path = Path(self.get_cache("token_molotov.json"))

        if tokens_cache_path.is_file():
            tokens = json.loads(tokens_cache_path.read_text(encoding="utf8"))
            
            if time.time() < tokens["access_token_expires_at"]:
                return tokens
            # expired, refreshing:
            tokens = self.refresh(refresh_token=tokens["refresh_token"])
        else:
            headers = {
                'authority': 'fapi.molotov.tv',
                'accept': 'application/json',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json',
                'origin': 'https://app.molotov.tv',
                'referer': 'https://app.molotov.tv/',
                'x-molotov-agent': json.dumps(self.config['molotov_agent']),
            }

            res = self.session.post(
                url='https://fapi.molotov.tv/v3.1/auth/login', 
                headers=headers,
                json={
                    'grant_type': 'password',
                    'email': self.credentials.username,
                    'password': self.credentials.password,
                }
            ).json()
            
            tokens = res['auth']

        os.makedirs(os.path.dirname(tokens_cache_path), exist_ok=True)
        with open(tokens_cache_path, "w", encoding="utf-8") as fd:
            json.dump(tokens, fd)
        
        return tokens

    def refresh(self, refresh_token):
        headers = {
            'authority': 'fapi.molotov.tv',
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'origin': 'https://app.molotov.tv',
            'referer': 'https://app.molotov.tv/',
            'x-molotov-agent': json.dumps(self.config['molotov_agent']),
        }

        res = self.session.get(
            url=f'https://fapi.molotov.tv/v3/auth/refresh/{refresh_token}', 
            headers=headers
        ).json()

        return res

    def get_api_url(self, channel, program):
        res = self.session.post(
            url='https://fapi.molotov.tv/v2/deeplinks/actions',
            json={
                'deeplink': f'molotov://deeplink?type=program&id={program}&channel_id={channel}',
            },
            params = {
                'access_token': self.token,
            },
            headers = {
                'accept': 'application/json',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json',
                'origin': 'https://app.molotov.tv',
                'priority': 'u=1, i',
                'referer': 'https://app.molotov.tv/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'x-molotov-agent': json.dumps(self.config['molotov_agent']),
            }
        ).json()
        
        return res[0]["url"]