import click
import re
import uuid
import requests
import base64
import os
import json

from pymp4.parser import Box
from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.config import config
from fuckdl.utils.widevine.device import LocalDevice

os.system("")
BLACK = '\033[30m'
RED = '\033[31m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
MAGENTA = '\033[35m'
CYAN = '\033[36m'
BOLD_CYAN = '\033[1;36m'
WHITE = '\033[37m'
UNDERLINE = '\033[4m'
RESET = '\033[0m'

class M6(BaseService):
    """
    Service code for M6. streaming service (https://www.m6.fr/).

    \b
    Authorization: Credentials
    Security: FHD@L3, doesn't care about releases.

    Original author: TANZ

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    """

    ALIASES = ["M6"]

    @staticmethod
    @click.command(name="M6", short_help="https://www.m6.fr")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.option("-di", "--divertissement", is_flag=True, default=False, help="Title is an entertainment show (renumber seasons sequentially).")
    @click.pass_context
    def cli(ctx, **kwargs):
        return M6(ctx, **kwargs)

    def __init__(self, ctx, title, movie, divertissement):
        super().__init__(ctx)
        try:
            self.title = re.search(r'\d+$', title).group(0)
        except (AttributeError, TypeError):
            pass
        self.url = title
        self.movie = movie
        self.divertissement = divertissement
        self.cdm = ctx.obj.cdm

        # Detect URL type: p_ = program, f_ = folder, c_ = clip
        self.url_type = None
        type_match = re.search(r'-([pfc])_(\d+)', title or '')
        if type_match:
            self.url_type = type_match.group(1)
            self.title = type_match.group(2)

        self.configure()

    def get_titles(self):

        # For program URLs (p_XXXXX), use the program endpoint to find folders/seasons
        if self.url_type == 'p' and not self.movie:
            return self._get_program_titles()

        metadata = self.session.get(
            url=f"https://layout.6cloud.fr/front/v1/m6web/{self.platform}/main/{self.platform_token}/video/{self.title}/layout",
            params={"nbPages": "10"},
        ).json()

        if self.movie:
            movie_info = metadata["blocks"][0]["content"]["items"][0]
            metadata["viewable"] = movie_info["itemContent"]["action"]["target"]["value_layout"]["id"]

            return Title(
                id_=movie_info.get("ucid") or metadata.get("entity", {}).get("id"),
                type_=Title.Types.MOVIE,
                name=metadata["entity"]["metadata"]["title"],
                source=self.ALIASES[0],
                original_lang="fr",
                service_data=metadata,
            )

        folder_match = re.search(r"-f_(\d+)", self.url)

        if folder_match:
            folder_ids = [folder_match.group(1)]
        else:
            # No folder ID in URL â€” extract season folders from program layout metadata
            season_folders = self._extract_season_folders(metadata, show_title)
            folder_ids = [sf['folder_id'] for sf in season_folders]
            if not folder_ids:
                self.log.warning(" - No folder IDs found in program metadata")
                return []

        # If user specified a season via -w SXX, try to match it
        target_season = None
        if hasattr(self, '_season_filter'):
            target_season = self._season_filter

        show_title = (
            metadata.get("entity", {})
            .get("metadata", {})
            .get("title", "")
        )
        show_title = re.sub(
            r"\s*[-:â€“]\s*(Saison\s*\d+|S\d+).*",
            "",
            show_title,
            flags=re.I,
        )

        titles = []
        seen = set()

        for folder_id in folder_ids:
            layout, episodes = self.get_all_program_episodes(folder_id)

            for ep in episodes:
                item = ep.get("itemContent") or {}
                atwn = item.get("androidTvWatchNext") or {}

                episode_name = (
                    item.get("title")
                    or item.get("extraTitle")
                    or atwn.get("title")
                    or ""
                )

                video_id = ep.get("viewable")

                if not video_id or video_id in seen:
                    continue

                seen.add(video_id)
                season = atwn.get("seasonNumber") or 0
                episode = atwn.get("episodeNumber") or 0

                titles.append(
                    Title(
                        id_=ep.get("ucid") or video_id,
                        type_=Title.Types.TV,
                        name=show_title,
                        season=season,
                        episode=episode,
                        episode_name=episode_name,
                        source=self.ALIASES[0],
                        original_lang="fr",
                        service_data=ep,
                    )
                )

        titles.sort(key=lambda x: (x.season, x.episode))

        # Fix duplicate episode numbers within same season
        ep_counters = {}
        for t in titles:
            ep_counters.setdefault(t.season, 0)
            ep_counters[t.season] += 1
            t.episode = ep_counters[t.season]

        # Divertissement mode: renumber seasons sequentially
        if self.divertissement and titles:
            from collections import OrderedDict
            season_info = OrderedDict()
            for t in titles:
                if t.season not in season_info:
                    season_info[t.season] = {"count": 0, "first_ep": t.episode_name}
                season_info[t.season]["count"] += 1

            season_map = {}
            seq = 0
            for orig_s in season_info:
                seq += 1
                season_map[orig_s] = seq

            self.log.info(f" + Divertissement: renumbering {len(season_map)} seasons:")
            for orig_s, new_s in season_map.items():
                info = season_info[orig_s]
                self.log.info(f"   S{orig_s:>5} â†’ S{new_s:02d} ({info['count']} eps) â€” {info['first_ep'][:60]}")

            for t in titles:
                t.season = season_map[t.season]

        return titles


    def get_tracks(self, title):
        svdata = title.service_data

        def get_mpd_live(data):
            mpd_url = None
            view_id = None

            for block in data.get('blocks', []):
                content = block.get('content', {})
                items = content.get('items', [])
                for item in items:
                    item_content = item.get('itemContent', {})
                    video = item_content.get('video', {})
                    assets = video.get('assets', [])
                    
                    for asset in assets:
                        if asset.get('video_quality') == 'hd':
                            mpd_url = asset.get('path')
                            drm_info = asset.get('drm', {}).get('config', {})
                            view_id = drm_info.get('contentId')
                            break

            return mpd_url, view_id

        if self.movie:
            view_id = svdata["viewable"]
        else:
            if '-c_' in self.url:
                view_id = f"clip_{self.title}"
            elif 'live' in self.url:
                pass
            else:
                view_id = svdata["itemContent"]["androidTvWatchNext"]["videoId"]

        if 'live' in self.url:
            mpd_url, view_id = get_mpd_live(svdata)

            get_license_token = self.session.get(
                url='https://drm.6cloud.fr/v1/customers/m6web/platforms/{platform}/services/m6/users/{gigya}/live/{clip}/upfront-token'.format(
                platform=self.platform,
                gigya=self.gigya,
                clip=view_id,
                ),
            ).json()["token"]
            self.license_token = get_license_token
        else:
            manifest = self.session.get(
                url='https://layout.6cloud.fr/front/v1/m6web/{platform}/main/{token}/{endpoint}/layout'.format(
                    platform=self.platform,
                    token=self.platform_token,
                    endpoint=f"video/{view_id}",
                ),
                params={"nbPages": "2"},
            ).json()

            playerBlock = [
                block for block in manifest["blocks"] if block["templateId"] == "Player"
            ][0]
            assets = playerBlock["content"]["items"][0]["itemContent"]["video"]["assets"]

            if not assets:
                self.log.exit(f" - Manifest not Available")

            get_license_token = self.session.get(
                url='https://drm.6cloud.fr/v1/customers/m6web/platforms/{platform}/services/m6/users/{gigya}/videos/{clip}/upfront-token'.format(
                platform=self.platform,
                gigya=self.gigya,
                clip=view_id,
                ),
            ).json()["token"]
            self.license_token = get_license_token

            mpd_url = [
                asset
                for asset in assets
                if asset["quality"]
                == f"{'hd'}"
            ]
            
            if not mpd_url:
                mpd_url = [asset for asset in assets if asset["quality"] == "sd"][0]["path"]
            else:
                mpd_url = mpd_url[0]["path"]

        self.log.info(f'MPD: {mpd_url}')
        tracks = Tracks.from_mpd(
            url=mpd_url,
            session=self.session,
            source=self.ALIASES[0]
        )

        for track in tracks:
            for uri in track.url.copy():
                track.url[track.url.index(uri)] = re.sub(
                    r"https://.+?.6cloud.fr",
                    "https://origin.vod.6cloud.fr",
                    uri.split("?")[0],
                )

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **kwargs):
        # TODO: Hardcode the certificate
        return self.license(**kwargs)

    def license(self, challenge, **_):
        _is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                        (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                         self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        if _is_playready:
            res = self.session.post(
                url='https://lic.drmtoday.com/license-proxy-headerauth/drmtoday/RightsManager.asmx',
                data=challenge,  # expects bytes
                headers={"x-dt-auth-token": self.license_token},
            )

            if res.status_code != 200:
                raise FailedLicensing

            licensing = res.content

            return licensing
            
        else:
            res = self.session.post(
                url='https://lic.drmtoday.com/license-proxy-widevine/cenc/',
                data=challenge,  # expects bytes
                headers={"x-dt-auth-token": self.license_token},
            )

            if res.status_code != 200:
                raise FailedLicensing

            licensing = res.json()

            return licensing["license"]

    # Service specific functions

    def configure(self):
        self.platform = 'm6group_android_tv'
        self.platform_token = 'token-androidtv-3'
        auth = M6_AUTH(self)
        self.access_token = auth.access_token
        self.gigya = auth.authorization["UID"]

        self.session.headers.update({
            "origin": "https://www.m6.fr",
            "Authorization": f"Bearer {self.access_token}",
            "x-client-release": '6.2.5',
            "x-customer-name": "m6web",
        })

    def get_all_program_episodes(self, folder_id):

        layout = self.session.get(
            f"https://layout.6cloud.fr/front/v1/m6web/{self.platform}/main/{self.platform_token}/folder/{folder_id}/layout",
            params={"nbPages": "10"},
        ).json()

        episodes = []

        for block in layout.get("blocks", []):
            items = (block.get("content") or {}).get("items") or []

            for item in items:
                item_content = item.get("itemContent") or {}
                atwn = item_content.get("androidTvWatchNext") or {}

                video_id = atwn.get("videoId")

                if video_id:
                    item["viewable"] = video_id
                    episodes.append(item)

        return layout, episodes

    def _get_program_titles(self):
        """Handle program URLs (p_XXXXX) â€” discover seasons/folders from program layout."""
        metadata = self.session.get(
            url=f"https://layout.6cloud.fr/front/v1/m6web/{self.platform}/main/{self.platform_token}/program/{self.title}/layout",
            params={"nbPages": "10"},
        ).json()

        show_title = (
            metadata.get("entity", {})
            .get("metadata", {})
            .get("title", "")
        )
        show_title_clean = re.sub(
            r"\s*[-:â€“]\s*(Saison\s*\d+|S\d+).*",
            "",
            show_title,
            flags=re.I,
        ).strip()

        self.log.info(f" + Program: {show_title_clean}")

        # Collect episodes directly from program layout blocks (items with videoId)
        titles = []
        seen = set()

        for block in metadata.get("blocks", []):
            block_title = block.get("title") or ""
            if not isinstance(block_title, str):
                block_title = str(block_title)

            content = block.get("content") or {}
            items = content.get("items") or []

            for item in items:
                item_content = item.get("itemContent") or {}
                atwn = item_content.get("androidTvWatchNext") or {}

                video_id = atwn.get("videoId")
                if not video_id:
                    continue

                # Filter: only keep episodes that belong to this program
                ep_program = atwn.get("programName") or atwn.get("title") or ""
                ep_show = atwn.get("showTitle") or item_content.get("showTitle") or ""

                # Check if this episode belongs to our show
                if show_title_clean:
                    show_lower = show_title_clean.lower()
                    if (show_lower not in ep_program.lower()
                        and show_lower not in ep_show.lower()
                        and show_lower not in (item_content.get("title") or "").lower()
                        and show_lower not in block_title.lower()):
                        # Check season block title pattern or avant-premiÃ¨re block
                        if (not re.search(r'[Ss]aison\s*\d+', block_title)
                            and 'avant-premi' not in block_title.lower()
                            and 'avant premi' not in block_title.lower()):
                            continue

                if video_id in seen:
                    continue
                seen.add(video_id)

                episode_name = (
                    item_content.get("title")
                    or item_content.get("extraTitle")
                    or atwn.get("title")
                    or ""
                )

                season = atwn.get("seasonNumber") or 0
                episode = atwn.get("episodeNumber") or 0

                item["viewable"] = video_id
                titles.append(
                    Title(
                        id_=item.get("ucid") or video_id,
                        type_=Title.Types.TV,
                        name=show_title_clean,
                        season=season,
                        episode=episode,
                        episode_name=episode_name,
                        source=self.ALIASES[0],
                        original_lang="fr",
                        service_data=item,
                    )
                )

        # If we didn't get enough from direct items, try season folders
        if not titles:
            season_folders = self._extract_season_folders(metadata, show_title_clean)

            if not season_folders:
                self.log.warning(" - No episodes or season folders found")
                return []

            self.log.info(f" + Found {len(season_folders)} season(s):")
            for sf in season_folders:
                self.log.info(f"   Saison {sf['season_num']}: folder {sf['folder_id']}")

            for sf in season_folders:
                folder_id = sf['folder_id']
                layout, episodes = self.get_all_program_episodes(folder_id)
                self.log.info(f"   Saison {sf['season_num']}: {len(episodes)} episodes")

                for ep in episodes:
                    item = ep.get("itemContent") or {}
                    atwn = item.get("androidTvWatchNext") or {}

                    video_id = ep.get("viewable")
                    if not video_id or video_id in seen:
                        continue
                    seen.add(video_id)

                    episode_name = (
                        item.get("title")
                        or item.get("extraTitle")
                        or atwn.get("title")
                        or ""
                    )
                    season = atwn.get("seasonNumber") or sf['season_num']
                    episode = atwn.get("episodeNumber") or 0

                    titles.append(
                        Title(
                            id_=ep.get("ucid") or video_id,
                            type_=Title.Types.TV,
                            name=show_title_clean,
                            season=season,
                            episode=episode,
                            episode_name=episode_name,
                            source=self.ALIASES[0],
                            original_lang="fr",
                            service_data=ep,
                        )
                    )

        titles.sort(key=lambda x: (x.season, x.episode))

        # Always fix duplicate episode numbers within same season (common with M6 multi-part episodes)
        # Group by season and renumber sequentially
        ep_counters = {}
        for t in titles:
            ep_counters.setdefault(t.season, 0)
            ep_counters[t.season] += 1
            t.episode = ep_counters[t.season]

        # Divertissement mode: renumber seasons sequentially (1, 2, 3...) 
        if self.divertissement and titles:
            from collections import OrderedDict
            season_info = OrderedDict()
            for t in titles:
                if t.season not in season_info:
                    season_info[t.season] = {"count": 0, "first_ep": t.episode_name}
                season_info[t.season]["count"] += 1

            season_map = {}
            seq = 0
            for orig_s in season_info:
                seq += 1
                season_map[orig_s] = seq

            self.log.info(f" + Divertissement: renumbering {len(season_map)} seasons:")
            for orig_s, new_s in season_map.items():
                info = season_info[orig_s]
                self.log.info(f"   S{orig_s:>5} â†’ S{new_s:02d} ({info['count']} eps) â€” {info['first_ep'][:60]}")

            for t in titles:
                t.season = season_map[t.season]

        return titles

    def _extract_season_folders(self, metadata, show_title=""):
        """Extract season folder entries from program layout. Returns list of {folder_id, season_num, label}."""
        seasons = []
        seen = set()
        show_lower = show_title.lower() if show_title else ""

        for block in metadata.get("blocks", []):
            title_text = block.get("title") or ""
            if not isinstance(title_text, str):
                title_text = str(title_text)
            content = block.get("content") or {}

            # Only consider blocks related to the show (title matches or contains "Saison")
            block_relevant = False
            if re.search(r'[Ss]aison\s*\d+', title_text):
                block_relevant = True
            elif show_lower and show_lower in title_text.lower():
                block_relevant = True
            elif not title_text:
                block_relevant = True  # untitled blocks might be season lists

            if not block_relevant:
                continue

            # Check if this block is a season block (title contains "Saison")
            season_match = re.search(r'[Ss]aison\s*(\d+)', title_text)

            # Look for folder action in the block header ("Voir tout" link)
            block_action = block.get("action") or {}
            block_target = block_action.get("target") or {}
            block_value = block_target.get("value_layout") or block_target.get("value") or {}

            if isinstance(block_value, dict):
                fid = block_value.get("id") or block_value.get("folderId")
                if fid and str(fid) not in seen and not str(fid).startswith("clip_"):
                    seen.add(str(fid))
                    snum = int(season_match.group(1)) if season_match else len(seasons) + 1
                    seasons.append({
                        "folder_id": str(fid),
                        "season_num": snum,
                        "label": title_text or f"Saison {snum}",
                    })
                    continue

            # Also check items for folder references (some layouts nest them)
            items = content.get("items") or []
            for item in items:
                item_content = item.get("itemContent") or {}
                action = item_content.get("action") or {}
                target = action.get("target") or {}
                value = target.get("value_layout") or target.get("value") or {}

                if isinstance(value, dict):
                    fid = value.get("id") or value.get("folderId")
                    if fid and str(fid) not in seen and not str(fid).startswith("clip_"):
                        item_title = item_content.get("title") or item.get("title") or ""
                        if not isinstance(item_title, str):
                            item_title = str(item_title)
                        sm = re.search(r'[Ss]aison\s*(\d+)', item_title)
                        if not sm:
                            sm = season_match
                        snum = int(sm.group(1)) if sm else len(seasons) + 1
                        seen.add(str(fid))
                        seasons.append({
                            "folder_id": str(fid),
                            "season_num": snum,
                            "label": item_title or title_text or f"Saison {snum}",
                        })

        # Fallback: search raw JSON for folder layout URLs (not clips)
        if not seasons:
            raw = json.dumps(metadata)
            for m in re.finditer(r'/folder/(\d+)/layout', raw):
                fid = m.group(1)
                if fid not in seen:
                    seen.add(fid)
                    seasons.append({
                        "folder_id": fid,
                        "season_num": len(seasons) + 1,
                        "label": f"Saison {len(seasons) + 1}",
                    })

        seasons.sort(key=lambda x: x["season_num"])
        return seasons

class M6_AUTH:
    def __init__(self, M6) -> None:
        self.device_id = uuid.uuid1().int
        self.authorization = self.authorize(M6)
        self.access_token = self.get_jwt(M6)
        self.profile_id = self.get_profiles(M6)
        self.access_token = self.get_jwt(M6)

    def authorize(self, M6):
        res = requests.post(
            url='https://login-gigya.m6.fr/accounts.login',
            data={
                "loginID": M6.credentials.username,
                "password": M6.credentials.password,
                "sessionExpiration": "0",
                "targetEnv": "jssdk",
                "include": "profile,data",
                "includeUserInfo": "true",
                "lang": "fr",
                "ApiKey": '3_hH5KBv25qZTd_sURpixbQW6a4OsiIzIEF2Ei_2H7TXTGLJb_1Hr4THKZianCQhWK',
                "authMode": "cookie",
                "pageURL": "https://www.m6.fr/",
                "sdkBuild": 16543,
                "format": "json",
            },
        ).json()

        if res.get("errorMessage"):
            log.exit(f"Could not authorize M6 account: {res['errorMessage']!r}")

        return res

    def get_jwt(self, M6):
        jwt_headers = {
            "x-auth-device-id": str(self.device_id),
            "x-auth-device-player-size-height": "3840",
            "x-auth-device-player-size-width": "2160",
            "X-Auth-gigya-signature": self.authorization["UIDSignature"],
            "X-Auth-gigya-signature-timestamp": self.authorization[
                "signatureTimestamp"
            ],
            "X-Auth-gigya-uid": self.authorization["UID"],
            "X-Client-Release": '6.2.5',
            "X-Customer-Name": "m6web",
        }

        if getattr(self, "profile_id", None):
            jwt_headers.update({"X-Auth-profile-id": self.profile_id})

        res = requests.get(
            url='https://front-auth.6cloud.fr/v2/platforms/{platform}/getJwt'.format(platform='m6group_android_tv'),
            headers=jwt_headers,
        ).json()

        if res.get("error"):
            log.exit(
                f"Could not get Access Token from M6: {res['error']['message']!r}"
            )

        return res["token"]

    def get_profiles(self, M6):
        res = requests.get(
            url="https://users.6cloud.fr/v2/platforms/{platform}/users/{gigya}/profiles".format(
                platform='m6group_android_tv', gigya=self.authorization["UID"]
            ),
            headers={"Authorization": f"Bearer {self.access_token}"},
        ).json()

        try:
            if res.get("error"):
                log.exit(
                    f"Could not get profiles from M6: {res['error']['message']!r}"
                )
        except AttributeError:
            pass

        return res[0]["uid"]