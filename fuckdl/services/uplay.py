from __future__ import annotations

import os
import re
import json
import time
import base64
import requests
import click
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from langcodes import Language

from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Tracks, TextTrack
from fuckdl.config import config
from fuckdl.parsers.mpd import parse as parse_mpd


class UPLAY(BaseService):
    """
    UltraPlay (https://www.ultraplay.in)
    DRM: Widevine L3
    Last Updated: 02-01-2026
    """

    ALIASES = ["UPLAY"]

    # -------------------------------------------------
    # CLI
    # -------------------------------------------------
    @staticmethod
    @click.command(name="UPLAY", short_help="https://www.ultraplay.in")
    @click.argument("title", type=str)
    @click.option("-m", "--movie", is_flag=True, help="Input is a movie URL.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return UPLAY(ctx, **kwargs)

    # -------------------------------------------------
    # INIT
    # -------------------------------------------------
    def __init__(self, ctx, title: str, movie: bool):
        # -------------------------------------------------
        # Normalize UltraPlay content ID FIRST
        # -------------------------------------------------
        self.title = title
        self.movie = movie
    
        if "ultra" in self.title:
            # Prefer numeric ID if present
            match = re.search(r"/(\d+)$", self.title)
            if match:
                self.title = match.group(1)
            else:
                # Fallback: last path segment
                match = re.search(r"/([^/]+)$", self.title)
                if match:
                    self.title = match.group(1)
                else:
                    raise ValueError("No UltraPlay Content ID found")
    
        # -------------------------------------------------
        # Call parent AFTER title is fixed
        # -------------------------------------------------
        super().__init__(ctx)
    
        assert ctx.parent is not None
    
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.range = ctx.parent.params["range_"]
    
        self.profile_id = ctx.obj.profile
    
        self.token: str
        self.license_api: Optional[str] = None

    # -------------------------------------------------
    # TITLES
    # -------------------------------------------------
    def get_titles(self):
        # -------------------------------------------------
        # Ensure token (use cache first)
        # -------------------------------------------------
        if not getattr(self, "token", None):
            try:
                self.token = self.get_token()
                self.session.headers["X-SESSION"] = self.token["success"]
            except Exception:
                self.authenticate()
    
        content_id = self.title
    
        res = self.session.get(
            f"https://vcms.mobiotics.com/prodv3/subscriber/v1/content/{content_id}",
            params={"displaylanguage": "eng"},
        )
    
        # Retry once if token expired
        if res.status_code == 401:
            self.log.info("Token expired, re-authenticating")
            self.authenticate()
            res = self.session.get(
                f"https://vcms.mobiotics.com/prodv3/subscriber/v1/content/{content_id}",
                params={"displaylanguage": "eng"},
            )
    
        try:
            data = res.json()
            #print(data)
        except Exception:
            raise ValueError(f"Failed to load title metadata: {res.text}")
    
        titles = []
    
        # -------------------------------------------------
        # MOVIE
        # -------------------------------------------------
        if self.movie:
            titles.append(
                Title(
                    content_id,
                    Title.Types.MOVIE,   # âœ… REQUIRED
                    data.get("title") or data.get("defaulttitle") or "Unknown Title",
                    year=int(data.get("productionyear") or 2025),
                    source=self.ALIASES[0],
                    service_data=data,
                )
            )
            return titles
    
        # -------------------------------------------------
        # SERIES
        # -------------------------------------------------
        series_id = data["objectid"]
        total_seasons = int(data.get("seasoncount", 0))
    
        for season in range(1, total_seasons + 1):
            eps = self.session.get(
                "https://vcms.mobiotics.com/prodv3/subscriber/v1/content",
                params={
                    "objecttype": "CONTENT",
                    "seriesid": series_id,
                    "seasonnum": season,
                    "pagesize": 50,
                    "page": 1,
                    "displaylanguage": "eng",
                },
            ).json()
    
            for ep in eps.get("data", []):
                titles.append(
                    Title(
                        ep["objectid"],
                        Title.Types.TV,      # âœ… REQUIRED
                        ep.get("seriesname") or "Unknown Series",
                        season=int(ep["seasonnum"]),
                        episode=int(ep["episodenum"]),
                        episode_name=ep.get("title") or f"Episode {ep.get('episodenum')}",
                        source=self.ALIASES[0],
                        service_data=ep,
                    )
                )
    
        return titles

    # -------------------------------------------------
    # TRACKS
    # -------------------------------------------------    
    def get_tracks(self, title: Title) -> Tracks:
        data = title.service_data
    
        # -------------------------------------------------
        # DASH package / availability
        # -------------------------------------------------
        dash = next(
            c for c in data["contentdetails"]
            if c.get("streamtype") == "DASH"
        )
    
        self.packageid = dash["packageid"]
        self.availabilityid = dash["availabilityset"][0]
        self.content_id = data["objectid"]
    
        # -------------------------------------------------
        # Request playback
        # -------------------------------------------------
        res = self.session.post(
            f"https://vcms.mobiotics.com/prodv3/subscriber/v1/content/package/{self.content_id}",
            data={
                "availabilityid": self.availabilityid,
                "packageid": self.packageid,
            },
        )
    
        try:
            payload = res.json()
            self.log.debug(json.dumps(payload, indent=2))
            mpd_url = payload["streamfilename"]
        except Exception:
            raise ValueError(f"Failed to load title manifest: {res.text}")
    
        # -------------------------------------------------
        # DRM token
        # -------------------------------------------------
        if dash.get("drmscheme", [None])[0] == "WIDEVINE":
            self.drm_token = self.get_drm_token()
    
        # -------------------------------------------------
        # Fetch MPD XML (session warm-up)
        # -------------------------------------------------
        mpd_res = self.session.get(mpd_url)
        mpd_res.raise_for_status()
    
        # -------------------------------------------------
        # Parse MPD
        # -------------------------------------------------
        tracks = parse_mpd(
            url=mpd_url,
            source=True,
            session=self.session,
        )
    
        # -------------------------------------------------
        # Language normalization
        # -------------------------------------------------
        lang = (
            getattr(title, "language", None)
            or (data.get("contentlanguage") or [None])[0]
            or "und"
        )
    
        for track in tracks.audios:
            if str(track.language) == "en" and str(lang) != "en":
                track.language = lang
    
        # -------------------------------------------------
        # Subtitle cleanup (FIXED)
        # -------------------------------------------------
        def is_blank_vtt(text: str) -> bool:
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line.startswith("WEBVTT"):
                    continue
                if line.startswith("NOTE"):
                    continue
                if "-->" in line:
                    return False
            return True
        
        
        valid_subs = []
        
        for sub in tracks.subtitles:
            try:
                r = self.session.get(
                    sub.url,
                    headers={"Accept": "text/vtt"},
                    timeout=10,
                )
        
                self.log.debug(f"Subtitle HTTP {r.status_code}")
                self.log.debug(r.text[:200])
        
                if r.status_code == 200 and is_blank_vtt(r.text):
                    self.log.info(" + Skipping blank subtitle")
                    self.log.info(sub.url)
                    continue
        
                # ðŸ”¥ FORCE codec assignment (because MPD has none)
                sub.codec = "vtt"
        
            except Exception as e:
                self.log.error(f"Subtitle fetch failed: {e}")
                continue
        
            valid_subs.append(sub)
        
        tracks.subtitles = valid_subs
    
        # -------------------------------------------------
        # Disable proxy requirement
        # -------------------------------------------------
        for track in tracks:
            track.needs_proxy = False
    
        # -------------------------------------------------
        # Cleanup stream session
        # -------------------------------------------------
        try:
            self.session.delete(
                f"https://vcms.mobiotics.com/prodv3/subscriber/v1/stream/content/{self.content_id}",
                data={"devicetype": "PC"},
            )
        except Exception:
            pass
    
        return tracks

    # -------------------------------------------------
    # CHAPTERS
    # -------------------------------------------------    
    def get_chapters(self, title: Title) -> List[Chapter]:
        """
        Build chapter markers from service skip metadata.
        """
        skips = (title.service_data or {}).get("skip")
        if not skips:
            return []
    
        chapters: List[Chapter] = []
        idx = 1
    
        # -------------------------------------------------
        # Always start at 00:00:00.000
        # -------------------------------------------------
        chapters.append(
            Chapter(
                number=idx,
                name="Start",
                timecode="00:00:00.000",
            )
        )
    
        # -------------------------------------------------
        # Skip-based chapters (Intro / Recap / Credits etc.)
        # -------------------------------------------------
        for skip in skips:
            skip_type = skip.get("skiptype")
            if not skip_type:
                continue
    
            name = skip_type.title()
    
            start = int(skip.get("start") or 0)
            if start > 0:
                idx += 1
                chapters.append(
                    Chapter(
                        number=idx,
                        name=f"{name} Start",
                        timecode=datetime.utcfromtimestamp(start).strftime("%H:%M:%S.%f")[:-3],
                    )
                )
    
            end = int(skip.get("end") or 0)
            if end > 0:
                idx += 1
                chapters.append(
                    Chapter(
                        number=idx,
                        name=f"{name} End",
                        timecode=datetime.utcfromtimestamp(end).strftime("%H:%M:%S.%f")[:-3],
                    )
                )
    
        return chapters


    # -------------------------------------------------
    # DRM
    # -------------------------------------------------
    def certificate(self, **_):
        return "CAUSxwUKwQIIAxIQFwW5F8wSBIaLBjM6L3cqjBiCtIKSBSKOAjCCAQoCggEBAJntWzsyfateJO/DtiqVtZhSCtW8yzdQPgZFuBTYdrjfQFEEQa2M462xG7iMTnJaXkqeB5UpHVhYQCOn4a8OOKkSeTkwCGELbxWMh4x+Ib/7/up34QGeHleB6KRfRiY9FOYOgFioYHrc4E+shFexN6jWfM3rM3BdmDoh+07svUoQykdJDKR+ql1DghjduvHK3jOS8T1v+2RC/THhv0CwxgTRxLpMlSCkv5fuvWCSmvzu9Vu69WTi0Ods18Vcc6CCuZYSC4NZ7c4kcHCCaA1vZ8bYLErF8xNEkKdO7DevSy8BDFnoKEPiWC8La59dsPxebt9k+9MItHEbzxJQAZyfWgkCAwEAAToUbGljZW5zZS53aWRldmluZS5jb20SgAOuNHMUtag1KX8nE4j7e7jLUnfSSYI83dHaMLkzOVEes8y96gS5RLknwSE0bv296snUE5F+bsF2oQQ4RgpQO8GVK5uk5M4PxL/CCpgIqq9L/NGcHc/N9XTMrCjRtBBBbPneiAQwHL2zNMr80NQJeEI6ZC5UYT3wr8+WykqSSdhV5Cs6cD7xdn9qm9Nta/gr52u/DLpP3lnSq8x2/rZCR7hcQx+8pSJmthn8NpeVQ/ypy727+voOGlXnVaPHvOZV+WRvWCq5z3CqCLl5+Gf2Ogsrf9s2LFvE7NVV2FvKqcWTw4PIV9Sdqrd+QLeFHd/SSZiAjjWyWOddeOrAyhb3BHMEwg2T7eTo/xxvF+YkPj89qPwXCYcOxF+6gjomPwzvofcJOxkJkoMmMzcFBDopvab5tDQsyN9UPLGhGC98X/8z8QSQ+spbJTYLdgFenFoGq47gLwDS6NWYYQSqzE3Udf2W7pzk4ybyG4PHBYV3s4cyzdq8amvtE/sNSdOKReuHpfQ="

    def license(self, *, challenge, **_: Any) -> str:
        # -------------------------------------------------
        # Normalize challenge (bytes â†’ base64 string)
        # -------------------------------------------------
        if isinstance(challenge, str):
            chal = challenge
        else:
            chal = base64.b64encode(challenge).decode("utf-8")
    
        # -------------------------------------------------
        # License request payload
        # -------------------------------------------------
        json_data = {
            "payload": chal,
            "contentid": self.content_id,
            "providerid": "ultrahin",
            "timestamp": int(time.time()),
            "drmscheme": "WIDEVINE",
            "customdata": {
                "packageid": self.packageid,
                "drmtoken": self.drm_token,
            },
        }
    
        # -------------------------------------------------
        # License request
        # -------------------------------------------------
        res = self.session.post(
            "https://vdrm.mobiotics.com/prod/proxy/v1/license/widevine",
            json=json_data,
            headers={
                "authority": "vdrm.mobiotics.com",
                "accept": "*/*",
                "content-type": "application/json",
                "dnt": "1",
                "origin": "https://www.ultraplay.in",
                "referer": "https://www.ultraplay.in/",
                "user-agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/138.0.0.0 Safari/537.36"
                ),
                "X-SESSION": self.token["success"],
            },
        )
    
        data = res.json()
        return data["body"]

    # -------------------------------------------------
    # AUTH
    # -------------------------------------------------
    def authenticate(self, **_):
        if not self.credentials:
            self.log.exit("No credentials provided")

        self.session.headers.update({
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "origin": "https://www.ultraplay.in",
            "referer": "https://www.ultraplay.in/",
        })

        self.token = self.get_token()
        self.session.headers["X-SESSION"] = self.token["success"]

        self.profile_id = self.get_profile_id()
        self.activate_profile()

    # -------------------------------------------------
    # TOKEN / PROFILE
    # -------------------------------------------------
    def get_token(self) -> dict:
        """
        Load cached UltraPlay token or perform login.
        Cache is profile-aware when profile_id is available.
        """
    
        profile_key = self.profile_id or "default"
    
        token_cache_path = self.get_cache(
            f"token_{profile_key}.json"
        )
    
        # -------------------------------------------------
        # Use cached token
        # -------------------------------------------------
        if os.path.isfile(token_cache_path):
            try:
                with open(token_cache_path, encoding="utf-8") as fd:
                    token = json.load(fd)
    
                if token.get("refreshtoken"):
                    self.log.info("Refreshing cached UltraPlay token")
                    token = self.refresh(token["refreshtoken"])
                    self.save_token(token, token_cache_path)
                else:
                    self.log.info("Using cached UltraPlay token")
    
                return token
    
            except Exception as e:
                self.log.warning(f"Cached token invalid, re-login required: {e}")
    
        # -------------------------------------------------
        # Fresh login
        # -------------------------------------------------
        self.log.info("Performing fresh UltraPlay login")
        token = self.login()
    
        self.save_token(token, token_cache_path)
        return token

    def save_token(self, token: dict, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(token, fd)

    def get_profile_id(self) -> str:
        return self.session.get(
            "https://vsms.mobiotics.com/prodv3/subscriberv2/v1/subscriber"
        ).json()["profileid"]

    def activate_profile(self):
        self.session.put(
            f"https://vsms.mobiotics.com/prodv3/subscriberv2/v1/profile/{self.profile_id}"
        )

    def get_drm_token(self) -> str:
        self.session.post(
            f"https://vcms.mobiotics.com/prodv3/subscriber/v1/stream/content/{self.content_id}",
            data={"devicetype": "PC"},
        )

        res = self.session.post(
            "https://vcms.mobiotics.com/prodv3/subscriber/v1/content/drmtoken",
            data={
                "contentid": self.content_id,
                "packageid": self.packageid,
                "availabilityid": self.availabilityid,
                "drmscheme": "WIDEVINE",
                "seclevel": "SW",
            },
        ).json()

        return res["success"]

    def refresh(self, refresh_token: str) -> dict:
        res = self.session.get(
            "https://vsms.mobiotics.com/prodv3/subscriberv2/v1/refreshtoken",
            headers={
                "authorization": f"Bearer {refresh_token}",
                "origin": "https://www.ultraplay.in",
                "referer": "https://www.ultraplay.in/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            },
        )

        if res.status_code == 403:
            self.log.exit("UltraPlay refresh blocked (403)")

        return res.json()

    def login(self) -> dict:
        if not self.credentials:
            self.log.exit("No credentials provided")

        device = self.session.post(
            "https://vsms.mobiotics.com/prodv3/subscriberv2/v1/device/register/ultrahin",
            params={"hash": "3c0c17e81571aef74d63e64aca03fad0209e1fc2"},
            data="k5IZnYemrJ/4AmZnPIjgvyTtHoKQanNalTm8IKYbyHei7sjjrmJKRsWTZG9QPGumFQdJ3w2NwkU1mUQAKM+g6tfc6MPhC6N1nvknzxR+bVRnW+ue4KDLtkDJo50hRuQsaubGqJK1r8w9fLOHhh/luN38K/fNhqlPfy33lepG/7BwvibWzz2evhYn5ZhQl4A5",
            headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"},
        ).json()

        if device.get("errorcode") == 8601:
            self.log.exit(device.get("reason"))

        login = self.session.get(
            "https://vsms.mobiotics.com/prodv3/subscriberv2/v1/login",
            params={
                "email": self.credentials.username,
                "password": self.credentials.password,
                "devicetype": "PC",
                "deviceos": "WINDOWS",
                "country": "IN",
            },
            headers={
                "authorization": f"Bearer {device['success']}",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            },
        )

        if login.status_code != 200:
            self.log.exit(f"Login failed: {login.text}")

        return login.json()

    # -------------------------------------------------
    # HELPERS
    # -------------------------------------------------
    def _norm_lang(self, lang) -> Language:
        if isinstance(lang, list) and lang:
            return Language.get(lang[0])
        if isinstance(lang, str):
            return Language.get(lang)
        return Language.get("und")

    def _ts(self, sec: int) -> str:
        return datetime.utcfromtimestamp(int(sec)).strftime("%H:%M:%S.%f")[:-3]

    def _is_blank_sub(self, url: str) -> bool:
        try:
            return requests.get(url, timeout=5).text.strip() == "WEBVTT"
        except Exception:
            return False
