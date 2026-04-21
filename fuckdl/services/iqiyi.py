import atexit
import base64
import hashlib
import json
import re
import time
import urllib.parse
from pathlib import Path
from typing import List, Optional, Dict, Tuple, Any, Union
import click
import requests
from bs4 import BeautifulSoup
from langcodes import Language
from fuckdl.services.BaseService import BaseService
from fuckdl.objects import Title, Tracks, VideoTrack, AudioTrack, TextTrack, MenuTrack
from fuckdl.objects.tracks import Track
from fuckdl.utils import try_get


class iQIYI(BaseService):
    """
    Service implementation for iQIYI streaming platform (iq.com).
    Merged & Updated from Unshackle service (CodeName393 / Hugov / sp4rk.y)
    
    Added by @AnotherBigUserHere
    Exclusive for Fuckdl
    Copyright AnotherBigUserHere 2026
    """
    ALIASES = ["iQIYI", "iqiyi", "iq", "IQ"]
    TITLE_RE = [r"^(?:https?://(?:www\.)?iq\.com/(?:play|album)/)?(?P<id>[^/?]+)(?:\?.*)?$"]

    ORIG_LANG_MAP = {
        "Mandarin": "zh-Hans", "Cantonese": "zh-Hant", "English": "en",
        "Korean": "ko", "Japanese": "ja", "Thai": "th", "Vietnamese": "vi",
        "Indonesian": "id", "Malay": "ms", "Spanish": "es-419",
        "Portuguese": "pt-BR", "Arabic": "ar", "French": "fr", "German": "de",
    }

    LANG_MAP = {1: "zh-Hans", 2: "zh-Hant", 3: "en", 5: "ko", 143: "pt-BR", 157: "th", 161: "vi"}
    SUB_LANG_MAP = {
        1: "zh-Hans", 2: "zh-Hant", 3: "en", 4: "ko", 5: "ja", 6: "fr",
        18: "th", 21: "ms", 23: "vi", 24: "id", 26: "es-419", 27: "pt-BR", 28: "ar", 30: "de",
    }

    BID_QUALITY = {
        4320: ["1020"], 2160: ["860", "800"], 1080: ["650", "600"],
        720: ["500"], 480: ["300"], 360: ["200"],
    }

    @staticmethod
    @click.command(name="iQIYI", short_help="https://iq.com", help=__doc__)
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx: click.Context, **kwargs: Any) -> "iQIYI":
        return iQIYI(ctx, **kwargs)

    def __init__(self, ctx, *args, **kwargs):
        title = kwargs.pop('title', None)
        super().__init__(ctx, *args, **kwargs)

        # Ruta al CDM de MonaLisa
        self.cdm_path = Path(__file__).parent.parent / "devices" / "monalisa.mld"
        self.title_id = title
        if title:
            for pattern in self.TITLE_RE:
                match = re.match(pattern, title)
                if match:
                    self.title_id = match.group("id")
                    break

        self.active_session: Dict[str, Any] = {
            "uid": "0", "qc005": "", "pck": "", "dfp": "",
            "type_code": "", "mode_code": "", "lang_code": "en_us",
            "ptid": "", "is_vip": False,
        }

        quality_param = ctx.parent.params.get("quality")
        self.quality: List[int] = [quality_param] if quality_param is not None and not isinstance(quality_param, list) else (quality_param or [1080])

        self.vcodec = ctx.parent.params.get("vcodec")
        self.acodec = ctx.parent.params.get("acodec")
        self.video_only = ctx.parent.params.get("video_only", False)
        self.audio_only = ctx.parent.params.get("audio_only", False)
        self.subs_only = ctx.parent.params.get("subs_only", False)
        self.chapters_only = ctx.parent.params.get("chapters_only", False)
        self.list_ = ctx.parent.params.get("list_", False)

        self.session.headers.update({
            "User-Agent": self.config["device"]["user_agent"],
            "Referer": "https://www.iq.com/",
            "Origin": "https://www.iq.com",
        })

        self._preferred_video_codec = "h265" if self.vcodec == "H265" else "h264"
        self._preferred_audio_codec = (self.acodec or "dolby").lower()
        self._current_title_data: Dict[str, Any] = {}
        self._temp_files: List[Path] = []
        atexit.register(self._cleanup_temp_files)

    def _cleanup_temp_files(self) -> None:
        for f in self._temp_files:
            if f.exists():
                try: f.unlink()
                except Exception: pass

    def authenticate(self, cookies, credential) -> None:
        if not cookies:
            raise EnvironmentError("iQIYI requires cookies for authentication.")
        self.cookies = cookies
        self.credentials = credential
        self.log.info("Authenticating with iQIYI...")
        self._login()
        self.log.info(f"  Account ID: {self.active_session.get('uid', 'N/A')}")
        self.log.info(f"  Region: {self.active_session.get('mode_code', 'N/A')}")
        self.log.info(f"  VIP Status: {self.active_session.get('is_vip', False)}")

    def _login(self) -> None:
        cookie_dict = {c.name: c.value for c in self.cookies} if self.cookies else {}
        self.active_session.update({
            "uid": cookie_dict.get("pspStatusUid", "0"),
            "qc005": cookie_dict.get("QC005", ""),
            "pck": cookie_dict.get("I00001", ""),
            "dfp": cookie_dict.get("__dfp", "").split("@")[0] if cookie_dict.get("__dfp") else "",
            "type_code": cookie_dict.get("QCVtype", ""),
            "mode_code": cookie_dict.get("mod", ""),
            "lang_code": cookie_dict.get("lang", "en_us"),
        })
        required = ["uid", "qc005", "type_code"]
        missing = [k for k in required if not self.active_session.get(k)]
        if missing:
            raise EnvironmentError(f"Missing required cookies: {missing}")
        if not self.active_session["pck"]:
            self.log.debug("Fetching PCK token...")
            self.active_session["pck"] = self._fetch_pck()
        if not self.active_session["mode_code"]:
            self.active_session["mode_code"] = self._get_mode_code()
        self.active_session["ptid"] = self._get_ptid()
        self.active_session["is_vip"] = self._check_vip_status()

    def _check_vip_status(self) -> bool:
        """Check VIP status - exact copy of Unshackle logic"""
        if not self.active_session.get("lang_code"):
            return False
        
        params = {
            "platformId": self.config["device"]["platform_id"],
            "modeCode": self.active_session["mode_code"],
            "langCode": self.active_session["lang_code"],
            "deviceId": self.active_session["qc005"],
            "fields": "userinfo",
            "version": "1.0",
            "vipInfoVersion": "5.0",
        }
        headers = {
            "User-Agent": self.config["device"]["user_agent_bws"],
            "Cookie": "; ".join(f"{c.name}={c.value}" for c in self.cookies),
        }
        
        try:
            data = self._request("GET", self.config["endpoint"]["vip"], params=params, headers=headers)
            
            if data.get("code") == "0" and "userinfo" in data.get("data", {}):
                vip_list = data["data"].get("vip_list", [])
                return any(str(vip.get("status")) == "1" for vip in vip_list)
            elif data.get("code") != "0":
                self.log.error(f"VIP check failed: {data.get('data', {}).get('msg', 'Unknown error')}")
        except Exception as e:
            self.log.error(f"VIP check exception: {e}")
        return False

    def get_titles(self) -> List[Title]:
        if self.active_session.get("uid") == "0" or not self.active_session.get("qc005"):
            if self.cookies: self.authenticate(self.cookies, self.credentials)
            else: raise EnvironmentError("No cookies available for authentication.")

        lang_code = self.active_session.get("lang_code", "en_us")
        video_info, lang_info, season_info = self._get_album_info(self.title_id, lang_code)

        if not video_info or not any(k in video_info for k in ["albumId", "qipuId", "tvId", "name"]):
            self.log.warning(f"Page parsing returned insufficient data. Keys: {list(video_info.keys()) if video_info else 'empty'}")
            album_id = self.title_id
            test_resp = self._get_series_episodes(album_id, 1, 1)
            if test_resp and "data" in test_resp and test_resp["data"].get("total", 0) > 0:
                video_info = {"albumId": album_id, "qipuId": album_id, "name": self.title_id, "videoType": "juji"}
            else:
                raise ValueError(f"Could not extract or validate album ID from '{self.title_id}'")

        album_id = self._extract_id(video_info, ["albumId", "album_id", "albumID"])
        qipu_id = self._extract_id(video_info, ["qipuId", "qipu_id", "tvId", "tvid"])

        if not album_id and self.title_id and re.match(r'^\d+$', self.title_id):
            album_id = self.title_id
            self.log.warning(f"Using title_id as album_id: {album_id}")
        if not qipu_id: qipu_id = album_id
        if not album_id: raise ValueError("Could not extract valid album ID")

        name = video_info.get("name") or video_info.get("albumName") or self.title_id
        year = str(video_info.get("publishTime") or "")[:4]
        content_type = str(video_info.get("videoType") or video_info.get("contentType") or " ").lower().strip()

        try:
            video_info_en, _, _ = self._get_album_info(self.title_id, "en_us")
            orig_lang_name = try_get(video_info_en, lambda x: x["categoryTagMap"]["Language"][0]["name"])
        except:
            orig_lang_name = "Mandarin"
        original_lang = Language.get(self.ORIG_LANG_MAP.get(orig_lang_name or "Mandarin", orig_lang_name))

        if not content_type:
            episode_count = video_info.get("episodeCount") or video_info.get("total") or 0
            content_type = "juji" if episode_count > 1 or season_info else "singlevideo"

        if content_type == "singlevideo":
            return [Title(
                id_=qipu_id, type_=Title.Types.MOVIE, name=name, year=year,
                original_lang=original_lang, source=self.ALIASES[0],
                service_data={"tvid": qipu_id, "orig_lang": original_lang}
            )]

        elif content_type in ("juji", "laiyuan", "series", "tv"):
            titles = []
            seen_episodes = set()
            seasons_data = self._get_seasons(season_info, album_id)
            if not seasons_data:
                seasons_data = [{"albumid": album_id, "name": name, "year": year, "season": 1}]

            for season in seasons_data:
                season_album_id = season.get("albumid") or album_id
                episodes = self._get_episodes(season_album_id, season["season"], content_type)
                if not episodes:
                    self.log.warning(f"No episodes found for season {season['season']}")
                    continue
                for ep in episodes:
                    ep_key = (ep["tvid"], ep["season"], ep["number"])
                    if ep_key not in seen_episodes:
                        seen_episodes.add(ep_key)
                        titles.append(Title(
                            id_=ep["tvid"], type_=Title.Types.TV, name=season["name"],
                            year=season["year"], season=ep["season"], episode=ep["number"],
                            episode_name=ep["name"], original_lang=original_lang,
                            source=self.ALIASES[0], service_data={"tvid": ep["tvid"], "orig_lang": original_lang}
                        ))
            return titles

        raise ValueError(f"Unsupported content type: '{content_type}'")

    def _extract_id(self, data: dict, keys: List[str]) -> Optional[str]:
        for key in keys:
            value = data.get(key)
            if value and str(value).strip() not in ("None", ""):
                return str(value).strip()
        return None

    def _get_album_info(self, title_id: str, lang_code: str) -> Tuple[dict, dict, dict]:
        """Get album info - Match Unshackle parsing logic"""
        endpoint = self.config["endpoint"]["album"].format(id=title_id, lang_code=lang_code)
        
        cookies_dict = {c.name: c.value for c in self.cookies} if self.cookies else {}
        cookies_dict["lang"] = lang_code
        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies_dict.items())
        
        headers = {
            "User-Agent": self.config["device"]["user_agent_bws"],
            "Cookie": cookie_header,
        }
    
        try:
            res = self.session.get(endpoint, headers=headers, timeout=30)
            res.raise_for_status()
            
            soup = BeautifulSoup(res.text, "html.parser")
            script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
            
            if not script_tag:
                self.log.error("Failed to find __NEXT_DATA__ script tag")
                return {}, {}, {}
    
            data = json.loads(script_tag.string)
            initial_state = data.get("props", {}).get("initialState", {})
            
            video_info = initial_state.get("play", {}).get("videoInfo")
            if not video_info or not video_info.get("qipuId"):
                video_info = initial_state.get("album", {}).get("videoAlbumInfo")
            
            lang_info = initial_state.get("language", {}).get("langPkg", {})
            season_info = initial_state.get("album", {}).get("superSeriesInfo", {})
            
            if not video_info or not any(k in video_info for k in ["albumId", "qipuId", "name"]):
                self.log.warning(f"Could not parse video info, using fallback for: {title_id}")
                return {"albumId": title_id, "qipuId": title_id, "name": title_id, "videoType": "juji"}, lang_info, season_info
            
            return video_info, lang_info, season_info
            
        except Exception as e:
            self.log.error(f"Failed to fetch album info: {e}")
            return {"albumId": title_id, "qipuId": title_id, "name": title_id}, {}, {}

    def _get_seasons(self, season_info: dict, album_id: str) -> List[dict]:
        seasons = []
        if season_info and "diffSeasons" in season_info:
            epg_list = season_info["diffSeasons"].get("epg", [])
            for season_data in sorted(epg_list, key=lambda x: x.get("season", 0)):
                seasons.append({
                    "albumid": str(season_data.get("qipuId") or album_id),
                    "name": season_data.get("name", f"Season {season_data.get('season', 1)}"),
                    "year": str(season_data.get("publishTime") or "")[:4],
                    "season": season_data.get("season", 1),
                })
        return seasons

    def _get_episodes(self, album_id: str, season_num: int, content_type: str) -> List[dict]:
        episodes = []
        block_size = 50
        album_id_str = str(album_id).strip()
        if not album_id_str: return []

        if content_type == "juji":
            first_batch = self._get_series_episodes(album_id_str, 1, block_size)
            if not first_batch or "data" not in first_batch: return []
            total_count = first_batch["data"].get("total", 0)
            batches = [first_batch]
            for start in range(block_size + 1, total_count + 1, block_size):
                end = min(start + block_size - 1, total_count)
                batches.append(self._get_series_episodes(album_id_str, start, end))

            special_count, regular_count = 0, 0
            for batch in batches:
                for ep in batch.get("data", {}).get("epg", []):
                    is_special = ep.get("contentType", 1) != 1
                    if is_special:
                        special_count += 1
                        ep_season, ep_number = 0, special_count
                        ep_name = ep.get("extraName") or f"Special Clip {special_count}"
                    else:
                        regular_count += 1
                        ep_season, ep_number = season_num, regular_count
                        ep_name = f"Episode {regular_count}"
                    episodes.append({"tvid": str(ep["qipuId"]), "season": ep_season, "number": ep_number, "name": ep_name})
        else:
            page = 1
            while True:
                batch = self._get_reality_episode_list(album_id_str, block_size, page)
                if not batch or "data" not in batch: break
                for ep in batch.get("data", {}).get("epg", []):
                    is_special = ep.get("contentType", 1) != 1
                    ep_season = 0 if is_special else season_num
                    episodes.append({
                        "tvid": str(ep.get("episodeId", "")),
                        "season": ep_season,
                        "number": len([e for e in episodes if e["season"] == ep_season]) + 1,
                        "name": ep.get("shortName") if is_special else f"Episode {len(episodes)+1}",
                    })
                if len(batch.get("data", {}).get("epg", [])) < block_size: break
                page += 1
        return episodes

    def get_tracks(self, title: Title) -> Tracks:
        if self.active_session.get("uid") == "0" or not self.active_session.get("qc005"):
            if self.cookies: self.authenticate(self.cookies, self.credentials)
            else: raise EnvironmentError("No cookies available for authentication.")

        self.log.debug(f"Getting tracks for TVID: {title.id}")
        self._current_title_data = {"has_external_audio": False, "chapters": {}, "highlights": []}

        videos, audios, subs = self._collect_media_sources(title.id)
        if not videos and not (self.audio_only or self.subs_only or self.chapters_only):
            self.log.error("No playable streams found.")
            return Tracks()

        tracks = Tracks()
        for video_data in videos:
            track = self._parse_video_track(video_data, title)
            if track: tracks.add(track)
        for audio_data in audios:
            track = self._parse_audio_track(audio_data, title)
            if track:
                tracks.add(track)
                self._current_title_data["has_external_audio"] = True
        for sub_data in subs:
            track = self._parse_subtitle_track(sub_data, title)
            if track: tracks.add(track)
        return tracks

    def _collect_media_sources(self, tvid: str) -> Tuple[List[dict], List[dict], List[dict]]:
        all_videos, all_audios, all_subs = [], [], []
        target_bids = []
        available_qualities = sorted(self.BID_QUALITY.keys())

        for q in self.quality:
            if q in self.BID_QUALITY:
                target_bids.extend(self.BID_QUALITY[q])
            else:
                closest_q = min(available_qualities, key=lambda x: abs(x - q))
                target_bids.extend(self.BID_QUALITY[closest_q])
        target_bids = list(set(target_bids))

        target_codecs = [self._preferred_video_codec]
        if self._preferred_video_codec == "h265":
            target_codecs.extend(["h265_edr", "dv_edr", "dv", "hdr_edr", "hdr"])
        if any(q >= 4320 for q in self.quality):
            target_codecs.append("8k")

        for codec_key in target_codecs:
            for bid in target_bids:
                try:
                    v_list, a_list, s_list = self._get_media_data(tvid, codec_key, bid)
                    if v_list: all_videos.extend(v_list)
                    if a_list and not self.video_only: all_audios.extend(a_list)
                    if s_list: all_subs.extend(s_list)
                except Exception: continue

        if not self.video_only and not all_audios:
            try:
                _, a_list, _ = self._get_media_data(tvid, "h264", "600")
                all_audios.extend(a_list)
            except Exception: pass

        video_map = {}
        for v in all_videos:
            if v.get("url") or v.get("fs") or v.get("m3u8"):
                key = (v.get("bid"), v.get("lid"), v.get("scrsz"), v.get("code"), v.get("dr"))
                if key not in video_map: video_map[key] = v

        audio_map = {}
        for a in all_audios:
            if a.get("m3u8Url") or a.get("url") or a.get("mpdUrl") or a.get("fs"):
                key = (a.get("bid"), a.get("lid"), a.get("cf"))
                if key not in audio_map: audio_map[key] = a

        sub_map = {}
        for s in all_subs:
            if s.get("webvtt") or s.get("xml") or s.get("srt"):
                if s.get("lid") not in sub_map: sub_map[s["lid"]] = s

        return list(video_map.values()), list(audio_map.values()), list(sub_map.values())

    def _parse_video_track(self, video_data: dict, title: Title) -> Optional[VideoTrack]:
        url = video_data.get("url") or video_data.get("fs") or video_data.get("m3u8")
        if not url: return None

        scrsz = video_data.get("scrsz", "")
        width, height = 0, 0
        if "x" in scrsz:
            try: width, height = map(int, scrsz.split("x"))
            except: pass

        duration = video_data.get("duration", 0)
        vsize = video_data.get("vsize", 0)
        bitrate = int((vsize / duration * 8) / 1000 * 1024) if duration and vsize else 0
        fps = float(video_data.get("fr", 0)) or 23.976
        codec = "h265" if video_data.get("code", 2) != 2 else "h264"
        dr = video_data.get("dr", 0)
        hdr10 = dr == 2
        dv = dr == 1
        lang_code = self.LANG_MAP.get(video_data.get("lid", 1), "zh-Hans")

        track_url = url
        from_file = None
        if not str(url).strip().startswith("http"):
            from_file = self._save_temp_m3u8(url, f"vid_{video_data.get('bid')}_{video_data.get('lid')}")
            if from_file: track_url = str(from_file.as_uri())

        track = VideoTrack(
            id_=f"video_{video_data.get('bid','')}_{video_data.get('lid','')}",
            source=self.ALIASES[0],
            url=track_url,
            codec=codec,
            language=Language.get(lang_code),
            bitrate=bitrate,
            width=width,
            height=height,
            fps=fps,
            hdr10=hdr10,
            dv=dv,
            descriptor=Track.Descriptor.M3U,
            encrypted=True,
        )
        
        # DRM attachment with MonaLisa (sin descifrado en tiempo real)
        drm_info = video_data.get("drm", {})
        if ticket := drm_info.get("ticket"):
            try:
                from fuckdl.utils.monalisa import MonaLisa
                device_path = str(self.cdm_path) if self.cdm_path.exists() else None
                
                if not hasattr(track, 'drm_objects'):
                    track.drm_objects = []
                    
                drm = MonaLisa(
                    ticket=ticket,
                    aes_key=self.config["key"]["ml"],
                    device_path=device_path,
                )
                track.drm_objects.append(drm)
                
                # Guardar key y kid para uso externo (sin descifrado automÃ¡tico)
                if drm.key and drm.kid:
                    track.key = drm.key
                    track.kid = drm.kid.hex if hasattr(drm.kid, 'hex') else str(drm.kid)
                    track.monalisa = True
                    track.mls_pssh = ticket
                    self.log.info(f"âœ“ MonaLisa video key obtained: KID={track.kid[:8]}...")
                else:
                    self.log.warning(f"MonaLisa returned no key for ticket: {ticket[:32]}...")
            except Exception as e:
                self.log.warning(f"MonaLisa attach failed: {e}")
        
        return track

    def _parse_audio_track(self, audio_data: dict, title: Title) -> Optional[AudioTrack]:
        url = audio_data.get("m3u8Url") or audio_data.get("url") or audio_data.get("mpdUrl")
        fs = audio_data.get("fs")
        
        if not url and not fs:
            self.log.debug(f"Audio track has no URL or segments: {audio_data.get('lid')}")
            return None

        lang_code = self.LANG_MAP.get(audio_data.get("lid", 1), "und")
        cf = audio_data.get("cf", "")
        ct = audio_data.get("ct", 0)
        codec = "eac3" if cf == "dolby" else "aac"
        channels = 5.1 if cf == "dolby" and ct in (2, 4) else 2.0
        atmos = cf == "dolby" and ct == 4

        from_file = None
        track_url = url
        
        if fs and not url:
            from_file = self._stitch_audio_segments(audio_data, f"aud_{audio_data.get('bid')}_{audio_data.get('lid')}")
            if not from_file:
                self.log.warning(f"Failed to stitch audio segments for lid={audio_data.get('lid')}")
                return None
            track_url = from_file.as_uri()
            
        elif url and not str(url).strip().startswith("http"):
            from_file = self._save_temp_m3u8(url, f"aud_{audio_data.get('bid')}_{audio_data.get('lid')}")
            if from_file:
                track_url = from_file.as_uri()

        track = AudioTrack(
            id_=f"audio_{audio_data.get('bid','')}_{audio_data.get('lid','')}",
            source=self.ALIASES[0],
            url=track_url,
            codec=codec,
            language=Language.get(lang_code),
            bitrate=0,
            channels=channels,
            descriptive=False,
            atmos=atmos,
            descriptor=Track.Descriptor.M3U,
            encrypted=True,
        )

        drm_info = audio_data.get("drm", {})
        if ticket := drm_info.get("ticket"):
            try:
                from fuckdl.utils.monalisa import MonaLisa
                device_path = str(self.cdm_path) if self.cdm_path.exists() else None
                
                drm = MonaLisa(
                    ticket=ticket,
                    aes_key=self.config.get("key", {}).get("ml", ""),
                    device_path=device_path,
                )
                track.drm_objects.append(drm)
                
                if drm.key and drm.kid:
                    track.key = drm.key
                    track.kid = drm.kid.hex if hasattr(drm.kid, 'hex') else str(drm.kid)
                    track.monalisa = True
                    self.log.info(f"âœ“ MonaLisa audio key obtained: KID={track.kid[:8]}...")
                else:
                    self.log.warning(f"MonaLisa audio returned no key for ticket: {ticket[:32]}...")
            except Exception as e:
                self.log.warning(f"MonaLisa audio DRM attach failed: {e}")
        
        return track

    def _parse_subtitle_track(self, sub_data: dict, title: Title) -> Optional[TextTrack]:
        url = sub_data.get("webvtt") or sub_data.get("xml") or sub_data.get("srt")
        if not url: return None
        lang_code = self.SUB_LANG_MAP.get(sub_data.get("lid", 3), "und")
        name = sub_data.get("_name", "").lower()
        forced = "forced" in name
        sdh = "sdh" in name
        return TextTrack(
            id_=f"sub_{sub_data.get('lid','')}", source=self.ALIASES[0],
            url=self.config["endpoint"]["subtitle"].format(path=url),
            codec="vtt", language=Language.get(lang_code),
            forced=forced, sdh=sdh, descriptor=Track.Descriptor.URL, encrypted=False
        )

    def get_chapters(self, title: Title) -> List[MenuTrack]:
        chapters = []
        chapter_data = self._current_title_data.get("chapters", {})
        highlight_data = self._current_title_data.get("highlights", [])
        intro_end = max(0, chapter_data.get("bt", 0))
        credits_start = max(0, chapter_data.get("et", 0))

        pre_chapter = []
        if intro_end > 1:
            pre_chapter.append(("Intro", 0))
            pre_chapter.append(("Scene", intro_end))
        else:
            pre_chapter.append(("Scene", 0))

        if highlight_data:
            for group in highlight_data:
                for v in group.get("vl", []):
                    sp = v.get("sp", 0)
                    if sp > intro_end and (credits_start == 0 or sp < credits_start):
                        pre_chapter.append(("Scene", sp))
                        break
        if credits_start > 0:
            pre_chapter.append(("Credits", credits_start))

        seen_times = set()
        for name, time_sec in sorted(pre_chapter, key=lambda x: x[1]):
            if time_sec not in seen_times and time_sec > 0:
                chapters.append(MenuTrack(number=len(chapters)+1, title=name, timecode=self._format_timecode(time_sec)))
                seen_times.add(time_sec)
        return chapters

    def license(self, challenge: bytes, **_) -> Optional[bytes]: return None
    def certificate(self, challenge: bytes, **_) -> Optional[bytes]: return None

    def _save_temp_m3u8(self, content: str, prefix: str) -> Optional[Path]:
        try:
            temp_path = self.config.directories.temp / f"{prefix}.m3u8"
            content_to_save = "\n".join(line for line in content.splitlines() if "#EM" not in line and line.strip() != "#EXT-X-DISCONTINUITY")
            temp_path.write_text(content_to_save, encoding="utf-8")
            self._temp_files.append(temp_path)
            return temp_path
        except Exception: return None

    def _stitch_audio_segments(self, audio: dict, audio_id: str) -> Optional[Path]:
        try:
            segments = audio.get("fs", [])
            if not segments:
                return None
                
            m3u8_lines = ["#EXTM3U", "#EXT-X-VERSION:3", "#EXT-X-TARGETDURATION:6"]
            resolved_count = 0
            
            for seg in segments:
                api_path = seg.get("l")
                if not api_path:
                    continue
                    
                seg_data = self._get_audio_segment_info(api_path)
                if seg_data and isinstance(seg_data, dict):
                    media_url = seg_data.get("l")
                elif seg_data and isinstance(seg_data, bytes):
                    media_url = self.config["endpoint"]["audio"].format(path=api_path)
                else:
                    continue
                    
                m3u8_lines.extend([f"#EXTINF:10.0,", media_url])
                resolved_count += 1
                
            if resolved_count > 0:
                m3u8_lines.append("#EXT-X-ENDLIST")
                raw_m3u8 = "\n".join(m3u8_lines)
                return self._save_temp_m3u8(raw_m3u8, audio_id)
                
        except Exception as e:
            self.log.error(f"Failed to stitch audio segments: {e}")
        return None

    def _get_series_episodes(self, album_id: str, start_order: int, end_order: int) -> dict:
        headers = {
            "User-Agent": self.config["device"]["user_agent_bws"],
            "Cookie": "; ".join([f"{c.name}={c.value}" for c in self.cookies]),
        }
        params = {
            "platformId": self.config["device"]["platform_id"],
            "modeCode": self.active_session.get("mode_code", ""),
            "langCode": self.active_session.get("lang_code", "en_us"),
            "deviceId": self.active_session.get("qc005", ""),
            "startOrder": str(start_order),
            "endOrder": str(end_order),
            "isVip": "true",
        }
        try:
            url = self.config["endpoint"]["series_episode"].format(albumid=album_id)
            return self._request("GET", url, params=params, headers=headers)
        except Exception as e:
            self.log.debug(f"Failed to get series episodes: {e}")
            return {}

    def _get_reality_episode_list(self, album_id: str, size: int, page_index: int) -> dict:
        headers = {"User-Agent": self.config["device"]["user_agent_bws"], "Cookie": "; ".join([f"{c.name}={c.value}" for c in self.cookies])}
        params = {"platformId": self.config["device"]["platform_id"], "modeCode": self.active_session.get("mode_code", ""), "langCode": self.active_session.get("lang_code", "en_us"), "deviceId": self.active_session.get("qc005", ""), "albumId": album_id, "size": str(size), "pageIndex": str(page_index)}
        try: return self._request("GET", self.config["endpoint"]["reality_episode"], params=params, headers=headers)
        except: return {}

    def _get_media_data(self, tvid: str, codec_key: str, bid: str, audio_codec_key: str = "dolby", lid: str = "1") -> Tuple[List[dict], List[dict], List[dict]]:
        video_config = self.config["quality"]["video"]
        audio_config = self.config["quality"]["audio"]
        video_params = video_config.get(codec_key, video_config["h264"]).copy()
        audio_params = audio_config.get(audio_codec_key, audio_config["dolby"]).copy()

        data = {
            "tvid": tvid, "uid": self.active_session.get("uid", "0"), "k_uid": self.active_session.get("qc005", ""),
            "tm": str(int(time.time() * 1000)), "bid": str(bid), "ut": self.active_session.get("type_code", ""),
            "src": self.active_session.get("ptid", ""), "ps": "1", "d": "0", "pm": "0", "fr": "25", "pt": "0",
            "s": "0", "rs": "1", "sr": "1", "sver": "2", "k_ver": "7.12.0", "k_tag": "1", "atype": "0", "vid": "",
            "lid": lid, "dcdv": "3", "ccsn": self.config["key"]["ccsn"], "agent_type": "366", "applang": "en_us",
            "ds": "0", "from_type": "1", "hdcp": "22", "cc_site": self.active_session.get("mode_code", ""),
            "cc_business": "1", "pano264": "800", "pano265": "800", "pre": "0", "ap": "1", "qd_v": "1",
            "fv": "2", "rt": "1", "dcv": "6", "ori": "puma", "X-USER-MODE": self.active_session.get("mode_code", ""), "ff": "ts",
        }
        data.update(video_params)
        data.update(audio_params)
        if codec_key == "8k": data.update({"ps": "0", "pt": "28000", "fr": "60"})

        try:
            json_data = self._get_dash_stream(data)
            self._current_title_data["chapters"] = json_data.get("p", {})
            self._current_title_data["highlights"] = json_data.get("svp") or json_data.get("asvp", [])
            program = json_data.get("program", {})
            return program.get("video", []), program.get("audio", []), program.get("stl", [])
        except Exception: return [], [], []

    def _get_dash_stream(self, data: dict) -> dict:
        query = urllib.parse.urlencode(data)
        path = f"/dash?{query}"
        vf = hashlib.md5((path + self.config["key"]["dash"]).encode()).hexdigest()
        url = self.config["endpoint"]["stream"].format(path=path, vf=vf)
        headers = {"accept": "*/*", "qyid": self.active_session.get("qc005", ""), "bop": f'{{"b_ft1":"3","dfp":"{self.active_session.get("dfp","")}","version":"9.0"}}', "pck": self.active_session.get("pck", ""), "User-Agent": self.config["device"]["user_agent"]}
        data = self._request("GET", url, headers=headers)
        return data.get("data", {})

    def _get_audio_segment_info(self, path: str) -> Union[dict, bytes, None]:
        url = self.config["endpoint"]["audio"].format(path=path)
        try:
            res = self.session.get(url, headers={"Accept": "*/*", "User-Agent": self.config["device"]["user_agent"]})
            return res.json() if res.status_code == 200 else res.content
        except: return None

    def _get_mode_code(self) -> str:
        try:
            data = self._request("GET", self.config["endpoint"]["mode"], params={"format": "json", "scene": "4"}, headers={"Accept": "*/*"})
            return data.get("data", {}).get("country", "us").lower()
        except: return "us"

    def _fetch_pck(self) -> Optional[str]:
        params = {"platformId": self.config["device"]["platform_id"], "modeCode": self.active_session.get("mode_code", ""), "langCode": self.active_session.get("lang_code", "en_us"), "deviceId": self.active_session.get("qc005", ""), "uid": self.active_session.get("uid", "0"), "interfaceCode": "indexnav_layer"}
        headers = {"User-Agent": self.config["device"]["user_agent_bws"], "Cookie": "; ".join([f"{c.name}={c.value}" for c in self.cookies])}
        try:
            res_data = self._request("GET", self.config["endpoint"]["pck"], params=params, headers=headers)
            for item in res_data.get("data", []):
                if url := item.get("apiUrl"):
                    parsed = urllib.parse.urlparse(url)
                    qs = urllib.parse.parse_qs(parsed.query)
                    return qs.get("P00001", [None])[0]
        except: return None

    def _get_ptid(self) -> str:
        params = {"platformId": self.config["device"]["platform_id"], "modeCode": self.active_session.get("mode_code", ""), "langCode": self.active_session.get("lang_code", "en_us"), "deviceId": self.active_session.get("qc005", "")}
        headers = {"User-Agent": self.config["device"]["user_agent_bws"], "Cookie": "; ".join([f"{c.name}={c.value}" for c in self.cookies])}
        try:
            data = self._request("GET", self.config["endpoint"]["ptid"], params=params, headers=headers)
            ptid = data.get("data", {}).get("ptid", "")
            return ptid.replace("0101003", "0202200", 1) if ptid.startswith("0101003") else ptid
        except: return ""

    def _request(self, method: str, endpoint: str, params: dict = None, headers: dict = None, payload: dict = None) -> Any:
        _headers = self.session.headers.copy()
        if headers: _headers.update(headers)
        req = requests.Request(method, endpoint, headers=_headers, params=params, json=payload)
        prepped = self.session.prepare_request(req)
        try:
            res = self.session.send(prepped)
            res.raise_for_status()
            return res.json() if res.text else {}
        except Exception as e:
            if any(key in endpoint for key in ["episode", "stream", "audio"]): raise e
            self.log.error(f"API Request failed: {e}")
            raise

    @staticmethod
    def _format_timecode(seconds: float) -> str:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:06.3f}"

    @property
    def pssh(self) -> str:
        if isinstance(self._ticket, bytes):
            try: return self._ticket.decode("utf-8")
            except: return base64.b64encode(self._ticket).decode("ascii")
        return self._ticket