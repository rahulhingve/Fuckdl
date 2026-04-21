import base64
import json
import click
import re
from requests import JSONDecodeError
from httpx import URL
import uuid
import xmltodict

import time
from datetime import datetime
from langcodes import Language
from fuckdl.objects import AudioTrack, TextTrack, Title, Tracks, VideoTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.vendor.pymp4.parser import Box
from fuckdl.utils.widevine.device import LocalDevice

class MoviesAnywhere(BaseService):
    """
    Service code for US' streaming service MoviesAnywhere (https://moviesanywhere.com).

    \b
    Authorization: Cookies
    Security: SD-HD@L3, FHD SDR@L1 (any active device), FHD-UHD HDR-DV@L1 (whitelisted devices).

    NOTE: Can be accessed from any region, it does not seem to care.
          Accounts can only mount services when its US based though.
    """
    ALIASES = ["MA", "moviesanywhere"]

    TITLE_RE = r"https://moviesanywhere\.com(?P<id>.+)"

    VIDEO_CODEC_MAP = {
        "H264": ["avc"],
        "H265": ["hvc", "hev", "dvh"]
    }
    AUDIO_CODEC_MAP = {
        "AAC": ["mp4a", "HE", "stereo"],
        "AC3": ["ac3"],
        "EC3": ["ec3", "atmos"]
    }

    @staticmethod
    @click.command(name="MoviesAnywhere", short_help="moviesanywhere.com")
    @click.argument("title", type=str)
   
    @click.pass_context
    def cli(ctx, **kwargs):
        return MoviesAnywhere(ctx, **kwargs)

    def __init__(self, ctx, title):
        super().__init__(ctx)
        self.parse_title(ctx, title)
        self.configure()
        cdm = ctx.obj.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.atmos = ctx.parent.params["atmos"]
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]

    def get_titles(self):
        self.headers={
            "authorization": f"Bearer {self.access_token}",
            "install-id": self.install_id,
        }
        res = self.session.post(
            url="https://gateway.moviesanywhere.com/graphql",
            json={
                "platform": "web",
                "variables": {"slug": self.title}, # Does not seem to care which platform will be used to give the best tracks available
                "extensions": '{"persistedQuery":{"sha256Hash":"5cb001491262214406acf8237ea2b8b46ca6dbcf37e70e791761402f4f74336e","version":1}}',  # ONE_GRAPH_PERSIST_QUERY_TOKEN
            },
            headers={
                "authorization": f"Bearer {self.access_token}",
                "install-id": self.install_id,
            }
        )

        try:
            self.content = res.json()
        except JSONDecodeError:
            self.log.exit(" - Not able to return title information")

        title_data = self.content["data"]["page"]

        title_info = [
            x
            for x in title_data["components"]
            if x["__typename"] == "MovieMarqueeComponent"
        ][0]
        
        title_info["title"] = re.sub(r" \(.+?\)", "", title_info["title"])

        title_data = self.content["data"]["page"]
        try:
            Id = title_data["components"][0]["mainAction"]["playerData"]["playable"]["id"]
        except KeyError:
            self.log.exit(" - Account does not seem to own this title")
        
        return Title(
                id_=Id,
                type_=Title.Types.MOVIE,
                name=title_info["title"],
                year=title_info["year"],
                original_lang="en",
                source=self.ALIASES[0],
                service_data=title_data,
            )
    
    def get_tracks(self, title):
        player_data = self.content["data"]["page"]["components"][0]["mainAction"]["playerData"]["playable"]

        playback_session_id = None
        if "playbackSessionId" in player_data:
            playback_session_id = player_data["playbackSessionId"]
        
        videos = []
        audios = []
        for cr in player_data["videoAssets"]["dash"].values():
            if not cr:
                continue
            for manifest in cr:
                tracks = Tracks.from_mpd(
                    url=manifest["url"],
                    source=self.ALIASES[0],
                    session=self.session,
                )
    
                for video in tracks.videos:
                    base_url = manifest["widevineLaUrl"] if not self.playready else manifest["playreadyLaUrl"]
                    
                    content_id = None
                    if "ContentId" in URL(base_url).params:
                        content_id = URL(base_url).params["ContentId"][0]
                    else:
                        content_id = player_data.get("contentId")
                    

                    if playback_session_id and content_id:
                        video.license_url = f"{base_url}&playbackSessionId={playback_session_id}"
                    else:
                        video.license_url = base_url
                        
                    video.contentId = content_id
                    videos += [video]
                    
                for audio in tracks.audios:
                    base_url = manifest["widevineLaUrl"] if not self.playready else manifest["playreadyLaUrl"]
                    content_id = None
                    if "ContentId" in URL(base_url).params:
                        content_id = URL(base_url).params["ContentId"][0]
                    
                    if playback_session_id and content_id:
                        audio.license_url = f"{base_url}&playbackSessionId={playback_session_id}"
                    else:
                        audio.license_url = base_url
                        
                    audio.contentId = content_id
                    if "atmos" in audio.url:
                        audio.atmos = True
                    audios += [audio]
    
        corrected_video_list = []
        for res in ("uhd", "hdp", "hd", "sd"):
            for video in videos:
                if f"_{res}_video" not in video.url or not video.url.endswith(f"&r={res}"):
                    continue
    
                if corrected_video_list and any(
                    video.id == vid.id for vid in corrected_video_list
                ):
                    continue
    
                if "dash_hevc_hdr" in video.url:
                    video.hdr10 = True
                if "dash_hevc_dolbyvision" in video.url:
                    video.dv = True
    
                corrected_video_list += [video]
    
        tracks = Tracks()
        tracks.add(corrected_video_list)
        tracks.audios = audios
        
        # Filtrar por codec
        if self.vcodec and self.vcodec in self.VIDEO_CODEC_MAP:
            tracks.videos = [x for x in tracks.videos if (x.codec or "")[:3] in self.VIDEO_CODEC_MAP[self.vcodec]]
        
        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None  # will use common privacy cert

    def license(self, challenge: bytes, track: Tracks, **_) -> bytes:
        license_url = track.license_url
        
        if "playbackSessionId" not in license_url and hasattr(track, 'playback_session_id'):
            license_url = f"{license_url}&playbackSessionId={track.playback_session_id}"
        
        self.log.debug(f" - License URL: {license_url}")
        self.log.debug(f" - Challenge size: {len(challenge)} bytes")
        
        try:
            license_message = self.session.post(
                url=license_url,
                data=challenge,
                headers={
                    "Content-Type": "application/octet-stream"
                }
            )
            
            if license_message.status_code != 200:
                self.log.error(f" - License request failed with status {license_message.status_code}")
                self.log.error(f" - Response: {license_message.text[:500]}")
                self.log.exit(" - Cannot complete license request")
            
            if "errorCode" in license_message.text:
                self.log.exit(f" - Cannot complete license request: {license_message.text}")
                
            return license_message.content
            
        except Exception as e:
            self.log.error(f" - License request exception: {str(e)}")
            raise
        
    def configure(self):
        access_token = None
        install_id = None
        for cookie in self.cookies:
            if cookie.name == "secure_access_token":
                access_token = cookie.value
            elif cookie.name == "install_id":
                install_id = cookie.value

        self.access_token = access_token
        self.install_id = install_id

        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Origin": "https://moviesanywhere.com",
                "Authorization": f"Bearer {self.access_token}",
            }
        )