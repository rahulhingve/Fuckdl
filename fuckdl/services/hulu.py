import hashlib
import re
import click
import requests
from langcodes import Language

from fuckdl.objects import TextTrack, MenuTrack, Title, Tracks
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.widevine.device import LocalDevice

class Hulu(BaseService):
    """
    Service code for the Hulu streaming service (https://hulu.com).

    Added by @AnotherBigUserHere
    Updated by @droctavius3902

    + Hulu gonna be merged into Disney+ staring from March 2026 so use it as much you can

    \b
    Authorization: Cookies
    Security: UHD@SL3000, HD@SL3000, SD@SL3000

    Tip: if you get an error in keys mismatch while decrypting just turn "multi_key" : True -> False then it will work fine.

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["HULU"]
    GEOFENCE = ["us"]
    TITLE_RE = (r"^(?:https?://(?:www\.)?hulu\.com/(?P<type>movie|series)/)?(?:[a-z0-9-]+-)?"
                r"(?P<id>[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})")

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "EC3": "ec-3"
    }

    @staticmethod
    @click.command(name="Hulu", short_help="https://hulu.com")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.option("-mt", "--mpd-type", type=click.Choice(["new", "old"], case_sensitive=False),
                  default="new",
                  help="which mpd type to use")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Hulu(ctx, **kwargs)

    def __init__(self, ctx, title, movie, mpd_type):
        super().__init__(ctx)
        m = self.parse_title(ctx, title)
        self.movie = movie or m.get("type") == "movie"
        self.mpd_type = mpd_type
        self._is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                             (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                              self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.range_ = ctx.parent.params["range_"]

        
        if self.vcodec != "H265":
            self.log.info("Switched video codec to H265 to be able to get UHD video tracks")
            self.vcodec = "H265"

        if self.range_ == "HDR10":
            self.log.info("Switched dynamic range to DV as Hulu only has HDR10+ compatible DV tracks")
            self.range_ = "DV"
            ctx.parent.params["range_"] = "DV"

        self.device = None
        self.playback_params = {}
        self.hulu_client = None
        self.license_url = None

        self.configure()

    def get_titles(self):
        titles = []

        if self.movie:
            res = self.session.get(self.config["endpoints"]["movie"].format(id=self.title)).json()
            title_data = res["details"]["vod_items"]["focus"]["entity"]
            titles.append(Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=title_data["name"],
                year=int(title_data["premiere_date"][:4]),
                source=self.ALIASES[0],
                service_data=title_data
            ))
        else:
            try:
                res = self.session.get(self.config["endpoints"]["series"].format(id=self.title)).json()
            except requests.HTTPError as e:
                res = e.response.json()
                raise self.log.exit(f" - Failed to get titles for {self.title}: {res['message']} [{res['code']}]")

            season_data = next((x for x in res["components"] if x["name"] == "Episodes"), None)
            if not season_data:
                raise self.log.exit(" - Unable to get episodes. Maybe you need a proxy?")

            for season in season_data["items"]:
                episodes = self.session.get(
                    self.config["endpoints"]["season"].format(
                        id=self.title,
                        season=season["id"].rsplit("::", 1)[1]
                    )
                ).json()
                for episode in episodes["items"]:
                    titles.append(Title(
                        id_=f"{season['id']}::{episode['season']}::{episode['number']}",
                        type_=Title.Types.TV,
                        name=episode["series_name"],
                        season=int(episode["season"]),
                        episode=int(episode["number"]),
                        episode_name=episode["name"],
                        source=self.ALIASES[0],
                        service_data=episode
                    ))

        return titles

    def remove_parts_mpd(self, mpd):
        pattern = r'<Representation[^>]*id="(?![^"]*ALT_1)[^"]*CENC_CTR_[^"]*"[^>]*width="1920"[^>]*height="1080"[^>]*>.*?</Representation>\s*'
        m = re.sub(pattern, "", mpd, flags=re.DOTALL)
        return m

    def get_tracks(self, title):
        
        try:
            playlist = self.session.post(
                url=self.config["endpoints"]["manifest"],
                json={
                    "deejay_device_id": 210 if self.mpd_type == "new" else 166,
                    "version": 1 if self.mpd_type == "new" else 9999999,
                    "all_cdn": False,
                    "content_eab_id": title.service_data["bundle"]["eab_id"],
                    "region": "US",
                    "language": "en",
                    "unencrypted": True,
                    "network_mode": "wifi",
                    "play_intent": "resume",
                    "playback": {
                        "version": 2,
                        "video": {
                            "dynamic_range": "DOLBY_VISION",  
                            "codecs": {
                                "values": [x for x in self.config["codecs"]["video"] if x["type"] == self.vcodec],
                                "selection_mode": self.config["codecs"]["video_selection"]
                            }
                        },
                        "audio": {
                            "codecs": {
                                "values": self.config["codecs"]["audio"],
                                "selection_mode": self.config["codecs"]["audio_selection"]
                            }
                        },
                        "drm": {
                            "multi_key": True,
                            "values": self.config["drm"]["schemas_pr"] if self._is_playready else self.config["drm"]["schemas_wv"],
                            "selection_mode": self.config["drm"]["selection_mode"],
                            "hdcp": self.config["drm"]["hdcp"]
                        },
                        "manifest": {
                            "type": "DASH",
                            "https": True,
                            "multiple_cdns": False,
                            "patch_updates": True,
                            "hulu_types": True,
                            "live_dai": True,
                            "secondary_audio": True,
                            "live_fragment_delay": 3
                        },
                        "segments": {
                            "values": [{
                                "type": "FMP4",
                                "encryption": {
                                    "mode": "CENC",
                                    "type": "CENC"
                                },
                                "https": True
                            }],
                            "selection_mode": "ONE"
                        }
                    }
                }
            ).json()

        except requests.HTTPError as e:
            res = e.response.json()
            raise self.log.exit(f" - {res['message']} ({res['code']})")

        title.original_lang = Language.get(playlist["video_metadata"]["language"])
        manifest = playlist["stream_url"]

        self.playlist = playlist
        self.license_url_widevine = playlist.get("wv_server")
        self.license_url_playready = playlist.get('dash_pr_server')

        self.log.info(f"DASH: {manifest}")

        if 'disney' in manifest:
            mpd = self.session.get(manifest).text
            mpd_data = self.remove_parts_mpd(mpd)

            tracks = Tracks.from_mpd(
                url=manifest,
                data=mpd_data,
                session=self.session,
                source=self.ALIASES[0]
            )
        else:
            tracks = Tracks.from_mpd(
                url=manifest,
                session=self.session,
                source=self.ALIASES[0]
            )

        try:
            if self.cdm.device.type == LocalDevice.Types.PLAYREADY:
                video_pssh = next(x.pr_pssh for x in tracks.videos if x.pr_pssh)

                for track in tracks.videos:
                    if track.hdr10:
                        track.hdr10 = False
                        track.dv = True

                for track in tracks.audios:
                    if not track.pr_pssh:
                        track.pr_pssh = video_pssh
            else:
                video_pssh = next(x.pssh for x in tracks.videos if x.pssh)

                for track in tracks.videos:
                    if track.hdr10:
                        track.hdr10 = False
                        track.dv = True

                for track in tracks.audios:
                    if not track.pssh:
                        track.pssh = video_pssh
        except:
            pass

        if self.acodec:
            tracks.audios = [x for x in tracks.audios if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]]

        try:
            for sub_lang, sub_url in playlist["transcripts_urls"]["webvtt"].items():
                tracks.add(TextTrack(
                    id_=hashlib.md5(sub_url.encode()).hexdigest()[0:6],
                    source=self.ALIASES[0],
                    url=sub_url,
                    # metadata
                    codec="vtt",
                    language=sub_lang,
                    forced=False,  # TODO: find out if sub is forced
                    sdh=False  # TODO: find out if sub is SDH/CC, it's actually quite likely to be true
                ))
        except KeyError:
            pass

        return tracks

    def get_chapters(self, title):
        try:
            segments = self.playlist["video_metadata"]["segments"]
            end_credits_time = self.playlist["video_metadata"].get("end_credits_time", None)
            if segments is None and end_credits_time is None:
                return []
            chapters = [segment.replace("T:", "") for segment in segments.split(",")]
            if end_credits_time:
                end_credits_time = end_credits_time.replace(";",".0")
                end_credits_time = end_credits_time[:-3] + end_credits_time[-3:].zfill(3)
                if end_credits_time != '00:00:00.000':
                    chapters.append(end_credits_time)
            chapters = [x.replace(";",".0") for x in chapters]
            chapters = [x[:-3] + x[-3:].zfill(3) for x in chapters]
            chapters.insert(0, '00:00:00.000')
            return [MenuTrack(index + 1, "Chapter {:02d}".format(index + 1), chapter) for index, chapter in enumerate(chapters)]

        except:
            return None

    def certificate(self, **_):
        return None  

    def license(self, challenge, track, **_):
        if self.cdm.device.type == LocalDevice.Types.PLAYREADY:
            req = self.session.post(
                url=self.license_url_playready,
                data=challenge  
            )
            res = req.content
            self.log.debug(res)
            return res
        else:
            res = self.session.post(
                url=self.license_url_widevine,
                data=challenge  
            ).content
            self.log.debug(res)
            return res


    def configure(self):
        self.session.headers.update({
            "User-Agent": self.config["user_agent"],
        })