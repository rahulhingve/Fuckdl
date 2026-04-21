import re
import urllib

import click
import pycountry
import requests
from langcodes import Language
from unidecode import unidecode

from fuckdl.objects import Title, Track
from fuckdl.objects.tracks import Tracks
from fuckdl.services import BaseService
from fuckdl.utils import base64, numeric_quality
from fuckdl.utils.collections import ObserverDict
from fuckdl.utils.path import Path
from fuckdl.utils.regex import find
from fuckdl.utils.sslciphers import SSLCiphers
from fuckdl.utils.xml import load_xml


class CanalPlus(BaseService):
    """
    Service code for Canal+ (https://www.canalplus.com/).

    \b
    Authorization: Cookies
    Security:
      Widevine:
            FR:
                L1: UHD (whitelisted)
                L1: FHD
                L3: HD (ChromeCDM)
                L3: SD
            PL:
                L3: FHD
        PlayReady:
            FR:
                SL3: UHD (whitelisted)
                SL3: FHD
                SL2: HD
            PL:
                SL2: FHD

    \b
    Notes:
        - If the connection just hangs/times out, your IP may be blocked.

    Added by default by @Mike

    """

    ALIASES = ["CNLP", "mycanal"]

    TITLE_RE = r"^https?://(?:www\.)?canalplus\.com(?:/(?P<region>[a-z]{2}))?/.+/h/(?P<id>[0-9_]+)"

    DRM_TYPE_MAP = {
        "DRM_MKPC_WIDEVINE_DASH": 0,
        "DRM_WIDEVINE": 1,
        "DRM_MKPC_PLAYREADY_DASH": 2,
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "EC3": "ec-3",
    }

    LANGUAGE_MAP = {
        "AT": "DE",
        "AU": "EN",
        "CI": "FR",
        "CN": "ZH",
        "DK": "DA",
        "GB": "EN",
        "JP": "JA",
        "KR": "KO",
        "MA": "AR",
        "SE": "SV",
        "US": "EN",
    }

    @staticmethod
    @click.command(
        name="CanalPlus",
        short_help="https://canalplus.com",
    )
    @click.argument("title", type=str, required=False)
    @click.option(
        "-r",
        "--region",
        default=None,
        help="Region to use. This is usually auto-detected from the URL or cookies.",
    )
    @click.option(
        "--trailers",
        is_flag=True,
        default=False,
        help="Download trailers for unreleased episodes.",
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return CanalPlus(ctx, **kwargs)

    def __init__(self, ctx, title, region, trailers):
        super().__init__(ctx)
        m = self.parse_title(ctx, title)
        # self.title = title
        self.region = region or m.get("region") or "fr"

        self.trailers = trailers

        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.range = ctx.parent.params["range_"]
        self.quality = ctx.parent.params["quality"] or 0

        self.audio_only = ctx.parent.params["audio_only"]
        # self.subtitles_only = ctx.parent.params["subtitles_only"]
        self.chapters_only = ctx.parent.params["chapters_only"]

        self.profile = ctx.parent.params["profile"] or "default"
        self.playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                         (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                          self.cdm.device.type == LocalDevice.Types.PLAYREADY)

        self.configure()

    # @cached_property
    def get_titles(self):
        # self.playready = self.drm_type is Track.DRM.PlayReady
        try:
            res = self.session.post(
                url=self.get_endpoint("metadata").format(title=self.title),
                json={
                    "operationName": "content",
                    "query": self.config["titles_query"],
                    "variables": {"id": self.title},
                },
            ).json()
        except requests.HTTPError as e:
            res = e.response.json()["mainError"]["extensions"]["response"]
            raise self.log.exit(f"{res['message']} [{res['code']}]") from res

        self.log.debug(res)

        title_info = res["data"]["catalog"]["unit"]

        try:
            iso_code_2 = title_info.get("productionNationalities", [{}])[0].get("codeISO2")
            original_lang = Language.get(self.LANGUAGE_MAP.get(iso_code_2, iso_code_2))
        except TypeError:
            original_lang = None

        if title_info["contentType"] == "movie" or (title_info["objectType"] == "unit" and not title_info["brand"]):
            return Title(
                id_=title_info["id"],
                type_=Title.Types.MOVIE,
                name=title_info["titles"]["originalTitle"],
                year=title_info["productionYear"],
                original_lang=original_lang,
                source=self.ALIASES[0],
            )
        else:
            titles = []

            if title_info["objectType"] == "brand":
                show_info = self.session.get(
                    url=self.get_endpoint("series").format(
                        token=self.session.cookies["tokenCMS"],
                        title=self.title,
                    ),
                    params={
                        "params[detailType]": "detailShow",
                        "objectType": "brand",
                        "params[dsp]": "detailPage",
                        "params[sdm]": "show",
                    },
                ).json()
                self.log.debug(show_info)

                try:
                    seasons = show_info["detail"]["seasons"]
                except KeyError:
                    raise self.log.exit(
                        f"Title: {show_info['detail']['informations']['title']} is unavailable"
                    ) from show_info

            elif title_info["objectType"] in ("season", "unit"):
                # The site often has only season IDs in URLs, so make sure to fetch the whole show
                if series := title_info["brand"]:
                    self.title = series["id"]
                    return self.titles
            else:
                raise self.log.exit(f"Unsupported content type: {title_info['objectType']}")

            for season in seasons:
                season_info = self.session.get(
                    url=self.get_endpoint("series").format(
                        token=self.session.cookies["tokenCMS"],
                        title=season["contentID"],
                    ),
                    params={
                        "params[detailType]": "detailSeason",
                        "objectType": "season",
                        "params[dsp]": "detailPage",
                        "params[sdm]": "show",
                    },
                ).json()

                self.log.debug(season_info)

                name = title_info.get("titles", {}).get("originalTitle") or title_info.get("titles", {}).get("title")

                titles += [
                    Title(
                        id_=ep["contentID"],
                        type_=Title.Types.TV,
                        name=re.sub(r" ep\. \d+$", "", name),
                        season=season.get("seasonNumber") or 1,
                        episode=ep.get("episodeNumber")
                        or find(r"^(Episode|Odcinek) (\d+)", unidecode(ep["title"]))
                        or i + 1,
                        episode_name=re.sub(r"^[EÃ‰]pisode \d+ - |^Odcinek \d+", "", ep["title"]),
                        original_lang=original_lang,
                        source=self.ALIASES[0],
                    )
                    for i, ep in enumerate(season_info["episodes"]["contents"])
                ]

            return titles

    def get_tracks(self, title: Title, retrying=False):
        # if isinstance(self.cdm.device, RemoteDevice) and not (
        #     self.audio_only or self.subtitles_only or self.chapters_only
        # ):
        #     # Populate device type
        #     self.cdm.device.get_device_info(
        #         service=self.__class__.__name__,
        #         codec=self.vcodec,
        #         range_=self.range,
        #         quality=title.quality,
        #     )

        res = self.session.get(self.get_endpoint("streams").format(title=title.id)).json()
        if res.get("status") == 403:
            self.log.warning("Received a 403 error, deleting cached tokens")
            self.tokens.clear()
            self.configure()
            return self.get_tracks(title, retrying=True)

        self.log.debug(res)

        if self.playready:
            desired_drm_type_map = [x for x in self.DRM_TYPE_MAP if "PLAYREADY" in x]
        else:
            desired_drm_type_map = [x for x in self.DRM_TYPE_MAP if "WIDEVINE" in x]

        streams = sorted(
            (x for x in res["available"] if x["drmType"] in desired_drm_type_map),
            key=lambda x: self.DRM_TYPE_MAP[x["drmType"]],
        )
        if not streams:
            raise self.log.exit("No suitable streams found")

        if self.vcodec == "H265" or numeric_quality(self.quality) > 1080:
            if self.range == "HDR10":
                quality = "UHD-HDR"
            elif self.range == "DV":
                quality = "UHD-DBV"
            else:
                quality = "UHD"
        else:
            quality = "HD"

        tracks, license_url, drm_id = self.get_manifest_tracks(title, streams, quality)

        if not tracks:
            raise self.log.exit("Requested quality not available")

        title.service_data["license_url"] = license_url
        title.service_data["license_url_uhd"] = license_url if quality == "UHD" else None
        title.service_data["drm_id"] = drm_id
        title.service_data["uhd_drm_id"] = drm_id if quality == "UHD" else None

        uhd_tracks, license_url, drm_id = self.get_manifest_tracks(title, streams, "UHD")
        self.uhd_present = bool(uhd_tracks)
        if not uhd_tracks:
            self.log.info("No UHD tracks found")
        else:
            if quality == "HD" and uhd_tracks:
                title.service_data["license_url_uhd"] = license_url
                title.service_data["uhd_drm_id"] = drm_id

                self.log.info("Fetching UHD tracks to find better audio")

                for audio in uhd_tracks.audios:
                    audio.id += "_uhd"

                tracks.add(uhd_tracks.audios)

        if quality == "UHD":
            self.log.info("Fetching HD tracks to find missing audio")
            hd_tracks, license_url, drm_id = self.get_manifest_tracks(title, streams, "HD")
            tracks.add(hd_tracks.audios, warn_only=True)

        if self.acodec:
            tracks.audios = [x for x in tracks.audios if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]]

        return tracks

    def certificate(self, *_, **__):
        # if self.cdm.device.type == Device.Type.CHROME:
        #     return self.cdm.common_privacy_cert
        return None

    def license(self, challenge, title, track, *_, **__):
        params = {}
        if self.playready:
            # Handle challenge - it might be bytes or string
            if isinstance(challenge, bytes):
                try:
                    # Try to decode as UTF-8
                    challenge_str = challenge.decode('utf-8')
                except UnicodeDecodeError:
                    # If that fails, try latin-1
                    challenge_str = challenge.decode('latin-1')
                # Extract SOAP envelope
                start = challenge_str.find("<soap:Envelope")
                if start != -1:
                    challenge = challenge_str[start:]
            else:
                # Already a string
                start = challenge.find("<soap:Envelope")
                if start != -1:
                    challenge = challenge[start:]

        params["drmConfig"] = "mkpl::true" if track.descriptor == Track.Descriptor.MPD else None
        try:
            res = self.session.post(
                url=title.service_data["license_url"]
                if not track.id.endswith("_uhd")
                else title.service_data["license_url_uhd"],
                params=params,
                data=challenge if self.playready else base64.encode(challenge),
                headers={
                    "Content-Type": "text/plain",
                },
            )
            # Always get raw bytes first
            response_data = res.content
        except requests.HTTPError as e:
            if e.response.status_code == 403 and self.uhd_present:
                self.log.warning("Retrying with UHD license url (ERROR 403)")
                try:
                    params["drmId"] = title.service_data["uhd_drm_id"]
                    res = self.session.post(
                        url=title.service_data["license_url"],
                        params=params,
                        data=challenge if self.playready else base64.encode(challenge),
                        headers={
                            "Content-Type": "text/plain",
                        },
                    )
                    response_data = res.content
                except requests.HTTPError as e:
                    res = e.response.json()
                    self.log.debug(res)
                    raise self.log.exit(f"{res['message']} [{res['code']}]") from res
            else:
                res = e.response.json()
                self.log.debug(res)
                raise self.log.exit(f"{res['message']} [{res['code']}]") from res

        if self.playready:
            # For PlayReady, try to decode as text
            try:
                response_text = response_data.decode('utf-8')
            except UnicodeDecodeError:
                response_text = response_data.decode('latin-1')
            
            # Extract SOAP envelope
            start = response_text.find("<soap:Envelope")
            end = response_text.rfind("</soap:Envelope>")
            
            if start != -1 and end != -1:
                return '<?xml version="1.0" encoding="utf-8"?>' + response_text[start:end + 16]
            return response_data

        # For Widevine
        try:
            response_text = response_data.decode('utf-8')
        except UnicodeDecodeError:
            response_text = response_data.decode('latin-1')
        
        res = load_xml(response_text)
        return res.findtext(".//license")

    # # Service-specific functions

    def save_tokens(self):
        self.tokens_path.parent.mkdirp()
        self.tokens_path.write_json(self.tokens)

    def configure(self):
        # Canal+ uses a weak DH key, SSL security level must be reduced
        self.session.mount(
            "https://",
            SSLCiphers(
                max_retries=self.session.adapters["https://"].max_retries,
            ),
        )

        # TODO: Check cookie expiry
        for cookie in ("passId", "deviceId", "sessionId"):
            if cookie not in self.session.cookies:
                raise self.log.exit(f"Missing {cookie} cookie")

        if not self.region:
            self.region = self.session.cookies["passId"].split("=")[0]

        if not self.region:
            raise self.log.exit("No region specified")

        if isinstance(self.region, str):
            # Don't look up region again if it's already a country object
            self.region = pycountry.countries.get(alpha_2=self.region)

        self.log.info(f" + Region: {self.region.alpha_2}")

        self.tokens_path = Path(self.get_cache(f"tokens_{self.profile}.json"))
        self.tokens = ObserverDict(self.tokens_path.read_json(missing_ok=True))
        self.tokens.on_change = self.save_tokens

        if self.tokens.get("pass_token"):
            self.log.info("Using cached token")
        else:
            self.log.info("Getting token")

            local_config = {
                "pass": {
                    "portailId": "vbdTj7eb6aM."
                }
            }
            self.log.info("Using local config instead of API request")
            
            res = self.session.post(
                url=self.get_endpoint("token"),
                data={
                    "deviceId": self.session.cookies["deviceId"],
                    "media": "web",
                    "noCache": "false",
                    "portailId": local_config["pass"]["portailId"],
                    "sessionId": self.session.cookies["sessionId"],
                    "vect": "INTERNET",
                    "zone": f"cp{self.region.alpha_3.lower()}",
                    "passId": self.session.cookies["passId"],
                },
            ).json()
            self.log.debug(res)

            self.tokens["pass_token"] = res["response"]["passToken"]
            if not self.tokens["pass_token"]:
                raise self.log.exit(
                    f"Failed to get PASS token: {res['response']['errorMessage']} [{res['response']['errorCode']}]",
                )

        self.session.headers.update({
            "Authorization": f'PASS Token="{self.tokens["pass_token"]}"',
            "Origin": "https://www.canalplus.com",
            "Referer": "https://www.canalplus.com/",
            "XX-API-VERSION": "3.0",
            "XX-DEVICE": f"pc {self.session.cookies['deviceId'].split(':')[0]}",
            "XX-DISTMODES": "catchup,live,svod,tvod,posttvod",
            "XX-DOMAIN": f"cp{self.region.alpha_3.lower()}",
            "XX-OL": self.region.alpha_2.lower(),
            "XX-OPERATOR": "pc",
            "XX-OZ": f"cp{self.region.alpha_3.lower()}",
            "XX-Profile-Id": "0",
            "XX-SERVICE": "mycanal",
            "XX-SPYRO-VERSION": "3.0",
        })

    def get_endpoint(self, name):
        endpoint = self.config["endpoints"][name]
        if isinstance(endpoint, dict):
            endpoint = endpoint["prod" if self.region.alpha_2 == "prod" else "intl"]
        # NOTE: Not using .format() here because it raises KeyError if we leave some fields unformatted
        return endpoint.replace("{region}", self.region.alpha_2.lower())

    def get_manifest_tracks(self, title: Title, streams, quality):
        streams = [x for x in streams if x["quality"] == quality]
        if not streams:
            return Tracks(), None, None

        r = self.session.put(self.get_endpoint("view"), params={"include": "medias,ads"}, json=streams[0])
        res = r.json()
        self.log.debug(res)

        license_url = urllib.parse.urljoin(r.url, res["@licence"])
        drm_id = res["medias"][0].get("drmId")

        res = self.session.get(urllib.parse.urljoin(r.url, res["@medias"])).json()
        self.log.debug(res)

        if res[0]["duration"] == 0 and not self.trailers:
            raise self.log.exit(
                "This episode is not yet available. Use --trailers if you want to download the trailer.",
            )

        manifest = next(x for x in res[0]["files"] if x["type"] == "video")

        if manifest["mimeType"] == "application/dash+xml":
            tracks: Tracks = Tracks.from_mpd(
                url=manifest["distribURL"],
                source=self.ALIASES[0],
                session=self.session,
            )
            # tracks = self.tracks_from_mpd(manifest["distribURL"])
            for track in tracks:
                track.needs_proxy = True
                track.extra_headers = {
                    "Accept": "*/*",
                    "Connection": "keep-alive",
                    "Origin": "https://www.canalplus.com",
                    "Referer": "https://www.canalplus.com/",
                    "XX-API-VERSION": "3.0",
                    "XX-DEVICE": f"pc {self.session.cookies['deviceId'].split(':')[0]}",
                    "XX-DOMAIN": f"cp{self.region.alpha_3.lower()}",
                    "XX-OPERATOR": "pc",
                    "XX-SERVICE": "mycanal",
                }
        else:
            raise self.log.exit(f"Unsupported manifest type: {manifest['mimeType']}")
        for x in tracks.subtitles:
            if "stl_1_" in x.url:  # does better way exist?
                x.sdh = True
        for track in tracks.audios:
            if str(track.language) == "qaa":
                track.language = Language.get(title.original_lang)
            elif str(track.language) == "qad":
                track.language = Language.get(title.original_lang)
                track.descriptive = True

        return tracks, license_url, drm_id
