import base64
import json
import random
import string
from hashlib import md5
from urllib.parse import urlencode, urlparse, parse_qs

import click
import requests
from langcodes import Language

from fuckdl.objects import Title, Tracks, VideoTrack, TextTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.collections import as_list
from fuckdl.utils.io import get_ip_info


class ClaroVideo(BaseService):
    """
    Service code for ClaroVideo streaming service (https://www.clarovideo.com).

    Original Script by @Dex 
    Ported by AnotherBigUserHere 

    Authorization: Credentials
    Security: FHD@L3

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["CV", "ClaroVideo", "CLVD", "clarovideo"]
    TITLE_RE = [
        r"https?://(?:www\.)?clarovideo\.com/(?P<region>[\w-]+)/vcard/(?:.*/)?(?P<id>\d+)/?$"
    ]
    LANGUAGE_MAP = {
        "MX": "es-MX", "AR": "es-AR", "CL": "es-CL", "CO": "es-CO", "PE": "es-PE",
        "US": "en-US", "BR": "pt-BR",
    }

    @staticmethod
    @click.command(name="ClaroVideo", short_help="https://www.clarovideo.com")
    @click.argument("title", type=str, required=False)
    @click.option("--master", type=str, required=False, default="ORIGINAL",
                  help="Get the selected master (e.g., ORIGINAL, ESPAÃ‘OL, INGLÃ‰S)")
    @click.pass_context
    def cli(ctx, **kwargs):
        return ClaroVideo(ctx, **kwargs)

    def __init__(self, ctx, title, master):
        super().__init__(ctx)
        m = self.parse_title(ctx, title)
        self.title_id = m["id"] if m else title
        self.region = m["region"].upper() if m and m.get("region") else None
        self.master = master or "ORIGINAL"

        self.device_id = ''.join(random.choices(string.hexdigits.lower(), k=16))
        self.user_info = {}
        self.current_manifest = None
        self._cdm = None

        self.configure()

    def configure(self):
        if not self.credentials:
            raise self.log.exit("Service requires credentials")

        # Detect region from IP if not provided
        if not self.region:
            ip_info = get_ip_info(self.session, fresh=True)
            country_code = ip_info.get('countryCode') or ip_info.get('country_code')
            self.region = (country_code or "MX").upper()
            self.log.info(f"Region detected: {self.region}")

        self.region = self.region.lower()
        self.log.info(f"Using region: {self.region}")
        
        self._login()

    def _login(self):
        """Login to ClaroVideo"""
        self.log.info("Logging into ClaroVideo...")
        
        # Step 1: Get initial HKS
        params = {
            "api_version": self.config["api_version"],
            "authpn": self.config["authpn"],
            "authpt": self.config["authpt"],
            "format": "json",
            "device_manufacturer": "samsung",
            "device_model": "android",
            "device_category": "tablet",
            "device_type": "SM-T560",
            "device_id": self.device_id,
            "osversion": "25",
            "device_so": "Android 7.1.2",
        }
        
        resp = self.session.get(self.config["endpoints"]["headerinfo"], params=params)
        resp.raise_for_status()
        hks = resp.json()["response"]["session_stringvalue"]
        self.log.info(f"Initial HKS obtained: {hks}")

        # Step 2: Login with credentials
        login_params = {
            "device_type": "SM-T560",
            "device_id": self.device_id,
            "device_category": "tablet",
            "device_manufacturer": "samsung",
            "device_so": "Android 7.1.2",
            "region": self.region,
            "authpt": self.config["params"]["android_base"]["authpt"],
            "device_model": "android",
            "authpn": self.config["params"]["android_base"]["authpn"],
            "HKS": hks,
            "format": "json"
        }
        
        login_data = {
            "username": self.credentials.username,
            "password": self.credentials.password
        }
        
        resp = self.session.post(
            self.config["endpoints"]["login_with_password"],
            params=login_params,
            data=login_data
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        
        self.log.info("Login successful, extracting session data...")
        
        # Step 3: Get profile info
        profile_params = {
            "device_type": "SM-T560",
            "device_manufacturer": "samsung",
            "device_category": "tablet",
            "region": self.region,
            "authpn": self.config["authpn"],
            "user_token": data["user_token"],
            "format": "json",
            "api_version": self.config["api_version"],
            "device_id": self.device_id,
            "authpt": self.config["authpt"],
            "lasttouch": "1773175683",
            "device_model": "android",
            "gamification_id": data.get("gamification_id", ""),
            "HKS": data["session_stringvalue"],
            "user_id": data["user_id"]
        }
        
        resp = self.session.get(self.config["endpoints"]["profile_read"], params=profile_params)
        resp.raise_for_status()
        
        # Find correct user_hash
        user_hash = None
        for member in resp.json()["response"]["data"]["members"]:
            if member["partnerUserId"] == data["user_id"]:
                user_hash = member["user_hash"]
                break
        
        if not user_hash:
            raise self.log.exit("Could not find user_hash in profile")

        self.user_info = {
            "user_id": data["user_id"],
            "user_token": data["user_token"],
            "user_hash": user_hash,
            "hks": data["session_stringvalue"],
            "gamification_id": data.get("gamification_id", "")
        }
        
        self.log.info(f"Final HKS: {self.user_info['hks']}")
        self.log.info(f"User ID: {self.user_info['user_id']}")
        self.log.info("Login completed successfully!")

    def get_titles(self):
        """Get title metadata"""
        params = {
            "user_id": self.user_info["user_id"],
            "HKS": self.user_info["hks"],
            "user_hash": self.user_info["user_hash"],
            "user_token": self.user_info["user_token"],
            "gamification_id": self.user_info.get("gamification_id", ""),
            "device_id": self.device_id,
            "device_type": "SM-T560",
            "device_category": "tablet",
            "device_manufacturer": "samsung",
            "device_model": "android",
            "region": "mexico",
            "api_version": self.config["api_version"],
            "authpn": self.config["authpn"],
            "authpt": self.config["authpt"],
            "group_id": self.title_id,
            "format": "json"
        }

        resp = self.session.get(self.config["endpoints"]["data"], params=params)
        resp.raise_for_status()
        metadata = resp.json()["response"]["group"]["common"]
        media = metadata["extendedcommon"]["media"]
        
        is_movie = "episode" not in media
        title_name = media.get("originaltitle", metadata.get("title"))
        release_year = media.get("publishyear", "")
        country_code = str(media.get("countryoforigin", {}).get("code", "MX")).upper()
        original_lang = self.LANGUAGE_MAP.get(country_code, "es-419")

        if is_movie:
            return Title(
                id_=self.title_id,
                type_=Title.Types.MOVIE,
                name=title_name,
                year=release_year,
                original_lang=original_lang,
                source=self.ALIASES[0],
                service_data=metadata
            )

        # Series: fetch episodes
        serie_params = params.copy()
        serie_params.update({
            "user_hash": self.user_info["user_hash"],
            "user_token": self.user_info["user_token"],
        })
        
        resp = self.session.get(self.config["endpoints"]["serie"], params=serie_params)
        resp.raise_for_status()
        
        titles = []
        for season in resp.json()["response"]["seasons"]:
            for episode in season["episodes"]:
                titles.append(Title(
                    id_=episode["id"],
                    type_=Title.Types.TV,
                    name=title_name,
                    season=int(episode['season_number']),
                    episode=int(episode['episode_number']),
                    episode_name=episode.get('title_episode', ''),
                    year=release_year,
                    original_lang=original_lang,
                    source=self.ALIASES[0],
                    service_data=episode,
                ))
        return titles

    def get_tracks(self, title):
        """Get tracks for a title"""
        # Get payway token
        payway_params = {
            "device_type": "SM-T560",
            "api_version": self.config["api_version"],
            "device_category": "tablet",
            "device_manufacturer": "samsung",
            "group_id": title.id,
            "region": self.region,
            "authpt": self.config["authpt"],
            "device_model": "android",
            "authpn": self.config["authpn"],
            "HKS": self.user_info["hks"],
            "format": "json",
            "user_id": self.user_info["user_id"]
        }
        
        resp = self.session.get(self.config["endpoints"]["payway"], params=payway_params)
        resp.raise_for_status()
        payway_token = resp.json()["response"]["playButton"]["payway_token"]

        # Get media info
        params = {
            "group_id": title.id,
            "stream_type": "dashwv_ma",
            "user_hash": self.user_info["user_hash"],
            "device_id": self.device_id,
            "device_type": "SM-T560",
            "device_category": "tablet",
            "device_manufacturer": "samsung",
            "device_model": "android",
            "region": self.region,
            "api_version": self.config["api_version"],
            "authpn": self.config["authpn"],
            "authpt": self.config["authpt"],
            "format": "json",
            "max_width": "1920",
            "max_height": "1080",
            "quality": "high",
            "video_quality": "FHD"
        }
        
        # Get content_id from title data
        data_params = {
            "user_id": self.user_info["user_id"],
            "HKS": self.user_info["hks"],
            "user_hash": self.user_info["user_hash"],
            "user_token": self.user_info["user_token"],
            "gamification_id": self.user_info.get("gamification_id", ""),
            "device_id": self.device_id,
            "device_type": "SM-T560",
            "device_category": "tablet",
            "device_manufacturer": "samsung",
            "device_model": "android",
            "region": "mexico",
            "api_version": self.config["api_version"],
            "authpn": self.config["authpn"],
            "authpt": self.config["authpt"],
            "group_id": title.id,
            "format": "json"
        }
        
        resp = self.session.get(self.config["endpoints"]["data"], params=data_params)
        resp.raise_for_status()
        title_data = resp.json()["response"]["group"]["common"]
        media_info = title_data["extendedcommon"]["media"]
        
        # Find the selected master audio
        audio_options = media_info["language"]["options"]["option"]
        title_audios = [opt for opt in audio_options if opt.get("option_name") != "subbed"]
        
        selected_master = None
        if self.master == "ORIGINAL":
            selected_master = next((x for x in title_audios if x["audio"] == "ORIGINAL"), title_audios[0])
        else:
            selected_master = next((x for x in title_audios if x["audio"] == self.master), None)
            if not selected_master:
                available = ', '.join([x['audio'] for x in title_audios])
                raise self.log.exit(f"Master '{self.master}' not found. Available: {available}")
        
        params["content_id"] = selected_master.get("content_id", "")
        params["preferred_audio"] = selected_master.get("audio", "")
        
        resp = self.session.post(
            self.config["endpoints"]["media"],
            params=params,
            data={"user_token": self.user_info["user_token"], "payway_token": payway_token}
        )
        resp.raise_for_status()
        
        manifest_info = resp.json()["response"]
        mpd_url = manifest_info["media"]["video_url"]
        
        # Remove resolution filter from MPD URL to get higher quality
        parsed = urlparse(mpd_url)
        query_params = parse_qs(parsed.query)
        if 'filter' in query_params:
            del query_params['filter']
            mpd_url = parsed._replace(query=urlencode(query_params, doseq=True)).geturl()
        
        self.current_manifest = manifest_info
        
        # Get service certificate
        cert_url = manifest_info["media"].get("certificate_url")
        if cert_url:
            cert_resp = self.session.get(cert_url)
            if cert_resp.ok:
                self.service_certificate = cert_resp.content

        # Send tracking stop
        tracking_stop_url = manifest_info["tracking"]["urls"]["stop"]
        self.session.get(tracking_stop_url, params={"timecode": 0})

        # Parse tracks from MPD
        tracks = Tracks.from_mpd(url=mpd_url, session=self.session, source=self.ALIASES[0])
        
        # Add subtitles
        if manifest_info["media"].get("subtitles"):
            subs = manifest_info["media"]["subtitles"]["options"]
            sub_items = subs.values() if isinstance(subs, dict) else as_list(subs)
            
            for sub in sub_items:
                sub_url = sub.get('external') or sub.get('url')
                if sub_url:
                    tracks.add(TextTrack(
                        id_=md5(sub_url.encode()).hexdigest(),
                        source=self.ALIASES[0],
                        url=sub_url,
                        codec="vtt",
                        language=sub.get('internal', 'unknown'),
                        cc=False,
                        sdh=False,
                        forced=False
                    ), warn_only=True)
        
        # Set correct language for tracks
        manifest_language = (
            title.original_lang
            if manifest_info["media"].get("audio", {}).get("selected", "") == "ORIGINAL"
            else manifest_info["media"].get("audio", {}).get("selected", "es-MX")
        )
        
        duration = int(manifest_info['media']['duration'].get('seconds', 0))
        for track in tracks:
            if isinstance(track, VideoTrack) and duration > 0 and track.bitrate:
                track.size = int((track.bitrate * duration) / 8)
            
            lang_str = str(track.language)
            if lang_str in ["or", "und"]:
                track.language = Language.get(manifest_language)
            elif lang_str == "pt":
                track.language = Language.get("pt-BR")
            elif lang_str == "es":
                track.language = Language.get("es-419")
            else:
                track.language = Language.get(manifest_language)
        
        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **kwargs):
        return None

    def license(self, challenge, title, **kwargs):
        """Get Widevine license"""
        if not self.current_manifest:
            raise self.log.exit("No manifest available for license request")

        # Parse challenge info from manifest
        challenge_info_string = self.current_manifest["media"]["challenge"]
        challenge_info = json.loads(challenge_info_string)
        token = challenge_info.get("token")
        
        # License server URL
        license_url = "https://widevine-claromex-vod.clarovideo.net/v2/licenser/getlicense"
        
        # URL parameters
        params = {"user_id": self.user_info["user_id"]}
        url_with_params = f"{license_url}?{urlencode(params)}"
        
        # Convert challenge to base64 if needed
        widevine_body = challenge if isinstance(challenge, str) else base64.b64encode(challenge).decode()
        
        # Request body
        request_body = {
            "device_id": self.device_id,
            "token": token,
            "widevineBody": widevine_body
        }
        
        # Headers
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Language': 'es-ES,es;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://www.clarovideo.com',
            'Referer': 'https://www.clarovideo.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0',
        }
        
        # Make license request
        response = self.session.post(url_with_params, json=request_body, headers=headers)
        response.raise_for_status()
        
        return response.content