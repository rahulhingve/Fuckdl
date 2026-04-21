import base64
import hashlib
import hmac
import json
import os
import posixpath
import re
import threading
import time
import urllib.parse
from datetime import datetime
from hashlib import md5

import click
import websocket
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad
from langcodes import Language

from fuckdl.objects import TextTrack, Title, Tracks
from fuckdl.services.BaseService import BaseService


class Fandango(BaseService):
    """
    Service code for Fandango At Home (https://athome.fandango.com/).

    Added by @AnotherBigUserHere

    Fixed by @iamawesom31

    + Login fixed with credentials and session keys
    + Playready added (SL3000)
    + Special payload to request (4K HDR and DV)

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    \b
    Authorization: Credentials
    Security: UHD@L1* HD@L1 SD@L3, HD@SL3000 SD@SL3000

    *HEVC/UHD requires a whitelisted CDM.
    """

    ALIASES = ["FAND", "VUDU"]
    GEOFENCE = ["us"]
    TITLE_RE = [
        r"^(?:https?://(?:www\.)?vudu\.com/content/movies/details/[a-zA-Z0-9-]+/)?(?P<id>\d+)",
        r"^https?://athome\.fandango\.com/content/browse/details/[a-zA-Z0-9-]+/(?P<id>\d+)",
    ]

    VIDEO_QUALITY_MAP = {
        "HD": "hdx",
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "EC3": "ec-3"
    }

    @staticmethod
    @click.command(name="Fandango", short_help="https://athome.fandango.com")
    @click.argument("title", type=str, required=False)
    @click.option("-q", "--quality", default=None,
                  type=click.Choice(["SD", "HD", "UHD"], case_sensitive=False),
                  help="Manifest quality to request")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Fandango(ctx, **kwargs)

    def __init__(self, ctx, title, quality):
        super().__init__(ctx)
        self.parse_title(ctx, title)
        self.quality = quality

        self.profile = ctx.obj.profile

        # Inherit global CLI parameters for proxy, video range, codecs
        self.proxy = ctx.parent.params["proxy"]
        self.range = ctx.parent.params["range_"]
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]

        # Auto-upgrade manifest quality and codec based on requested resolution/codec/range
        quality = ctx.parent.params.get("quality") or 0
        if quality != "SD" and quality > 1080 and self.quality != "UHD":
            self.log.info(" + Switched manifest quality to UHD to be able to get 2160p video track")
            self.quality = "UHD"

        if self.vcodec == "H265" and self.quality != "UHD":
            self.log.info(" + Switched manifest quality to UHD to be able to get H265 manifest")
            self.quality = "UHD"

        if self.range in ("HDR10", "DV") and self.quality != "UHD":
            self.log.info(f" + Switched manifest quality to UHD to be able to get {self.range} dynamic range")
            self.quality = "UHD"

        if self.quality == "UHD" and self.vcodec != "H265":
            self.log.info(" + Switched video codec to H265 to be able to get UHD manifest")
            self.vcodec = "H265"

        # Session state â€” populated during configure()
        self.user_id = None
        self.session_key = None
        self.light_device_id = None
        self.websocket = None
        self.keepalive_thread = None

        self.configure()

    def get_titles(self):
        # Fetch content metadata by ID to determine type (movie, series, season, episode)
        res = self.cache_request({
            "_type": "contentSearch",
            "clientType": "html5app",
            "contentEncoding": "gzip",
            "contentId": self.title,
            "count": "1",
            "followup": [
                "contentVariants", "editions", "longCredits",
            ],
            "format": "application/json",
            "offset": "0",
        })
        if "content" not in res:
            raise self.log.exit(" - Title not found")

        res = res["content"][0]

        content_type = res["type"][0]
        title = res["title"][0]
        season_ids = []
        contents = []

        if content_type == "program":
            return Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=res["title"][0],
                year=res["releaseTime"][0].split("-")[0],
                original_lang=Language.find(res["language"][0]),
                source=self.ALIASES[0],
                service_data=res,
            )
        else:
            # TODO: Figure out a better way to get series titles without extra things at the end
            if content_type == "series":
                # Strip " [TV Series]" suffix from series titles
                title = re.sub(r" \[TV Series]$", "", title)
                # Fetch all seasons for this series
                res = self.cache_request({
                    "_type": "contentSearch",
                    "contentEncoding": "gzip",
                    "count": "75",
                    "dimensionality": "any",
                    "followup": ["seasonNumber", "promoTags", "ratingsSummaries", "advertEnabled", "uxPromoTags"],
                    "format": "application/json",
                    "includeComingSoon": "true",
                    "listType": "useful",
                    "offset": "0",
                    "seriesId": self.title,
                    "sortBy": "-seasonNumber",
                    "type": "season"
                })
                if "content" not in res:
                    raise self.log.exit(" - Title not found")
                season_ids = [x["contentId"][0] for x in res["content"]]
            elif content_type == "season":
                title = re.sub(r": Season \d+$", "", title)
                season_ids = [self.title]
            elif content_type == "episode":
                title = re.sub(r": .+", "", title)
                contents += res["content"]

            # Fetch all episodes for each season
            for season_id in season_ids:
                res = self.cache_request({
                    "_type": "contentSearch",
                    "contentEncoding": "gzip",
                    "count": "75",
                    "dimensionality": "any",
                    "followup": [
                        "episodeNumberInSeason", "seasonNumber", "contentVariants", "editions",
                    ],
                    "format": "application/json",
                    "includeComingSoon": "true",
                    "listType": "useful",
                    "offset": "0",
                    "seasonId": season_id,
                    "sortBy": "episodeNumberInSeason"
                })
                if "content" not in res:
                    raise self.log.exit(" - Title not found")
                contents += res["content"]

            return [Title(
                id_=self.title,
                type_=Title.Types.TV,
                name=title,
                season=int(x["seasonNumber"][0]),
                episode=int(x["episodeNumberInSeason"][0]),
                # TODO: Figure out a better way to get the unprefixed episode name.
                # Episode name often/always(?) starts with the show name, but it's not always an exact match.
                episode_name=re.sub(r"^.+?: ", "", x["title"][0]),
                original_lang=Language.find(x["language"][0]),
                source=self.ALIASES[0],
                service_data=x
            ) for x in contents]

    def get_tracks(self, title):
        """Fetch video, audio, and subtitle tracks for a given title."""
        # Reconnect WebSocket if it was closed (e.g., between episodes)
        self._ensure_websocket()

        tracks = Tracks()

        # Select the content variant based on requested quality.
        # If no quality specified, pick the highest available variant with DASH support.
        if self.quality is None:
            try:
                variant = [
                    x for x in title.service_data["contentVariants"][0]["contentVariant"] if "dashEditionId" in x
                ][-1]
            except IndexError:
                raise self.log.exit(" - No DASH streams found")
        else:
            variant = next((
                x for x in title.service_data["contentVariants"][0]["contentVariant"]
                if x["videoQuality"][0] == self.VIDEO_QUALITY_MAP.get(self.quality, self.quality).lower()
            ), None)
            if not variant:
                raise self.log.exit(" - Requested quality not available")

        # Determine the video profile string used to match DASH editions
        if self.vcodec == "H265":
            if self.range == "SDR":
                video_profile = "main10"
            elif self.range == "HDR10":
                video_profile = "hdr10"
            elif self.range == "DV":
                video_profile = "dvheStn"
        else:
            video_profile = "highP"

        # Find the DASH edition matching the desired video profile
        edition = next((
            x for x in variant["editions"][0]["edition"]
            if x["editionFormat"][0] == "dash" and video_profile in x["videoProfile"]
        ), None)
        if not edition:
            raise self.log.exit(" - Requested edition not found")

        edition_format = edition["editionFormat"][0]
        edition_id = edition["editionId"][0]

        # Store edition ID for DRM license requests later
        title.service_data["edition_id"] = edition_id
        self._current_edition_id = edition_id

        # Store contentVariantId and start streaming session (required before DRM requests)
        content_variant_id = variant.get("contentVariantId", [""])[0]
        if content_variant_id:
            self.log.info(f" + Starting streaming session for contentVariantId={content_variant_id}")
            ss_res = self.websocket_send({
                "_type": "streamingSessionStart",
                "accountId": self.user_id,
                "contentVariantId": content_variant_id,
                "lightDeviceId": self.light_device_id,
                "requestCallbackId": 2,
            })
            ss_type = ss_res.get("_type", [""])[0] if isinstance(ss_res.get("_type"), list) else ss_res.get("_type", "")
            if ss_type == "streamingSessionResult":
                ss_status = ss_res.get("streamingSessionStatus", [""])[0]
                if ss_status != "success":
                    self.log.warning(f" - Streaming session status: {ss_status}")
                else:
                    self.log.info(" + Streaming session started")
            elif ss_type == "error":
                self.log.warning(f" - Streaming session error: {ss_res.get('text', ['unknown'])[0]}")
            else:
                self.log.debug(f" + streamingSessionStart response: {ss_res}")

        # Request the streaming location (CDN URLs, subtitle base URI, etc.) via WebSocket
        res = self.websocket_send({
            "_type": "editionLocationGet",
            "editionFormat": edition_format,
            "editionId": edition_id,
            "isSecure": "true",
            "requestCallbackId": 1,
            "userId": self.user_id,
            "videoProfile": video_profile,
        })
        if res["_type"] == ["error"]:
            raise self.log.exit(f" - Failed to get manifest: {res['text'][0]}")

        # Build the MPD manifest URL from the base URI and signed suffix
        mpd_url = posixpath.join(res["location.0.baseUri"][0], "manifest.mpd" + res["location.0.uriSuffix"][0])
        self.log.debug(f" + MPD URL: {mpd_url}")

        tracks = Tracks.from_mpd(
            url=mpd_url,
            source=self.ALIASES[0]
        )

        # Tag video tracks with HDR10 flag if the edition uses HDR10 dynamic range
        if res["location.0.dynamicRange"] == ["hdr10"]:
            for video in tracks.videos:
                video.hdr10 = True

        # Filter audio tracks by requested codec (e.g., AAC or EC3)
        if self.acodec:
            tracks.audios = [x for x in tracks.audios if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]]

        # Add subtitle tracks by probing the subtitleBaseUri from the editionLocationGet response.
        # The API doesn't expose subtitle metadata (version, language) directly, so we probe
        # sequential version numbers with HEAD requests to find valid VTT files.
        # URL pattern from JS: subtitleBaseUri + "/subtitle." + version + "." + langCode + ".vtt" + subtitleUriSuffix
        subtitle_base_uri = res.get("location.0.subtitleBaseUri", [None])[0]
        subtitle_uri_suffix = res.get("location.0.subtitleUriSuffix", [""])[0]
        if subtitle_base_uri:
            # Derive language code from the content's primary language (e.g. "English" -> "en")
            lang = title.service_data.get("language", ["English"])[0]
            lang_code = Language.find(lang).to_tag()
            # Probe versions 1-10 to discover available subtitle files
            for ver in range(1, 11):
                sub_url = f"{subtitle_base_uri}/subtitle.{ver}.{lang_code}.vtt{subtitle_uri_suffix}"
                try:
                    head_resp = self.session.head(sub_url)
                    if head_resp.status_code == 200:
                        self.log.debug(f" + Found subtitle: subtitle.{ver}.{lang_code}.vtt")
                        tracks.add(TextTrack(
                            id_=md5(sub_url.encode()).hexdigest()[0:6],
                            source=self.ALIASES[0],
                            url=sub_url,
                            codec="vtt",
                            language=lang_code,
                        ))
                        break  # Found a valid subtitle for this language, skip remaining versions
                except Exception:
                    pass

        # Start background keepalive thread to maintain the WebSocket during download
        self.keepalive_thread = threading.Thread(target=self.websocket_keepalive)
        self.keepalive_thread.daemon = True
        self.keepalive_thread.start()

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, *, challenge, **_):
        # Send the Widevine service certificate challenge via WebSocket
        # The browser sends CAQ= (\x08\x04) and gets the cert in the license field
        drm_token = base64.b64encode(challenge).decode()
        res = self.websocket_send({
            "_type": "widevineDrmLicenseRequest",
            "drmToken": drm_token,
            "editionId": self._current_edition_id if hasattr(self, '_current_edition_id') else "0",
            "requestCallbackId": 5,
            "userId": self.user_id,
        })
        if res.get("status", ["error"])[0] != "ok":
            self.log.warning(f" - Failed to get service certificate via WS: {res}")
            # Fallback to hardcoded certificate from config
            return self.config.get("certificate")
        cert = res["license"][0]
        try:
            cert_bytes = base64.b64decode(cert)
            self.log.debug(f" + Service certificate (decoded hex): {cert_bytes}...")
            self.log.debug(f" + Service certificate (decoded length): {len(cert_bytes)} bytes")
            self._log_raw_certificate(cert)
        except Exception:
            self.log.debug(f" + Service certificate (raw): {cert[:200]}")
        return cert

    def license(self, *, challenge, title, **_):
        """Request Widevine DRM license. Handles ad-supported content if needed."""
        self.keepalive_thread = None  # Signal the keepalive thread to stop

        if title.service_data.get("isAdvertEnabled") == ["true"]:
            advert_def_id = (
                title.service_data["advertContentDefinitions"][0]["advertContentDefinition"][0]
                ["advertContentDefinitionId"][0]
            )

            self.keepalive_thread = threading.Thread(target=self.websocket_keepalive)
            self.keepalive_thread.daemon = True
            self.keepalive_thread.start()

            self.log.info(" + Requesting adverts")

            res = self.api_request({
                "_type": "advertContentRequest",
                "accountId": self.user_id,
                "advertContentDefinitionId": advert_def_id,
                "claimedAppId": "html5app",
                "contentEncoding": "gzip",
                "editionFormat": "dash",
                "format": "application/json",
                "includeClickThrough": "true",
                "lightDeviceId": 1,
                "noCache": "true",
                "sessionKey": self.session_key,
            })
            self.log.debug(f" + Advert content response: {res}")
            if res["_type"] == "error":
                raise self.log.exit(f" - Failed to get adverts: {res['text'][0]}")

            adverts = res["advertContent"][0].get("advertStreamingSessions", [])
            if adverts:
                adverts = adverts[0]["advertStreamingSession"]

            for i, advert in enumerate(adverts):
                self.log.info(f" + Requesting advert {i + 1}/{len(adverts)}")

                for req in ("start", "stop"):
                    res2 = self.api_request({
                        "_type": f"advertStreamingSession{req.title()}",
                        "accountId": self.user_id,
                        "advertContentId": res["advertContent"][0]["advertContentId"][0],
                        "advertStreamingSessionId": advert["advertStreamingSessionId"][0],
                        "claimedAppId": "html5app",
                        "contentEncoding": "gzip",
                        "format": "application/json",
                        "noCache": "true",
                        "sessionKey": self.session_key,
                    })
                    self.log.debug(f" + Advert session {req} response: {res2}")
                    if res2["_type"] == "error":
                        raise self.log.exit(f" - Failed to {req} advert: {res2['text'][0]}")

                    if req == "start":
                        duration = int(advert["advert"][0]["lengthSeconds"][0])
                        self.log.info(f" + Waiting {duration} seconds for advert...")
                        time.sleep(duration)

            self.keepalive_thread = None  # Signal the thread to stop

            self.log.info(" + Adverts finished")
            res = self.api_request({
                "_type": "advertContentStreamingSessionStart",
                "accountId": self.user_id,
                "advertContentId": res["advertContent"][0]["advertContentId"][0],
                "claimedAppId": "html5app",
                "contentEncoding": "gzip",
                "format": "application/json",
                "noCache": "true",
                "sessionKey": self.session_key,
            })
            self.log.debug(f" + Advert streaming session start response: {res}")
            if res["_type"] == "error":
                self.log.warning(f" - Failed to start streaming session: {res['text'][0]}")

        # Send the Widevine license challenge via WebSocket
        drm_token = base64.b64encode(challenge).decode()
        res = self.websocket_send({
            "_type": "playReadyDrmLicenseRequest",
            "drmToken": drm_token,
            "editionId": title.service_data["edition_id"],
            "requestCallbackId": 5,
            "userId": self.user_id,
        })
        if res.get("status", ["error"])[0] != "ok":
            raise self.log.exit(f" - License request failed: {res}")
        return res["license"][0]

    # Service-specific functions

    @staticmethod
    def extract_json(r):
        """Strip Vudu's JSONP-like security wrapper and parse the JSON response."""
        return json.loads(r.text.replace("/*-secure-", "").replace("*/", ""))

    def api_request(self, params):
        """Send a POST request to the Vudu API endpoint with URL-encoded params."""
        return self.extract_json(self.session.post(self.config["endpoints"]["api"], data={
            "contentType": "application/x-vudu-url-note",
            "query": urllib.parse.urlencode(params),
        }))

    @staticmethod
    def _vudu_path_encode(value):
        """
        Encode a value for use in apicache URL paths.
        Uses *XX notation (like percent-encoding but with asterisk instead of %).
        """
        safe_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
        result = []
        for ch in str(value):
            if ch in safe_chars:
                result.append(ch)
            else:
                result.append(f"*{ord(ch):02X}")
        return "".join(result)

    def cache_request(self, params):
        """Make a GET request to apicache with URL-path-based parameters."""
        path_parts = []
        for k, v in params.items():
            if v is None:
                continue
            if k == "followup" and isinstance(v, list):
                for fu in v:
                    path_parts.append(f"followup/{self._vudu_path_encode(fu)}")
            else:
                path_parts.append(f"{k}/{self._vudu_path_encode(v)}")

        url = f"{self.config['endpoints']['cache']}/{'/'.join(path_parts)}"
        return self.extract_json(self.session.get(url))

    def websocket_send(self, params):
        self.log.debug(f"<< {params}")
        try:
            self.websocket.send(urllib.parse.urlencode(params))
            res = urllib.parse.parse_qs(self.websocket.recv())
        except Exception as e:
            self.log.warning(f" - WebSocket send/recv failed ({e}), reconnecting...")
            self._ensure_websocket()
            self.websocket.send(urllib.parse.urlencode(params))
            res = urllib.parse.parse_qs(self.websocket.recv())
        self.log.debug(f">> {res}")
        return res

    def websocket_keepalive(self):
        # NOTE: Technically it's usually the server that sends keepAliveRequests,
        # but the client sending them works too and was easier to implement.
        while self.keepalive_thread:
            res = self.websocket_send({"_type": "keepAliveRequest"})
            if res["_type"] != ["keepAliveResponse"]:
                raise ValueError("Did not receive keepAliveResponse from WebSocket")
            time.sleep(30)

    def get_session_keys(self):
        cache_path = self.get_cache(f"session_keys_{self.profile}.json")
        if os.path.isfile(cache_path):
            with open(cache_path, encoding="utf-8") as fd:
                session_keys = json.load(fd)
            if datetime.strptime(session_keys["expirationTime"][0], "%Y-%m-%d %H:%M:%S.%f") > datetime.utcnow():
                self.log.info(" + Using cached session keys")
                return session_keys

        self.log.info(" + Logging in")

        # Compute loginSignature (HMAC-SHA1) like unshackle FATH does
        login_sig = None
        try:
            pem = self._get_playback_error_message()
            login_sig = self._login_signature(
                data=self.credentials.username + self.credentials.password,
                key_string=pem,
            )
        except Exception as e:
            self.log.warning(f" - Could not compute loginSignature: {e}")

        # Try multiple claimedAppId values; macWeb is what the browser uses
        for app_id in ["macWeb", "vuduAndroid", "android", "roku", "iphone"]:
            params = {
                "_type": "sessionKeyRequest",
                "claimedAppId": app_id,
                "contentEncoding": "gzip",
                "followup": "user",
                "format": "application/json",
                "noCache": "true",
                "password": self.credentials.password,
                "userName": self.credentials.username,
                "weakSeconds": 2592000,
            }
            if login_sig:
                params["loginSignature"] = login_sig

            try:
                res = self.api_request(params)
                self.log.debug(f" + Login attempt with {app_id}: {res}")
                if res.get("status") == ["success"]:
                    self.log.info(f" + Login succeeded with claimedAppId={app_id}")
                    session_keys = res["sessionKey"][0]

                    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                    with open(cache_path, "w", encoding="utf-8") as fd:
                        json.dump(session_keys, fd)

                    return session_keys
                self.log.debug(f" + Login with {app_id} returned status: {res.get('status')}")
            except Exception as e:
                self.log.debug(f" + Login with {app_id} failed: {e}")
                continue

        raise self.log.exit(" - Login failed with all claimedAppId values")

    @staticmethod
    def _login_signature(data, key_string):
        """
        Compute the Vudu login signature using HMAC-SHA1.
        The key is derived from a 32-char string: key[i] = (charCode[i] * (i+1)) ^ charCode[31-i]
        for i in 0..15, then used as HMAC-SHA1 key.
        """
        if len(key_string) < 32:
            key_string = key_string.ljust(32, "\x00")

        key_bytes = bytearray(16)
        for i in range(16):
            key_bytes[i] = (ord(key_string[i]) * (i + 1) ^ ord(key_string[31 - i])) & 0xFF

        return hmac.new(key_bytes, data.encode(), hashlib.sha1).hexdigest()

    def _get_playback_error_message(self):
        """Fetch the playbackErrorMessage from server-owned config (used as HMAC key seed)."""
        res = self.extract_json(self.session.get(
            "https://apicache.vudu.com/api2/_type/serverOwnedConfigGet/contentEncoding/gzip/domain/fandango/format/application*2Fjson"
        ))
        for entry in res.get("entry", []):
            key = entry.get("key", [""])[0] if isinstance(entry.get("key"), list) else entry.get("key", "")
            if key == "playbackErrorMessage":
                val = entry.get("value", [""])[0] if isinstance(entry.get("value"), list) else entry.get("value", "")
                return val
        raise RuntimeError("Could not retrieve playbackErrorMessage from server config")

    def get_light_device_key(self):
        """Request a light device key and wrap it with AES for WebSocket auth."""
        # TODO: Cache this, it seems to never change for an account
        res = self.api_request({
            "_type": "lightDeviceRequest",
            "accountId": self.user_id,
            "claimedAppId": "html5app",
            "contentEncoding": "gzip",
            "domain": "vudu",
            "format": "application/json",
            "lightDeviceType": "html5app",
            "noCache": "true",
            "sessionKey": self.session_key,
        })
        self.log.debug(f" + lightDeviceRequest response: {res}")
        if res["_type"] == "error":
            raise self.log.exit(f" - Failed to get lightDeviceKey: {res['text'][0]}")

        # Store the actual lightDeviceId from the API
        self.light_device_id = res["lightDevice"][0]["lightDeviceId"][0]
        self.log.debug(f" + lightDeviceId: {self.light_device_id}")

        # AES-CBC wrap the raw device key using the configured AES key (zero IV)
        raw_key = bytes.fromhex(res["lightDevice"][0]["lightDeviceKey"][0])
        cipher = AES.new(bytes.fromhex(self.config["aes_key"]), AES.MODE_CBC, b"\x00" * 16)
        wrapped_key = cipher.encrypt(pad(raw_key, 32))
        return f"A{wrapped_key.hex()}"

    def _kickstart(self):
        """Get the WebSocket server URL from the kickstart endpoint (like FATH)."""
        resp = self.session.post(
            url="https://startup.vudu.com/kickstart/webSocketLightDeviceQuery",
            data=f"accountId={self.user_id}&lightDeviceId={self.light_device_id}&sessionKey={self.session_key}",
            headers={
                "Content-Type": "text/plain;charset=UTF-8",
                "Origin": "https://athome.fandango.com",
                "Referer": "https://athome.fandango.com/",
            },
        )
        self.log.debug(f" + Kickstart response: status={resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        self.log.debug(f" + Kickstart response data: {data}")
        server = data.get("server", [None])
        if isinstance(server, list):
            server = server[0]
        if not server:
            raise self.log.exit(" - Failed to get WebSocket server URL from kickstart")
        return server

    def _ensure_websocket(self):
        """Check if WebSocket is still alive, reconnect if needed."""
        try:
            if self.websocket and self.websocket.connected:
                self.websocket.ping()
                return
        except Exception:
            pass

        self.log.info(" + Reconnecting WebSocket...")
        try:
            if self.websocket:
                self.websocket.close()
        except Exception:
            pass

        # Retry loop â€” HTTP session may also be stale
        import requests as req_lib
        last_err = None
        for attempt in range(3):
            try:
                if attempt > 0:
                    self.log.info(f" + Reconnect attempt {attempt + 1}/3...")
                    time.sleep(2)
                    # Force close stale HTTP connections
                    self.session.close()
                    self.session.headers.update({
                        "Accept": "application/json",
                        "Origin": "https://www.vudu.com",
                        "Referer": "https://www.vudu.com/",
                    })

                light_device_key = self.get_light_device_key()

                proxy_ = self.get_proxy(self.proxy or self.GEOFENCE[0])
                proxy = urllib.parse.urlparse(proxy_) if proxy_ else None
                ws_url = self._kickstart()

                self.websocket = websocket.create_connection(
                    ws_url,
                    http_proxy_host=proxy.hostname if proxy else None,
                    http_proxy_port=proxy.port if proxy else None,
                    http_proxy_auth=(
                        urllib.parse.unquote(proxy.username) if proxy.username else None,
                        urllib.parse.unquote(proxy.password) if proxy.password else None
                    ) if proxy else None
                )

                res = self.websocket_send({
                    "_type": "lightDeviceLoginQuery",
                    "accountId": self.user_id,
                    "lightDeviceId": self.light_device_id,
                    "lightDeviceKey": light_device_key,
                    "sessionKey": self.session_key
                })
                if res["status"] != ["ok"]:
                    raise self.log.exit(f" - WebSocket re-authentication failed: {res}")
                self.log.info(" + WebSocket reconnected")
                return
            except (req_lib.exceptions.ConnectionError, ConnectionError, OSError) as e:
                last_err = e
                self.log.warning(f" - Reconnect attempt failed: {e}")
                continue

        raise self.log.exit(f" - WebSocket reconnection failed after 3 attempts: {last_err}")

    def configure(self):
        """Initialize HTTP session, authenticate, and open WebSocket connection."""
        self.session.headers.update({
            "Accept": "application/json",
            "Origin": "https://www.vudu.com",
            "Referer": "https://www.vudu.com/",
        })

        # Authenticate and extract user/session info
        session_keys = self.get_session_keys()
        self.user_id = session_keys["user"][0]["userId"][0]
        self.session_key = session_keys["sessionKey"][0]

        light_device_key = self.get_light_device_key()

        self.log.info(" + Opening WebSocket connection")
        proxy_ = self.get_proxy(self.proxy or self.GEOFENCE[0])
        proxy = urllib.parse.urlparse(proxy_) if proxy_ else None

        # Get dynamic WebSocket URL via kickstart (like FATH)
        ws_url = self._kickstart()

        self.websocket = websocket.create_connection(
            ws_url,
            http_proxy_host=proxy.hostname if proxy else None,
            http_proxy_port=proxy.port if proxy else None,
            http_proxy_auth=(
                urllib.parse.unquote(proxy.username) if proxy.username else None,
                urllib.parse.unquote(proxy.password) if proxy.password else None
            ) if proxy else None
        )

        self.log.info(" + Authenticating with session keys")
        res = self.websocket_send({
            "_type": "lightDeviceLoginQuery",
            "accountId": self.user_id,
            "lightDeviceId": self.light_device_id,
            "lightDeviceKey": light_device_key,
            "sessionKey": self.session_key
        })
        if res["status"] != ["ok"]:
            raise self.log.exit(f" - WebSocket authentication failed: {res['errorDescription'][0]}")
    
    def save_json_response(self, data: dict, filename: str = "response.json"):
        """Dump API response data to a JSON file for debugging."""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            self.log.debug(f" + Saved JSON response to {filename}")
        except Exception as e:
            self.log.error(f" - Failed to save JSON: {e}")

    def _log_raw_certificate(self, cert_b64: str):
        """Decodes and logs key identifiers from the raw Widevine certificate."""
        import re
        try:
            raw_cert = base64.b64decode(cert_b64)
            
            # 1. Check for the Provider ID (e.g., vudu.com)
            # We look for printable ASCII sequences to find the domain
            provider_match = re.search(rb"[a-z0-9.-]+\.[a-z]{2,}", raw_cert)
            provider = provider_match.group(0).decode() if provider_match else "Unknown"
            
            # 2. Identify the Message Type
            # \x08\x05 (Field 1, Type 5) indicates a Service Certificate
            msg_type = "Service Certificate" if raw_cert.startswith(b"\x08\x05") else "Unknown Message"
            
            self.log.info(f"[Raw DRM Data] Type: {msg_type} | Provider: {provider}")
            self.log.debug(f"[Raw Binary] {raw_cert[:50]}...") # Log start of binary for verification
            
        except Exception as e:
            self.log.error(f"Failed to decode raw certificate data: {e}")