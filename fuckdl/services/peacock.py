import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import datetime
from urllib.parse import unquote

import click
import requests

from fuckdl.objects import Title, Tracks
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.regex import find
from fuckdl.utils.widevine.device import LocalDevice


class Peacock(BaseService):
    """
    Service code for NBC's Peacock streaming service (https://peacocktv.com).
    Updated By @AnotherBigUserHere
    V3.1
    
    + More details abouth token and Verification
    + Better handling of Widevine and playready of their licence
    + Shows the manifest, Content ID, and a Variant ID
    + Adapted of a selling that i do, but enjoy this new rewrite of the service
    + Corrected H265 Retrieving, thanks @thecrew_wh by the python script that i modified
    to get the mdp url fixed
    + Finally solved personas endpoint, removing totally of the code, i see the @thecrew_wh script
    version to fix this finally
    + Added cookie decoding support for encrypted/encoded cookies (skyUMV, peacock_session, etc.)
    (a big thanks to @rosmander, to their skyshowtime script of unshackle that i used to fix mine)

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026

    \b
    Authorization: Cookies
    Security: 
    2160p - L1 Widevine
    1080p - SL3000/L1 Widevine and Playready
    720 down to 480p - SL2000/L3 Widevine and playready

    + L3 and SL2000 can up to 1080p but itÂ´s better SL3000 for that
    + L1 must be not totally revoked to 4K and HDR/DV 
    + To H265 Codec, must be L1, to get licence working, and not totally revoked

    \b
    Tips: - The library of contents can be viewed without logging in at https://www.peacocktv.com/stream/tv
            See the footer for links to movies, news, etc. A US IP is required to view.
    """

    ALIASES = ["PCOK", "peacock"]
    TITLE_RE = [
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>movies/[a-z0-9/./-]+/[a-f0-9-]+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>sports/[a-z0-9/./-]+/[a-f0-9-]+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>tv/[a-z0-9/./-]+/[a-f0-9-]+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>tv/[a-z0-9-/.]+/\d+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>news/[a-z0-9/./-]+/[a-f0-9-]+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>news/[a-z0-9-/.]+/\d+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/watch/asset/|/?)(?P<id>-/[a-z0-9-/.]+/\d+)",
        r"(?:https?://(?:www\.)?peacocktv\.com/stream-tv/)?(?P<id>[a-z0-9-/.]+)",
    ]

    @staticmethod
    @click.command(name="Peacock", short_help="https://peacocktv.com")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Peacock(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        super().__init__(ctx)
        self.parse_title(ctx, title)
        self.movie = movie
        self.cdm = ctx.obj.cdm
        self.profile = ctx.obj.profile

        self.service_config = None
        self.hmac_key = None
        self.tokens = None
        self.license_api = None
        self.license_bt = None
        self.vcodec = ctx.parent.params["vcodec"]
        self.vrange = ctx.parent.params["range_"]
        
        if self.vrange in ["HDR10", "DV", "HYBRID"] and self.vcodec != "H265":
            self.log.info(f" + Switched video codec to H265 to be able to get {self.vrange} dynamic range")
            self.vcodec = "H265"

        self.configure()

    def _decode_cookie_value(self, cookie_value: str) -> str:
        """
        Decode a cookie value that might be URL-encoded or base64-encoded.
        Similar to SkyShowtime's unquote() approach.
        
        Args:
            cookie_value: The raw cookie value from the cookie jar
            
        Returns:
            Decoded cookie value ready for use in API headers
        """
        if not cookie_value:
            return cookie_value
            
        original_value = cookie_value
        
        # Step 1: URL-decode the cookie (handles %2B, %2F, %3D, etc.)
        try:
            cookie_value = unquote(cookie_value)
            if cookie_value != original_value:
                self.log.debug(f"Cookie was URL-decoded: {original_value[:20]}... -> {cookie_value[:20]}...")
        except Exception as e:
            self.log.debug(f"URL decode failed: {e}")
        
        # Step 2: Check if it's base64 encoded (some cookie exporters base64 encode the token)
        # Try to detect base64 by checking for typical base64 characters and length
        if len(cookie_value) > 20 and all(c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=' for c in cookie_value):
            try:
                # Try to decode as base64
                decoded_bytes = base64.b64decode(cookie_value)
                # Check if decoded result looks like a valid token (printable ASCII)
                try:
                    decoded_str = decoded_bytes.decode('utf-8')
                    # Verify it's not binary garbage and looks like a token
                    if all(32 <= ord(c) < 127 or c in '\n\r\t' for c in decoded_str[:50]):
                        self.log.debug(f"Cookie was base64-decoded")
                        cookie_value = decoded_str
                except UnicodeDecodeError:
                    # If it's binary data, keep original base64
                    pass
            except Exception as e:
                self.log.debug(f"Base64 decode failed: {e}")
        
        return cookie_value

    def _get_cookie_value(self, name: str) -> str:
        """
        Get a cookie value from the session cookie jar with automatic decoding.
        
        Args:
            name: Name of the cookie to retrieve
            
        Returns:
            Decoded cookie value or None if not found
        """
        if not self.session.cookies:
            return None
            
        for cookie in self.session.cookies:
            if cookie.name == name:
                return self._decode_cookie_value(cookie.value)
        return None

    def authenticate(self, cookies=None, credential=None):
        """
        Override authenticate to handle encoded cookies properly.
        """
        # Call parent authenticate first
        super().authenticate(cookies, credential)
        
        # After cookies are loaded, decode any encoded cookies
        # The main cookie we care about is the session cookie that gives us userToken
        # In Peacock, this might be 'skyUMV' or similar
        
        # Check for skyUMV cookie (used in SkyShowtime and possibly Peacock)
        sky_umv = self._get_cookie_value("skyUMV")
        if sky_umv:
            self.log.info("Found and decoded skyUMV cookie")
            # Store the decoded token for later use
            self._decoded_sky_umv = sky_umv
        else:
            self._decoded_sky_umv = None
        
        # Check for other common Peacock cookies that might be encoded
        peacock_session = self._get_cookie_value("peacock_session")
        if peacock_session:
            self.log.info("Found and decoded peacock_session cookie")
            self._decoded_session = peacock_session
        
        # Also check for auth_token if present
        auth_token = self._get_cookie_value("auth_token")
        if auth_token:
            self.log.info("Found and decoded auth_token cookie")
            self._decoded_auth_token = auth_token

    def get_titles(self):
        # Title is a slug, e.g. `/tv/the-office/4902514835143843112` or just `the-office`

        if "/" not in self.title:
            r = self.session.get(self.config["endpoints"]["stream_tv"].format(title_id=self.title))
            self.title = find("/watch/asset(/[^']+)", r.text)
            if not self.title:
                raise self.log.exit(" - Title ID not found or invalid")

        if not self.title.startswith("/"):
            self.title = f"/{self.title}"

        if self.title.startswith("/movies/"):
            self.movie = True

        res = self.session.get(
            url=self.config["endpoints"]["node"],
            params={
                "slug": self.title,
                "represent": "(items(items))"
            },
            headers={
                "Accept": "*",
                "Referer": f"https://www.peacocktv.com/watch/asset{self.title}",
                "X-SkyOTT-Device": self.config["client"]["device"],
                "X-SkyOTT-Platform": self.config["client"]["platform"],
                "X-SkyOTT-Proposition": self.config["client"]["proposition"],
                "X-SkyOTT-Provider": self.config["client"]["provider"],
                "X-SkyOTT-Territory": self.config["client"]["territory"],
                "X-SkyOTT-Language": "en"
            }
        ).json()

        if self.movie:
            return Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=res["attributes"]["title"],
                year=res["attributes"]["year"],
                source=self.ALIASES[0],
                service_data=res,
            )
        else:
            titles = []
            for season in res["relationships"]["items"]["data"]:
                for episode in season["relationships"]["items"]["data"]:
                    titles.append(episode)
            return [Title(
                id_=self.title,
                type_=Title.Types.TV,
                name=res["attributes"]["title"],
                year=x["attributes"].get("year"),
                season=x["attributes"].get("seasonNumber"),
                episode=x["attributes"].get("episodeNumber"),
                episode_name=x["attributes"].get("title"),
                source=self.ALIASES[0],
                service_data=x
            ) for x in titles]
            
    def get_tracks(self, title):
        # Keep it simple like the original
        supported_colour_spaces = ["SDR"]
        
        if self.vrange in ["HDR10", "DV", "HYBRID"]:
            self.log.warning(f" + {self.vrange} requested but device may not support it, falling back to SDR")
        
        if self.vcodec == "H265":
            self.log.info(" + Using H.265 video codec")
        else:
            self.log.info(" + Using H.264 video codec")
        
        content_id = title.service_data["attributes"]["formats"]["HD"]["contentId"]
        variant_id = title.service_data["attributes"]["providerVariantId"]
        
        self.log.info(f" + Content ID: {content_id}")
        self.log.info(f" + Variant ID: {variant_id}")
        self.log.info(f" + DRM System: {'PlayReady' if self.cdm.device.type == LocalDevice.Types.PLAYREADY else 'Widevine'}")
    
        sky_headers = {
            "X-SkyOTT-Agent": ".".join([
                self.config["client"]["proposition"].lower(),
                self.config["client"]["device"].lower(),
                self.config["client"]["platform"].lower()
            ]),
            "X-SkyOTT-PinOverride": "false",
            "X-SkyOTT-Provider": self.config["client"]["provider"],
            "X-SkyOTT-Territory": self.config["client"]["territory"],
            "X-SkyOTT-UserToken": self.tokens["userToken"]
        }
    
        body = json.dumps({
            "device": {
                "capabilities": [
                    {
                        "protection": "PLAYREADY" if self.cdm.device.type == LocalDevice.Types.PLAYREADY else "WIDEVINE",
                        "container": "ISOBMFF",
                        "transport": "DASH",
                        "acodec": "AAC",
                        "vcodec": "H265" if self.vcodec == "H265" else "H264",
                    }
                ],
                "maxVideoFormat": "UHD" if self.vcodec == "H265" else "HD",
                "supportedColourSpaces": supported_colour_spaces,
                "model": self.config["client"]["platform"],
                "hdcpEnabled": "true"
            },
            "client": {
                "thirdParties": ["FREEWHEEL", "YOSPACE"]
            },
            "contentId": content_id,
            "providerVariantId": variant_id,
            "parentalControlPin": "null",
            "personaParentalControlRating": 9,
        }, separators=(",", ":"))
    
        manifest = self.session.post(
            url=self.config["endpoints"]["vod"],
            data=body,
            headers=dict(**sky_headers, **{
                "Accept": "application/vnd.playvod.v1+json",
                "Content-Type": "application/vnd.playvod.v1+json",
                "X-Sky-Signature": self.create_signature_header(
                    method="POST",
                    path="/video/playouts/vod",
                    sky_headers=sky_headers,
                    body=body,
                    timestamp=int(time.time())
                )
            })
        ).json()
    
        if "errorCode" in manifest:
            raise self.log.exit(f" - An error occurred: {manifest['description']} [{manifest['errorCode']}]")
    
        self.license_api = manifest["protection"]["licenceAcquisitionUrl"]
        self.license_bt = manifest["protection"]["licenceToken"]
        
        self.log.debug(f" + License URL: {self.license_api}")
        manifest_url = manifest["asset"]["endpoints"][0]["url"]
        self.log.info(f" + Manifest URL: {manifest_url}")
    
        tracks = Tracks.from_mpd(
            url=manifest_url,
            session=self.session,
            source=self.ALIASES[0]
        )
        
        for track in tracks.videos:
            track.needs_proxy = False
    
        for track in tracks.audios:
            track.needs_proxy = False
            if track.language.territory == "AD":
                track.language.territory = None
    
        for track in tracks.subtitles:
            track.needs_proxy = False
    
        return tracks

    def _get_tracks_for_color_space(self, title, supported_colour_spaces, range_name, max_format):
        """Helper method to fetch tracks for a specific color space configuration"""
        content_id = title.service_data["attributes"]["formats"]["HD"]["contentId"]
        variant_id = title.service_data["attributes"]["providerVariantId"]
        
        self.log.info(f" + Content ID: {content_id}")
        self.log.info(f" + Variant ID: {variant_id}")
        self.log.info(f" + DRM System: {'PlayReady' if self.cdm.device.type == LocalDevice.Types.PLAYREADY else 'Widevine'}")

        sky_headers = {
            # order of these matter!
            "X-SkyOTT-Agent": ".".join([
                self.config["client"]["proposition"].lower(),
                self.config["client"]["device"].lower(),
                self.config["client"]["platform"].lower()
            ]),
            "X-SkyOTT-PinOverride": "false",
            "X-SkyOTT-Provider": self.config["client"]["provider"],
            "X-SkyOTT-Territory": self.config["client"]["territory"],
            "X-SkyOTT-UserToken": self.tokens["userToken"]
        }

        body = json.dumps({
            "device": {
                "capabilities": [
                    {
                        "protection": "PLAYREADY" if self.cdm.device.type == LocalDevice.Types.PLAYREADY else "WIDEVINE",
                        "container": "ISOBMFF",
                        "transport": "DASH",
                        "acodec": "AAC",
                        "vcodec": "H265" if self.vcodec == "H265" else "H264",
                    },
                    {
                        "protection": "PLAYREADY" if self.cdm.device.type == LocalDevice.Types.PLAYREADY else "WIDEVINE",
                        "container": "ISOBMFF",
                        "transport": "DASH",
                        "acodec": "AAC",
                        "vcodec": "H265" if self.vcodec == "H265" else "H264",
                    }
                ],
                "maxVideoFormat": max_format,  # Use the format passed in
                "supportedColourSpaces": supported_colour_spaces,
                "model": self.config["client"]["platform"],
                "hdcpEnabled": "true"
            },
            "client": {
                "thirdParties": ["FREEWHEEL", "YOSPACE"]
            },
            "contentId": content_id,
            "providerVariantId": variant_id,
            "parentalControlPin": "null",
            "personaParentalControlRating": 9,
        }, separators=(",", ":"))

        manifest_data = self._get_manifest_with_retry(sky_headers, body)

        # Store license info (will be overwritten if multiple requests, but should be the same)
        self.license_api = manifest_data["protection"]["licenceAcquisitionUrl"]
        self.license_bt = manifest_data["protection"]["licenceToken"]
        
        self.log.debug(f" + License URL: {self.license_api}")
        if self.license_bt:
            self.log.debug(f" + License token available: {self.license_bt[:20]}...")

        manifest_url = manifest_data["asset"]["endpoints"][0]["url"]
        self.log.info(f" + Manifest URL: {manifest_url}")

        tracks = Tracks.from_mpd(
            url=manifest_url,
            session=self.session,
            source=self.ALIASES[0]
        )
        
        
        # Tag video tracks with appropriate HDR metadata and make them unique
        for track in tracks.videos:
            track.needs_proxy = False
            # Modify the track ID to make it unique per color space
            # This prevents deduplication when merging tracks
            if range_name == "HDR10":
                track.id = f"{track.id}-hdr10"
                track.hdr10 = True
                track.dolbyvision = False
                # Update the internal range attribute for proper display
                if hasattr(track, 'range'):
                    track.range = "HDR10"
                
                # Safely get resolution for logging
                resolution = "Unknown"
                if hasattr(track, 'height') and track.height:
                    resolution = f"{track.height}p"
                
            elif range_name == "DV":
                track.id = f"{track.id}-dv"
                track.dolbyvision = True
                track.hdr10 = False
                # Update the internal range attribute for proper display
                if hasattr(track, 'range'):
                    track.range = "DV"
                
                # Safely get resolution for logging
                resolution = "Unknown"
                if hasattr(track, 'height') and track.height:
                    resolution = f"{track.height}p"
                
            else:
                track.hdr10 = False
                track.dolbyvision = False
                
                # Safely get resolution for logging
                resolution = "Unknown"
                if hasattr(track, 'height') and track.height:
                    resolution = f"{track.height}p"

        for track in tracks.audios:
            track.needs_proxy = False
            if hasattr(track, 'language') and hasattr(track.language, 'territory') and track.language.territory == "AD":
                # This is supposed to be Audio Description, not Andorra
                track.language.territory = None

        for track in tracks.subtitles:
            track.needs_proxy = False

        return tracks

    def _get_manifest_with_retry(self, sky_headers, body, max_retries=3):
        """Get manifest with retry logic for better reliability"""
        
        for attempt in range(max_retries):
            try:
                self.log.debug(f" + Fetching manifest (attempt {attempt + 1}/{max_retries})")
                
                response = self.session.post(
                    url=self.config["endpoints"]["vod"],
                    data=body,
                    headers=dict(**sky_headers, **{
                        "Accept": "application/vnd.playvod.v1+json",
                        "Content-Type": "application/vnd.playvod.v1+json",
                        "X-Sky-Signature": self.create_signature_header(
                            method="POST",
                            path="/video/playouts/vod",
                            sky_headers=sky_headers,
                            body=body,
                            timestamp=int(time.time())
                        )
                    })
                )
                
                if response.status_code != 200:
                    self.log.warning(f"Manifest request failed: HTTP {response.status_code}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        self.log.info(f" + Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
                
                manifest_data = response.json()
                
                if "errorCode" in manifest_data:
                    error_msg = f"API Error: {manifest_data.get('description', 'Unknown error')} [{manifest_data['errorCode']}]"
                    self.log.error(error_msg)
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        self.log.info(f" + Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(error_msg)
                
                return manifest_data
                
            except requests.exceptions.RequestException as e:
                self.log.warning(f"Network error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.log.info(f" + Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
        
        raise Exception("Failed to fetch manifest after all retries")

    def get_chapters(self, title):
        return []
        
    def certificate(self, challenge, **_):
        """Handle certificate challenge based on CDM type"""
        _is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                        (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                         self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        if _is_playready:
            self.log.debug(" + Requesting PlayReady certificate")
            # PlayReady doesn't need a separate certificate request
            return []
        else:
            self.log.debug(" + Requesting Widevine certificate")
            # First challenge (small size) is for certificate
            try:
                # Extract path from license URL for signature
                if "://" in self.license_api:
                    if self.license_api.count("://") > 1:
                        path_parts = self.license_api.split("://", 2)
                        path = "/" + path_parts[2].split("/", 1)[1] if len(path_parts) > 2 else "/"
                    else:
                        path = "/" + self.license_api.split("://", 1)[1].split("/", 1)[1]
                else:
                    path = self.license_api
            except Exception as e:
                self.log.warning(f"Could not parse license URL path: {e}")
                path = "/wvls"
            
            signature = self.create_signature_header(
                method="POST",
                path=path,
                sky_headers={},
                body="",
                timestamp=int(time.time())
            )
            
            headers = {
                "Accept": "*/*",
                "Content-Type": "application/octet-stream",
                "X-Sky-Signature": signature
            }
            
            if hasattr(self, 'license_bt') and self.license_bt:
                headers["X-SkyOTT-LicenseToken"] = self.license_bt
            
            response = self.session.post(
                url=self.license_api,
                headers=headers,
                data=challenge
            )
            
            if response.status_code != 200:
                self.log.error(f"Certificate request failed: HTTP {response.status_code}")
                if response.text:
                    self.log.error(f"Response: {response.text[:200]}")
                raise Exception(f"Certificate request failed: {response.status_code}")
            
            return response.content

    def license(self, challenge, **_):
        """Handle license request for both PlayReady and Widevine"""
        
        # Extract path from license URL for signature
        try:
            # Handle different URL formats
            if "://" in self.license_api:
                if self.license_api.count("://") > 1:
                    # Handle case with multiple protocols (e.g., https://something://)
                    path_parts = self.license_api.split("://", 2)
                    path = "/" + path_parts[2].split("/", 1)[1] if len(path_parts) > 2 else "/"
                else:
                    # Standard URL
                    path = "/" + self.license_api.split("://", 1)[1].split("/", 1)[1]
            else:
                path = self.license_api
        except Exception as e:
            self.log.warning(f"Could not parse license URL path: {e}")
            path = "/wvls"  # Default path for Widevine
            if self.cdm.device.type == LocalDevice.Types.PLAYREADY:
                path = "/prls"  # Default path for PlayReady
        
        signature = self.create_signature_header(
            method="POST",
            path=path,
            sky_headers={},
            body="",
            timestamp=int(time.time())
        )
        
        # Set headers based on DRM type
        if self.cdm.device.type == LocalDevice.Types.PLAYREADY:
            # PlayReady expects XML content type
            headers = {
                "Accept": "*/*",
                "Content-Type": "text/xml; charset=utf-8",
                "X-Sky-Signature": signature
            }
            self.log.debug(" + Using PlayReady with Content-Type: text/xml")
        else:
            # Widevine expects binary content type
            headers = {
                "Accept": "*/*",
                "Content-Type": "application/octet-stream",
                "X-Sky-Signature": signature
            }
            self.log.debug(" + Using Widevine with Content-Type: application/octet-stream")
        
        # Add license token if available
        if hasattr(self, 'license_bt') and self.license_bt:
            headers["X-SkyOTT-LicenseToken"] = self.license_bt
            self.log.debug(f" + Using license token: {self.license_bt[:20]}...")
        
        self.log.debug(f" + Requesting license from: {self.license_api}")
        self.log.debug(f" + License type: {'PlayReady' if self.cdm.device.type == LocalDevice.Types.PLAYREADY else 'Widevine'}")
        self.log.debug(f" + Challenge size: {len(challenge)} bytes")
        
        try:
            response = self.session.post(
                url=self.license_api,
                headers=headers,
                data=challenge
            )
            
            if response.status_code != 200:
                error_msg = f"License request failed: HTTP {response.status_code}"
                self.log.error(error_msg)
                
                # Try to parse error response
                try:
                    error_data = response.json()
                    self.log.error(f"Error details: {error_data}")
                    if "errorCode" in error_data:
                        if error_data["errorCode"] == "OVP_00117":
                            self.log.error("Robustness failure - Your CDM does not meet security requirements")
                            self.log.error("This content requires L1 (hardware-level) Widevine")
                        elif error_data["errorCode"] == "OVP_00114":
                            self.log.error("Unsupported browser/client - Invalid device identification")
                    raise Exception(f"{error_msg}: {error_data}")
                except:
                    pass
                
                response.raise_for_status()
            
            # Check if response is encrypted (license) or error message
            content_type = response.headers.get('Content-Type', '')
            if 'json' in content_type.lower() and len(response.content) < 1000:
                # Probably an error message
                error_data = response.json()
                if "errorCode" in error_data:
                    self.log.error(f"License server error: {error_data}")
                    if error_data["errorCode"] == "OVP_00117":
                        self.log.error("Try using PlayReady instead of Widevine, or use a higher-level CDM")
                raise Exception(f"License error: {error_data}")
            
            self.log.debug(f" + Received license: {len(response.content)} bytes")
            return response.content
                
        except requests.exceptions.RequestException as e:
            self.log.error(f"License request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.log.error(f"Response status: {e.response.status_code}")
                self.log.error(f"Response body: {e.response.text[:500]}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error during license request: {e}")
            raise

    def configure(self):
        self.session.headers.update({"Origin": "https://www.peacocktv.com"})
        self.log.info("Getting Peacock Client configuration")
        if self.config["client"]["platform"] != "PC":
            self.service_config = self.session.get(
                url=self.config["endpoints"]["config"].format(
                    territory=self.config["client"]["territory"],
                    provider=self.config["client"]["provider"],
                    proposition=self.config["client"]["proposition"],
                    device=self.config["client"]["platform"],
                    version=self.config["client"]["config_version"],
                )
            ).json()
        self.hmac_key = bytes(self.config["security"]["signature_hmac_key_v4"], "utf-8")
        
        self.log.info("Getting Authorization Tokens")
        self.tokens = self.get_tokens()
        
        # Display session information
        self.log.info("Fetching session data")
        session_info = self.get_session_info()
        
        # Log profile, account ID, and session ID (already logged in get_session_info)
        if not session_info:
            self.log.warning(" + Could not retrieve detailed session information")
        
        self.log.info("Verifying Authorization Tokens")
        if not self.verify_tokens():
            raise self.log.exit(" - Failed! Cookies might be outdated.")

    def get_session_info(self):
        """Get session information including available data from the user info endpoint"""
        sky_headers = {
            "X-SkyOTT-Device": self.config["client"]["device"],
            "X-SkyOTT-Platform": self.config["client"]["platform"],
            "X-SkyOTT-Proposition": self.config["client"]["proposition"],
            "X-SkyOTT-Provider": self.config["client"]["provider"],
            "X-SkyOTT-Territory": self.config["client"]["territory"],
            "X-SkyOTT-UserToken": self.tokens["userToken"]
        }
        
        try:
            response = self.session.get(
                url=self.config["endpoints"]["me"],
                headers=dict(**sky_headers, **{
                    "Accept": "application/vnd.userinfo.v2+json",
                    "Content-Type": "application/vnd.userinfo.v2+json",
                    "X-Sky-Signature": self.create_signature_header(
                        method="GET",
                        path="/auth/users/me",
                        sky_headers=sky_headers,
                        body="",
                        timestamp=int(time.time())
                    )
                })
            )
            
            # Check if we got a valid response
            if response.status_code != 200:
                self.log.warning(f"Session info request failed: HTTP {response.status_code}")
                return None
            
            user_info = response.json()
            
            # Extract session information from response headers
            session_id = response.headers.get('X-Skyint-Requestid', 'N/A')
            
            # Build account tier information from entitlements
            account_tiers = []
            if "entitlements" in user_info:
                for entitlement in user_info.get("entitlements", []):
                    if entitlement.get("state") == "ACTIVATED":
                        account_tiers.append(entitlement.get("name", "N/A"))
            
            # Build account features from segmentation
            account_features = {
                "video": [],
                "content": [],
                "discovery": []
            }
            
            if "segmentation" in user_info:
                segmentation = user_info.get("segmentation", {})
                
                # Video/account features
                for item in segmentation.get("account", []):
                    if "name" in item:
                        account_features["video"].append(item["name"])
                
                # Content entitlements
                for item in segmentation.get("content", []):
                    if "name" in item:
                        account_features["content"].append(item["name"])
                
                # Discovery features
                for item in segmentation.get("discovery", []):
                    if "name" in item:
                        account_features["discovery"].append(item["name"])
            
            # Create a profile name from available data
            profile_name = "N/A"
            if account_tiers:
                # Use the first active entitlement as profile indicator
                profile_name = account_tiers[0]
            elif account_features["video"]:
                # Or use the highest video tier
                video_tiers = account_features["video"]
                if "UHD" in video_tiers:
                    profile_name = "UHD Premium"
                elif "PREMIUM" in video_tiers:
                    profile_name = "Premium"
                elif "HD" in video_tiers:
                    profile_name = "HD"
                else:
                    profile_name = video_tiers[0] if video_tiers else "Standard"
            
            # Create a composite account ID from territory and entitlements
            account_id_parts = []
            if "homeTerritory" in user_info:
                account_id_parts.append(user_info["homeTerritory"])
            if "providerTerritory" in user_info:
                account_id_parts.append(user_info["providerTerritory"])
            if account_tiers:
                # Use the first entitlement as part of account ID
                account_id_parts.append(account_tiers[0].split("_")[0] if "_" in account_tiers[0] else account_tiers[0])
            
            account_id = "-".join(account_id_parts) if account_id_parts else "N/A"
            
            # Log detailed session information
            self.log.info(" + Session Information:")
            self.log.info(f" + Profile: {profile_name}")
            self.log.info(f" + Account ID: {account_id}")
            self.log.info(f" + Session ID: {session_id}")
            
            # Return comprehensive session info
            return {
                'profile': profile_name,
                'accountId': account_id,
                'sessionId': session_id,
                'homeTerritory': user_info.get('homeTerritory', 'N/A'),
                'currentTerritory': user_info.get('currentLocationTerritory', 'N/A'),
                'providerTerritory': user_info.get('providerTerritory', 'N/A'),
                'entitlements': account_tiers,
                'features': account_features,
                'lastUpdate': user_info.get('lastEntitlementUpdateTimestamp', 'N/A')
            }
            
        except Exception as e:
            self.log.warning(f"Failed to fetch session info: {e}")
            return None

    @staticmethod
    def calculate_sky_header_md5(headers):
        if len(headers.items()) > 0:
            headers_str = "\n".join(f"{x[0].lower()}: {x[1]}" for x in headers.items()) + "\n"
        else:
            headers_str = "{}"
        return str(hashlib.md5(headers_str.encode()).hexdigest())

    @staticmethod
    def calculate_body_md5(body):
        return str(hashlib.md5(body.encode()).hexdigest())

    def calculate_signature(self, msg):
        digest = hmac.new(self.hmac_key, bytes(msg, "utf-8"), hashlib.sha1).digest()
        return str(base64.b64encode(digest), "utf-8")

    def create_signature_header(self, method, path, sky_headers, body, timestamp):
        data = "\n".join([
            method.upper(),
            path,
            "",
            self.config["client"]["client_sdk"],
            "1.0",
            self.calculate_sky_header_md5(sky_headers),
            str(timestamp),
            self.calculate_body_md5(body)
        ]) + "\n"

        signature_hmac = self.calculate_signature(data)

        return self.config["security"]["signature_format"].format(
            client=self.config["client"]["client_sdk"],
            signature=signature_hmac,
            timestamp=timestamp
        )

    def get_tokens(self):
        tokens_cache_path = self.get_cache("tokens_{profile}_{id}.json".format(
            profile=self.profile,
            id=self.config["client"]["id"]
        ))
    
        if os.path.isfile(tokens_cache_path):
            with open(tokens_cache_path, encoding="utf-8") as fd:
                tokens = json.load(fd)
            tokens_expiration = tokens.get("tokenExpiryTime", None)
            if tokens_expiration:
                expiry_time = datetime.strptime(tokens_expiration, "%Y-%m-%dT%H:%M:%S.%fZ")
                if expiry_time > datetime.now():
                    self.log.info(" + Using cached tokens (valid until {})".format(
                        expiry_time.strftime("%Y-%m-%d %H:%M:%S")
                    ))
                    return tokens
                else:
                    self.log.info(" + Cached tokens expired on {}".format(
                        expiry_time.strftime("%Y-%m-%d %H:%M:%S")
                    ))
            else:
                self.log.info(" + Cached tokens found (no expiry info)")
    
        self.log.info(" + Requesting new tokens")
    
        sky_headers = {
            "X-SkyOTT-Agent": ".".join([
                self.config["client"]["proposition"],
                self.config["client"]["device"],
                self.config["client"]["platform"]
            ]).lower(),
            "X-SkyOTT-Device": self.config["client"]["device"],
            "X-SkyOTT-Platform": self.config["client"]["platform"],
            "X-SkyOTT-Proposition": self.config["client"]["proposition"],
            "X-SkyOTT-Provider": self.config["client"]["provider"],
            "X-SkyOTT-Territory": self.config["client"]["territory"]
        }
    
        body = json.dumps({
            "auth": {
                "authScheme": self.config["client"]["auth_scheme"],
                "authIssuer": self.config["client"]["auth_issuer"],
                "provider": self.config["client"]["provider"],
                "providerTerritory": self.config["client"]["territory"],
                "proposition": self.config["client"]["proposition"],
            },
            "device": {
                "type": self.config["client"]["device"],
                "platform": self.config["client"]["platform"],
                "id": self.config["client"]["id"],
                "drmDeviceId": self.config["client"]["drm_device_id"]
            }
        }, separators=(",", ":"))
    
        timestamp = int(time.time())
    
        try:
            response = self.session.post(
                url=self.config["endpoints"]["tokens"],
                headers=dict(**sky_headers, **{
                    "Accept": "application/vnd.tokens.v1+json",
                    "Content-Type": "application/vnd.tokens.v1+json",
                    "X-Sky-Signature": self.create_signature_header(
                        method="POST",
                        path="/auth/tokens",
                        sky_headers=sky_headers,
                        body=body,
                        timestamp=timestamp
                    )
                }),
                data=body
            )
            
            # Check for common error responses
            if response.status_code == 401:
                self.log.error("Authentication failed - Check your cookies")
                self.log.error("If your cookies are encoded, the script will attempt to decode them automatically")
                raise self.log.exit(" - Authentication failed")
            
            tokens = response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Token request failed: {e}")
            raise self.log.exit(" - Could not obtain tokens")
    
        if "errorCode" in tokens:
            error_msg = f" - Token error: {tokens.get('description', 'Unknown error')} [{tokens['errorCode']}]"
            if tokens['errorCode'] == "OVP_00006":
                error_msg += "\n   This may be due to an encoded cookie. The script attempted to decode it."
                error_msg += "\n   Try exporting cookies in a different format (Netscape format works best)."
            raise self.log.exit(error_msg)
    
        if "userToken" not in tokens:
            raise self.log.exit(" - Invalid tokens response: missing 'userToken'")
    
        os.makedirs(os.path.dirname(tokens_cache_path), exist_ok=True)
        with open(tokens_cache_path, "w", encoding="utf-8") as fd:
            json.dump(tokens, fd)
    
        return tokens

    def verify_tokens(self):
        sky_headers = {
            "X-SkyOTT-Device": self.config["client"]["device"],
            "X-SkyOTT-Platform": self.config["client"]["platform"],
            "X-SkyOTT-Proposition": self.config["client"]["proposition"],
            "X-SkyOTT-Provider": self.config["client"]["provider"],
            "X-SkyOTT-Territory": self.config["client"]["territory"],
            "X-SkyOTT-UserToken": self.tokens["userToken"]
        }
        try:
            response = self.session.get(
                url=self.config["endpoints"]["me"],
                headers=dict(**sky_headers, **{
                    "Accept": "application/vnd.userinfo.v2+json",
                    "Content-Type": "application/vnd.userinfo.v2+json",
                    "X-Sky-Signature": self.create_signature_header(
                        method="GET",
                        path="/auth/users/me",
                        sky_headers=sky_headers,
                        body="",
                        timestamp=int(time.time())
                    )
                })
            )
            response.raise_for_status()
            
            # Quick check to ensure we got a valid response
            data = response.json()
            if "entitlements" in data or "segmentation" in data:
                return True
            else:
                self.log.error("Token verification returned unexpected data structure")
                return False
                
        except requests.HTTPError as e:
            self.log.error(f"Token verification failed with HTTP error: {e}")
            if e.response.status_code == 401:
                self.log.error("   This usually means your cookies are invalid or expired")
                self.log.error("   Try exporting fresh cookies from your browser")
            return False
        except Exception as e:
            self.log.error(f"Token verification failed: {e}")
            return False