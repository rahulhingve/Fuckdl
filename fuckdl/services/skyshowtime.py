from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import time
import requests
import os
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote
from http.cookiejar import CookieJar

import click
from click import Context
from fuckdl.utils.widevine.device import LocalDevice
from fuckdl.objects import MenuTrack, Title, Tracks
from fuckdl.services.BaseService import BaseService

class SkySignature:
    """SkyShowtime API signature generator (from Unshackle)."""
    
    def __init__(self, app_id: str, signature_key: str, version: str = "1.0"):
        self.app_id = app_id
        self.signature_key = signature_key.encode('utf-8')
        self.sig_version = version
    
    def calculate_signature(self, method: str, url: str, headers: dict, 
                          payload: bytes = b'', timestamp: int = None) -> dict:
        if timestamp is None:
            timestamp = int(time.time())
        
        if url.startswith('http'):
            from urllib.parse import urlparse
            parsed_url = urlparse(url)
            path = parsed_url.path
            if parsed_url.query:
                path += '?' + parsed_url.query
        else:
            path = url
        
        # Filter only SkyOTT headers
        text_headers = ''
        for key in sorted(headers.keys()):
            if key.lower().startswith('x-skyott'):
                text_headers += key.lower() + ': ' + str(headers[key]) + '\n'
        
        if not text_headers:
            text_headers = '{}'
        
        headers_md5 = hashlib.md5(text_headers.encode()).hexdigest()
        
        if isinstance(payload, str):
            payload = payload.encode('utf-8')
        payload_md5 = hashlib.md5(payload).hexdigest()
        
        to_hash = (
            f'{method}\n'
            f'{path}\n'
            f'\n'
            f'{self.app_id}\n'
            f'{self.sig_version}\n'
            f'{headers_md5}\n'
            f'{timestamp}\n'
            f'{payload_md5}\n'
        )
        
        hashed = hmac.new(self.signature_key, to_hash.encode('utf8'), hashlib.sha1).digest()
        signature = base64.b64encode(hashed).decode('utf8')
        
        return {
            'x-sky-signature': f'SkyOTT client="{self.app_id}",signature="{signature}",timestamp="{timestamp}",version="{self.sig_version}"'
        }


class Skyshowtime(BaseService):
    """
    Service code for SkyShowtime streaming service (https://skyshowtime.com).
    Updated with Unshackle improvements.
    """

    ALIASES = ["SKST", "Skyshowtime"]
    
    TITLE_RE = [
        r"(?:https?://(?:www\.)?skyshowtime\.com/watch/asset)?(?P<id>/movies/[a-z0-9-]+/[a-f0-9-]+)",
        r"(?:https?://(?:www\.)?skyshowtime\.com/watch/asset)?(?P<id>/tv/[a-z0-9-]+/[a-f0-9-]+)",
    ]

    VIDEO_RANGE_MAP = {
        "DV": "DOLBY_VISION"
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "AC3": "ac-3",
        "EC3": "ec-3"
    }
    
    @staticmethod
    @click.command(name="Skyshowtime", short_help="https://skyshowtime.com")
    @click.argument("title", type=str)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a Movie.")
    @click.option("-t", "--territory", type=str, default=None, help="Territory code (e.g., PL, NL, ES)")
    @click.option("-p", "--profile", type=str, default=None, help="Profile name or ID to use")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Skyshowtime(ctx, **kwargs)

    def __init__(self, ctx, title: str, movie: bool = False, territory: str = None, profile: str = None):
        self.parse_title(ctx, title)
        self.movie = movie
        super().__init__(ctx)

        self.profile = ctx.obj.profile
        self.cdm = ctx.obj.cdm
        self.range = ctx.parent.params["range_"]
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]

        self.territory = territory or self._get_territory_from_cookies()
        self.requested_profile = profile
        self.user_token = None
        self.device_id = None
        self._token_expiry = None
        self.persona_id = None
        self.persona_data = None
        self.all_personas = []
        
        # Start signature generator
        self.signer = SkySignature(
            app_id=self.config["client"]["client_sdk"],
            signature_key=self.config["security"]["signature_hmac_key_v4"],
            version="1.0"
        )

        if (ctx.parent.params.get("quality") or 0) > 1080 and self.vcodec != "H265":
            self.log.info(" + Switched video codec to H265 to be able to get 2160p video track")
            self.vcodec = "H265"

        if self.range in ("HDR10", "DV") and self.vcodec != "H265":
            self.log.info(f" + Switched video codec to H265 to be able to get {self.range} dynamic range")
            self.vcodec = "H265"

        self.service_config = None
        self.hmac_key: bytes
        self.tokens: dict
        self.license_api = None
        self.license_bt = None

        self.configure()
    
    def _get_territory_from_cookies(self) -> str:
        """Get territory from cookies or return default."""
        if self.cookies:
            for cookie in self.cookies:
                if cookie.name == "activeTerritory":
                    return cookie.value.upper()
        return self.config["client"].get("territory", "PL")
    
    def _get_language_for_territory(self, territory: str) -> str:
        """Get language code for territory."""
        territory_languages = {
            "PL": "pl-PL", "NL": "nl-NL", "ES": "es-ES", "PT": "pt-PT",
            "SE": "sv-SE", "NO": "nb-NO", "DK": "da-DK", "FI": "fi-FI",
            "CZ": "cs-CZ", "SK": "sk-SK", "HU": "hu-HU", "RO": "ro-RO",
            "BG": "bg-BG", "HR": "hr-HR", "SI": "sl-SI", "BA": "bs-BA",
            "RS": "sr-RS", "ME": "sr-ME", "MK": "mk-MK", "AL": "sq-AL",
        }
        return territory_languages.get(territory.upper(), "en-US")

    def get_titles(self):
        """Get titles with improved parsing and error handling."""
        res = self.session.get(
            url=self.config["endpoints"]["node"],
            params={
                "slug": self.title,
                "represent": "(items(items),recs[take=8],collections(items(items[take=8])),trailers)"
            },
            headers={
                "Accept": "*",
                "Referer": f"https://www.skyshowtime.com/watch/asset{self.title}",
                "x-skyott-Activeterritory": self.session.cookies.get("activeTerritory") or self.territory,
                "x-skyott-device": self.config["client"]["device"],
                "x-skyott-language": self._get_language_for_territory(self.territory),
                "x-skyott-platform": self.config["client"]["platform"],
                "x-skyott-proposition": self.config["client"]["proposition"],
                "x-skyott-provider": self.config["client"]["provider"],
                "x-skyott-territory": self.session.cookies.get("activeTerritory") or self.territory
            }
        )
        if not res.ok:
            self.log.exit(f" - HTTP Error {res.status_code}: {res.reason}")
            raise
        
        data = res.json()
        
        # Debug: Log the actual response structure
        self.log.debug(f"Response keys: {data.keys()}")
        
        # Get attributes
        attrs = data.get("attributes", {})
        content_type = data.get("type", "")
        
        # Check if it's a series (has episodes/items) or movie
        relationships = data.get("relationships", {})
        
        # Different possible paths for items/seasons
        items_data = None
        
        # Try multiple possible paths for seasons/items
        if "items" in relationships:
            items_data = relationships["items"].get("data", [])
        elif "seasons" in relationships:
            items_data = relationships["seasons"].get("data", [])
        elif "episodes" in relationships:
            items_data = relationships["episodes"].get("data", [])
        
        # Also check for direct items in the root
        if not items_data and "items" in data:
            items_data = data.get("items", [])
        
        # For series with seasons
        if items_data and len(items_data) > 0:
            titles = []
            series_title = attrs.get("title", attrs.get("titleMedium", "Unknown Series"))
            
            for season_data in items_data:
                if season_data.get("type") in ["CATALOGUE/SEASON", "SEASON"]:
                    season_attrs = season_data.get("attributes", {})
                    season_number = season_attrs.get("seasonNumber", 1)
                    
                    # Get episodes for this season
                    season_relationships = season_data.get("relationships", {})
                    episodes_data = None
                    
                    if "items" in season_relationships:
                        episodes_data = season_relationships["items"].get("data", [])
                    elif "episodes" in season_relationships:
                        episodes_data = season_relationships["episodes"].get("data", [])
                    
                    if episodes_data:
                        for episode_data in episodes_data:
                            if episode_data.get("type") in ["ASSET/EPISODE", "EPISODE"]:
                                ep_attrs = episode_data.get("attributes", {})
                                titles.append(Title(
                                    id_=self.title,
                                    type_=Title.Types.TV,
                                    name=series_title,
                                    year=ep_attrs.get("year"),
                                    season=season_number,
                                    episode=ep_attrs.get("episodeNumber", 1),
                                    episode_name=ep_attrs.get("title", ep_attrs.get("episodeName", f"Episode {ep_attrs.get('episodeNumber', 1)}")),
                                    original_lang="en",
                                    source=self.ALIASES[0],
                                    service_data=episode_data
                                ))
                    else:
                        # Season without episodes (maybe empty or error)
                        self.log.debug(f"Season {season_number} has no episodes")
                        
            if titles:
                return titles
        
        # Check if it's a movie or single asset
        if "MOVIE" in content_type or "FILM" in content_type or not items_data:
            return [Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=attrs.get("title", attrs.get("titleMedium", "Unknown Title")),
                year=attrs.get("year"),
                original_lang="en",
                source=self.ALIASES[0],
                service_data=data
            )]
        
        # Fallback: try to parse as single episode
        if "EPISODE" in content_type:
            series_attrs = attrs.get("series", {})
            return [Title(
                id_=self.title,
                type_=Title.Types.TV,
                name=series_attrs.get("title", attrs.get("seriesTitle", "Unknown Series")),
                year=attrs.get("year"),
                season=attrs.get("seasonNumber", 1),
                episode=attrs.get("episodeNumber", 1),
                episode_name=attrs.get("title", attrs.get("episodeName", f"Episode {attrs.get('episodeNumber', 1)}")),
                original_lang="en",
                source=self.ALIASES[0],
                service_data=data
            )]
        
        # If we got here, something is wrong
        self.log.warning(f"Unexpected response structure: {json.dumps(data, indent=2)[:500]}")
        raise ValueError("Could not parse titles from response")

    def get_tracks(self, title: Title) -> Tracks:
        """Get tracks with refreshed tokens and profile support."""
        # Refresh token if needed
        self._refresh_user_token_if_needed()
        
        supported_colour_spaces = ["SDR"]
    
        if self.range == "HDR10":
            self.log.info("Switched dynamic range to HDR10")
            supported_colour_spaces = ["HDR10"]
        if self.range == "DV":
            self.log.info("Switched dynamic range to DV")
            supported_colour_spaces = ["DolbyVision"]
            
        content_id = title.service_data["attributes"]["formats"]["HD"]["contentId"]
        variant_id = title.service_data["attributes"]["providerVariantId"]

        _is_playready = False
        if hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__:
            _is_playready = True
        elif hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type'):
            _is_playready = (self.cdm.device.type == LocalDevice.Types.PLAYREADY)
    
        sky_headers = {
            "x-skyott-Activeterritory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-agent": ".".join([
                self.config["client"]["proposition"].lower(),
                self.config["client"]["device"].lower(),
                self.config["client"]["platform"].lower()
            ]),
            "x-skyott-device": self.config["client"]["device"],
            "x-skyott-language": self._get_language_for_territory(self.territory),
            "x-skyott-platform": self.config["client"]["platform"],
            "x-skyott-proposition": self.config["client"]["proposition"],
            "x-skyott-provider": self.config["client"]["provider"],
            "x-skyott-territory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-usertoken": self.user_token or self.tokens.get("userToken")
        }
        
        # Get persona maturity rating if profile selected
        persona_maturity = "9"
        if self.persona_data:
            controls = self.persona_data.get("controls", {})
            persona_maturity = controls.get("maturityRating", "9")
    
        body = json.dumps({
            "contentId": content_id,
            "providerVariantId": variant_id,
            "device": {
                "capabilities": [
                    {
                        "transport": "DASH",
                        "protection": "NONE",
                        "vcodec": "H265",
                        "acodec": "AAC",
                        "container": "ISOBMFF"
                    },
                    {
                        "transport": "DASH",
                        "protection": "PLAYREADY" if _is_playready else "WIDEVINE",
                        "vcodec": "H265",
                        "acodec": "AAC",
                        "container": "ISOBMFF"
                    },
                    {
                        "transport": "DASH",
                        "protection": "NONE",
                        "vcodec": "H264",
                        "acodec": "AAC",
                        "container": "ISOBMFF"
                    },
                    {
                        "transport": "DASH",
                        "protection": "PLAYREADY" if _is_playready else "WIDEVINE",
                        "vcodec": "H264",
                        "acodec": "AAC",
                        "container": "ISOBMFF"
                    },
                ],
                "maxVideoFormat": "UHD" if self.vcodec == "H265" else "HD",
                "supportedColourSpaces": supported_colour_spaces,
                "hdcpEnabled": "true",
            },
            "client": {
                "thirdParties": [
                    "COMSCORE",
                    "CONVIVA",
                    "FREEWHEEL"
                ]
            },
            "personaParentalControlRating": persona_maturity
        }, separators=(",", ":"))
        
        # Calculate signature using new method
        sig_result = self.signer.calculate_signature(
            method="POST",
            url=self.config["endpoints"]["vod"],
            headers=sky_headers,
            payload=body.encode('utf-8')
        )
        
        manifest = self.session.post(
            url=self.config["endpoints"]["vod"],
            data=body,
            headers=dict(**sky_headers, **{
                "accept": "application/vnd.playvod.v1+json",
                "content-type": "application/vnd.playvod.v1+json",
                **sig_result
            })
        ).json()
    
        if "errorCode" in manifest:
            self.log.exit(f" - An error occurred: {manifest['description']} [{manifest['errorCode']}]")
            raise
    
        self.license_api = manifest["protection"]["licenceAcquisitionUrl"]
        self.license_bt = manifest["protection"]["licenceToken"]
    
        tracks = Tracks.from_mpd(
            url=manifest["asset"]["endpoints"][0]["url"] + '&audio=all&subtitle=all',
            session=self.session,
            source=self.ALIASES[0]
        )
        
        if supported_colour_spaces == ["HDR10"]:
            for track in tracks.videos:
                track.hdr10 = True
        if supported_colour_spaces == ["DolbyVision"]:
            for track in tracks.videos:
                track.dv = True
    
        for track in tracks:
            track.needs_proxy = True
    
        if self.acodec:
            tracks.audios = [
                x for x in tracks.audios
                if x.codec and x.codec[:4] == self.AUDIO_CODEC_MAP.get(self.acodec, "")
            ]
    
        return tracks

    def get_chapters(self, title: Title) -> list[MenuTrack]:
        return []

    def certificate(self, challenge, **_):
        return self.license(challenge)
    
    def license(self, challenge: bytes, **_) -> bytes:
        is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                       (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                        self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        
        assert self.license_api is not None
        
        # Calculate signature for license request
        sig_result = self.signer.calculate_signature(
            method="POST",
            url=self.license_api,
            headers={},
            payload=b''
        )
        
        headers = {
            "Accept": "*",
            **sig_result
        }
        
        return self.session.post(
            url=self.license_api,
            headers=headers,
            data=challenge
        ).content

    # Service specific functions
    def configure(self) -> None:
        """Configure service and get tokens."""
        self.session.headers.update({"Origin": "https://www.skyshowtime.com"})
        self.log.info("Getting Skyshowtime Client configuration")
        
        self.hmac_key = bytes(self.config["security"]["signature_hmac_key_v4"], "utf-8")
        self.log.info("Getting Authorization Tokens")
        self.tokens = self.get_tokens()
        self.user_token = self.tokens.get("userToken")
        
        # Fetch personas (profiles) if cookies available
        if self.cookies:
            self._fetch_personas()
            self._select_profile()
        
        self.log.info("Verifying Authorization Tokens")
    
    def _fetch_personas(self) -> None:
        """Fetch available personas/profiles."""
        persona_url = self.config.get("endpoints", {}).get("personas")
        if not persona_url:
            self.log.debug("Personas endpoint not configured")
            return
        
        headers = {
            "Accept": "application/json",
            "content-type": "application/json",
            "x-skyott-Activeterritory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-device": self.config["client"]["device"],
            "x-skyott-language": self._get_language_for_territory(self.territory),
            "x-skyott-platform": self.config["client"]["platform"],
            "x-skyott-proposition": self.config["client"]["proposition"],
            "x-skyott-provider": self.config["client"]["provider"],
            "x-skyott-territory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-usertoken": self.user_token
        }
        
        params = {
            "personaType": "Adult",
            "in_setup": "false"
        }
        
        try:
            r = self.session.post(persona_url, headers=headers, params=params)
            if r.status_code == 200:
                persona_data = r.json()
                self.all_personas = persona_data.get("personas", [])
                if self.all_personas:
                    self.log.info(f" + Found {len(self.all_personas)} profile(s)")
            else:
                self.log.debug(f"Personas request returned {r.status_code}")
        except Exception as e:
            self.log.debug(f"Could not fetch personas: {e}")
            self.all_personas = []
    
    def _select_profile(self) -> None:
        """Select profile by name or ID."""
        if not hasattr(self, 'all_personas') or not self.all_personas:
            return
        
        selected_persona = None
        
        if self.requested_profile:
            # Try match by name
            for persona in self.all_personas:
                if persona.get("displayName", "").lower() == self.requested_profile.lower():
                    selected_persona = persona
                    break
            
            # Try match by ID
            if not selected_persona:
                for persona in self.all_personas:
                    if persona.get("id") == self.requested_profile:
                        selected_persona = persona
                        break
            
            # Try by index
            if not selected_persona:
                try:
                    idx = int(self.requested_profile) - 1
                    if 0 <= idx < len(self.all_personas):
                        selected_persona = self.all_personas[idx]
                except ValueError:
                    pass
            
            if not selected_persona:
                self.log.warning(f"Profile '{self.requested_profile}' not found, using default")
                selected_persona = self.all_personas[0]
        else:
            selected_persona = self.all_personas[0]
        
        self.persona_id = selected_persona.get("id")
        self.persona_data = selected_persona
        
        display_name = selected_persona.get("displayName", "Unknown")
        self.log.info(f" + Using profile: {display_name}")
    
    def _refresh_user_token_if_needed(self) -> None:
        """Refresh user token if expired."""
        # If no token expiry set, refresh
        if not self._token_expiry:
            try:
                self.log.debug("No token expiry, refreshing...")
                self.tokens = self.get_tokens()
                self.user_token = self.tokens.get("userToken")
            except Exception as e:
                self.log.debug(f"Token refresh failed: {e}")
            return
        
        # Make sure expiry is naive for comparison
        if self._token_expiry.tzinfo is not None:
            expiry = self._token_expiry.replace(tzinfo=None)
        else:
            expiry = self._token_expiry
        
        # Check if expired (give 5 minute buffer)
        if expiry <= datetime.utcnow():
            try:
                self.log.debug("Token expired, refreshing...")
                self.tokens = self.get_tokens()
                self.user_token = self.tokens.get("userToken")
                self.log.debug("Token refreshed successfully")
            except Exception as e:
                self.log.debug(f"Token refresh failed: {e}")

    @staticmethod
    def calculate_sky_header_md5(headers: dict) -> str:
        if len(headers.items()) > 0:
            headers_str = "\n".join(list(map(lambda x: f"{x[0].lower()}: {x[1]}", headers.items()))) + "\n"
        else:
            headers_str = "{}"
        return str(hashlib.md5(headers_str.encode()).hexdigest())

    @staticmethod
    def calculate_body_md5(body: str) -> str:
        return str(hashlib.md5(body.encode()).hexdigest())

    def calculate_signature(self, msg: str) -> str:
        digest = hmac.new(self.hmac_key, bytes(msg, "utf-8"), hashlib.sha1).digest()
        return str(base64.b64encode(digest), "utf-8")

    def create_signature_header(self, method: str, path: str, sky_headers: dict, body: str, timestamp: int) -> str:
        data = "\n".join([
            method.upper(),
            path,
            "",  # important!
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
        """Get tokens with improved error handling."""
        tokens_cache_path = self.get_cache("tokens_{profile}_{id}.json".format(
            profile=self.profile,
            id=self.config["client"]["id"]
        ))
        if os.path.isfile(tokens_cache_path):
            with open(tokens_cache_path, encoding="utf-8") as fd:
                tokens = json.load(fd)
            tokens_expiration = tokens.get("tokenExpiryTime", None)
            if tokens_expiration:
                try:
                    # Parse expiry
                    expiry = datetime.strptime(tokens_expiration, "%Y-%m-%dT%H:%M:%S.%fZ")
                    # Make it naive (no timezone) for easier comparison
                    if expiry.tzinfo is not None:
                        expiry = expiry.replace(tzinfo=None)
                    
                    # Check if still valid (give 5 minute buffer)
                    if expiry > datetime.utcnow():
                        # Store as naive for consistency
                        self._token_expiry = expiry
                        return tokens
                except ValueError:
                    pass
    
        # Get all SkyOTT headers
        sky_headers = {
            "x-skyott-Activeterritory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-device": self.config["client"]["device"],
            "x-skyott-language": self._get_language_for_territory(self.territory),
            "x-skyott-platform": self.config["client"]["platform"],
            "x-skyott-proposition": self.config["client"]["proposition"],
            "x-skyott-provider": self.config["client"]["provider"],
            "x-skyott-territory": self.session.cookies.get("activeTerritory") or self.territory
        }
    
        # Craft the body data
        body = json.dumps({
            "auth": {
                "authScheme": self.config["client"]["auth_scheme"],
                "authIssuer": self.config["client"]["auth_issuer"],
                "provider": self.config["client"]["provider"],
                "providerTerritory": self.session.cookies.get("activeTerritory") or self.territory,
                "proposition": self.config["client"]["proposition"],
            },
            "device": {
                "type": self.config["client"]["device"],
                "platform": self.config["client"]["platform"],
                "id": self.config["client"]["id"],
                "drmDeviceId": self.config["client"]["drm_device_id"]
            }
        }, separators=(",", ":"))
    
        # Calculate signature using new method
        sig_result = self.signer.calculate_signature(
            method="POST",
            url=self.config["endpoints"]["tokens"],
            headers=sky_headers,
            payload=body.encode('utf-8')
        )
    
        # Get the tokens
        tokens = self.session.post(
            url=self.config["endpoints"]["tokens"],
            headers=dict(**sky_headers, **{
                "Accept": "application/vnd.tokens.v1+json",
                "Content-Type": "application/vnd.tokens.v1+json",
                **sig_result
            }),
            data=body
        ).json()
    
        os.makedirs(os.path.dirname(tokens_cache_path), exist_ok=True)
        with open(tokens_cache_path, "w", encoding="utf-8") as fd:
            json.dump(tokens, fd)
        
        # Parse expiry as naive datetime
        expiry_str = tokens.get("tokenExpiryTime")
        if expiry_str:
            try:
                self._token_expiry = datetime.strptime(expiry_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                # Remove timezone info if present
                if self._token_expiry.tzinfo is not None:
                    self._token_expiry = self._token_expiry.replace(tzinfo=None)
            except ValueError:
                self._token_expiry = datetime.utcnow() + timedelta(minutes=10)
        else:
            self._token_expiry = datetime.utcnow() + timedelta(minutes=10)
    
        return tokens

    def verify_tokens(self) -> bool:
        """Verify the tokens."""
        sky_headers = {
            "x-skyott-Activeterritory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-device": self.config["client"]["device"],
            "x-skyott-platform": self.config["client"]["platform"],
            "x-skyott-proposition": self.config["client"]["proposition"],
            "x-skyott-provider": self.config["client"]["provider"],
            "x-skyott-territory": self.session.cookies.get("activeTerritory") or self.territory,
            "x-skyott-usertoken": self.tokens["userToken"]
        }
        
        sig_result = self.signer.calculate_signature(
            method="GET",
            url=self.config["endpoints"]["me"],
            headers=sky_headers,
            payload=b''
        )
        
        me = self.session.get(
            url=self.config["endpoints"]["me"],
            headers=dict(**sky_headers, **{
                "accept": "application/vnd.userinfo.v2+json",
                "content-type": "application/vnd.userinfo.v2+json",
                **sig_result
            })
        )

        return me.status_code == 200