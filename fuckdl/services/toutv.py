import base64
import json
import time
import re
import gzip
import uuid
import subprocess
import shutil
import hashlib
import os, sys
import m3u8
import requests
import click
import http.cookiejar as cookielib

from typing import Any, Union
from hashlib import md5
from fuckdl.objects import MenuTrack, Title, Tracks, TextTrack, AudioTrack
from fuckdl.services.BaseService import BaseService
from urllib.parse import urlencode
from urllib.request import build_opener, HTTPCookieProcessor, Request
from io import StringIO as StringIO 

os.system('')
GREEN = '\033[32m'
MAGENTA = '\033[35m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
RED = '\033[31m'
RESET = '\033[0m'


class TouTV(BaseService):
    """
    \b
    Authorization: Credentials
    Security: FHD@L3 / FHD@SL2K, doesn't seem to care about releases.
    
    Example input: 
    MOVIE: poetry run fuckdl dl toutv https://ici.tou.tv/frontieres
    SERIES: poetry run fuckdl dl toutv https://ici.tou.tv/le-monde-de-gabrielle-roy

    \b

    Original Author: TANZ - fixed by @rxeroxhd

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["TOUTV"]
    GEOFENCE = []

    @staticmethod
    @click.command(name="TouTV", short_help="https://ici.tou.tv/")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return TouTV(ctx, **kwargs)

    def __init__(self, ctx, title):
        self.title = title.split("/")[-1]
        super().__init__(ctx)

        self.configure()

    def get_titles(self):

        res = self.session.get(
            url='https://services.radio-canada.ca/ott/catalog/v2/toutv/show/{id}'.format(id=self.title),
            params = {
                'device': 'web',
            }
        )
        data = res.json()
        content_type = data['contentType']

        if content_type == 'Standalone':
            metadata = data["content"][0]['lineups'][0]['items'][0]['metadata']

            return Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=data["title"],
                year=metadata['productionYear'],
                original_lang='fr', #TODO: Don't assume
                source=self.ALIASES[0],
                service_data=data["content"][0]['lineups'][0]['items'][0]
            )
        else:
            titles = []

            season_data = data['content'][0]['lineups']
            
            for season in season_data:
                for episode in season["items"]:
                    eps_number = episode['episodeNumber']
                    if eps_number == 0:
                        continue

                    titles.append(Title(
                        id_=self.title,
                        type_=Title.Types.TV,
                        name=data['title'],
                        season=season['seasonNumber'],
                        episode=eps_number,
                        episode_name=episode['title'],
                        original_lang='fr', #TODO: Don't assume
                        source=self.ALIASES[0],
                        service_data=episode
                    ))

            return titles
   
    def get_tracks(self, title):
        svdata = title.service_data
        
        # Debug: show what tokens we're using
        auth = self.access_token['Authorization']
        claims = self.access_token.get('x-claims-token', '')
        try:
            auth_payload = json.loads(base64.b64decode(auth.replace("Bearer ", "").split(".")[1] + "==").decode())
            self.log.info(f" + Auth token exp: {auth_payload.get('exp')} (now: {int(time.time())})")
        except Exception:
            pass
        try:
            claims_payload = json.loads(base64.b64decode(claims.split(".")[1] + "==").decode())
            self.log.info(f" + Claims tier: {claims_payload.get('Tier', 'N/A')}, exp: {claims_payload.get('exp')}")
        except Exception:
            self.log.warning(f" + Could not decode claims token")

        res = self.session.get(
            url='https://services.radio-canada.ca/media/validation/v2/',
            params = {
                'appCode': 'toutv',
                'connectionType': 'hd',
                'deviceType': 'multiams',
                'idMedia': svdata['idMedia'],
                'multibitrate': 'true',
                'output': 'json',
                'tech': 'dash',
                'manifestVersion': '2',
            },
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Authorization': self.access_token['Authorization'],
                'connection': 'Keep-Alive',
                'user-agent': 'okhttp/4.12.0',
                'x-claims-token': self.access_token['x-claims-token']
            }
        )
        
        try:
            data = res.json()
        except Exception:
            raise ValueError(f"Failed to load manifest url: {res.text}")

        manifest_url = data.get('url')
        if not manifest_url:
            self.log.error(f" - Media validation failed: {json.dumps(data, indent=2)[:500]}")
            raise ValueError(f"No manifest URL returned. Message: {data.get('message', data.get('errorCode', 'unknown'))}")
        
        self.log.debug(data)

        self.license_url = None
        self.license_token = None
        
        try:
            for param in data['params']:
                if param['name'] == 'playreadyLicenseUrl':
                    self.license_url = param['value']
                elif param['name'] == 'playreadyAuthToken':
                    self.license_token = param['value']
        except (KeyError, TypeError):
            pass

        if ".mpd" in manifest_url:
            tracks = Tracks.from_mpd(
                url=manifest_url,
                session=self.session,
                source=self.ALIASES[0]
            )
        else:
            tracks = Tracks.from_m3u8(
                m3u8.loads(self.session.get(manifest_url).text, manifest_url),
                source=self.ALIASES[0]
            )

        for track in tracks:
            if isinstance(track, AudioTrack): # no encryption
                track.encrypted = False

        for track in tracks.audios:
            try:
                role = track.extra[1].find("Role")
                if role is not None and role.get("value") in ["description", "alternative", "alternate", "commentary"]:
                    track.descriptive = True
                else:
                    track.descriptive = False
            except (TypeError, IndexError, AttributeError):
                track.descriptive = False

        for track in tracks:
            track.extra = manifest_url

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None  # will use common privacy cert

    def license(self, challenge, **_):     
        r = self.session.post(
            url=self.license_url,
            data=challenge,
            headers={
                'x-dt-auth-token': self.license_token,
            }
        )
        if not r.content:
            self.log.exit(" - No license returned!")
        else:
            return r.content
   
    def configure(self):
        self.log.info(" + Starting TouTV...")

        self.access_token = self.login()

    
    def login(self):
        tokens_cache_path = self.get_cache("tokens_toutv.json")
        tokens = None
        if os.path.isfile(tokens_cache_path):
            with open(tokens_cache_path, encoding="utf-8") as fd:
                tokens = json.load(fd)

        # Try cached tokens first
        if tokens:
            auth = tokens.get("Authorization")
            claims = tokens.get("x-claims-token")
            refresh = tokens.get("refresh_token")

            if auth and self.verify_premium(auth):
                # Check if manual claims available for Premium upgrade
                manual_claims = self._get_manual_claims()
                if manual_claims:
                    tokens["x-claims-token"] = manual_claims
                    self._save_login_cache(tokens_cache_path, tokens)
                
                self.log.info(" + Using cached tokens")
                self._refresh_token = refresh
                return tokens

            # Try refresh token
            if refresh:
                try:
                    self.log.info(" + Token expired, refreshing...")
                    new_auth = self._do_refresh(refresh)
                    new_claims = self._fetch_claims(new_auth)
                    headers = {"Authorization": new_auth, "x-claims-token": new_claims, "refresh_token": self._refresh_token}
                    self._save_login_cache(tokens_cache_path, headers)
                    self.log.info(" + Tokens refreshed successfully")
                    return headers
                except Exception as e:
                    self.log.warning(f" + Refresh failed ({e}), doing fresh login...")

        # Fresh login via ROPC
        self.log.info(" + Performing fresh login...")
        auth, refresh = self._do_ropc_login(self.credentials.username, self.credentials.password)
        claims = self._fetch_claims(auth)
        self._refresh_token = refresh

        # Check if claims is Premium
        is_premium = False
        try:
            payload = json.loads(base64.b64decode(claims.split(".")[1] + "==").decode())
            is_premium = payload.get("Tier") == "Premium"
        except Exception:
            pass

        if not is_premium:
            # ROPC doesn't give Premium tier â€” check for manual claims token
            manual_claims = self._get_manual_claims()
            if manual_claims:
                claims = manual_claims
                self.log.info(" + Using manually provided Premium claims token")
            else:
                self.log.warning(" + ROPC login gives Member tier, not Premium.")
                self.log.warning(" + For Premium content, provide your x-claims-token in:")
                self.log.warning(f"   {tokens_cache_path.parent / 'claims_token.txt'}")
                self.log.warning(" + Get it from browser DevTools â†’ Network â†’ x-claims-token header")

        headers = {"Authorization": auth, "x-claims-token": claims, "refresh_token": refresh}
        self._save_login_cache(tokens_cache_path, headers)
        return headers

    def _get_manual_claims(self):
        """Check for manually provided claims token file."""
        claims_path = self.get_cache("claims_token.txt")
        if os.path.isfile(claims_path):
            with open(claims_path, "r", encoding="utf-8") as f:
                token = f.read().strip()
            if token and "." in token:
                try:
                    payload = json.loads(base64.b64decode(token.split(".")[1] + "==").decode())
                    exp = payload.get("exp", 0)
                    if exp > time.time():
                        tier = payload.get("Tier", "unknown")
                        self.log.info(f" + Manual claims token: tier={tier}, expires in {int((exp - time.time()) / 3600)}h")
                        return token
                    else:
                        self.log.warning(" + Manual claims token is expired")
                except Exception:
                    pass
        return None

    def _save_login_cache(self, path, headers):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fd:
            json.dump(headers, fd)

    def _get_ropc_settings(self):
        """Get ROPC token URL and scopes from Radio-Canada settings API."""
        r = requests.get(
            "https://services.radio-canada.ca/ott/catalog/v1/toutv/settings",
            params={"device": "web"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        settings = r.json()
        ropc = settings.get("identityManagement", {}).get("ropc", {})
        return ropc.get("url"), ropc.get("scopes", "")

    def _do_ropc_login(self, email, password):
        """OAuth2 ROPC login â€” same method as CBC Gem."""
        token_url, api_scopes = self._get_ropc_settings()

        if not token_url:
            raise ValueError("Could not find ROPC token URL from settings API")

        self.log.info(f" + ROPC endpoint: {token_url}")

        # Use full scopes matching the browser login (API settings scopes may be too limited for Premium)
        full_scopes = (
            "openid offline_access "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/media-drmt "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/media-meta "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/media-validation "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/media-validation.read "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/metrik "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/toutv "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/toutv-presentation "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/toutv-profiling "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/ott-profiling "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/ott-subscription "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/subscriptions.validate "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/subscriptions.write "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/id.account.info "
            "https://rcmnb2cprod.onmicrosoft.com/84593b65-0ef6-4a72-891c-d351ddd50aab/profile"
        )

        # Try full scopes first, fallback to API scopes
        for scopes in [full_scopes, api_scopes]:
            r = requests.post(
                url=token_url,
                data={
                    "client_id": "ebe6e7b0-3cc3-463d-9389-083c7b24399c",
                    "grant_type": "password",
                    "username": email,
                    "password": password,
                    "scope": scopes,
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                },
            )
            data = r.json()

            if "access_token" in data:
                self.log.info(" + ROPC login successful")
                return f"Bearer {data['access_token']}", data.get("refresh_token")

        raise ValueError(f"ROPC login failed: {data.get('error_description', data.get('error', r.text[:200]))}")

    def _do_refresh(self, refresh_token):
        """Refresh the access token."""
        token_url, scopes = self._get_ropc_settings()

        r = requests.post(
            url=token_url,
            data={
                "client_id": "ebe6e7b0-3cc3-463d-9389-083c7b24399c",
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "scope": scopes,
            },
        )
        data = r.json()

        if "access_token" not in data:
            raise ValueError(f"Refresh failed: {data.get('error_description', 'unknown')}")

        self._refresh_token = data.get("refresh_token", refresh_token)
        return f"Bearer {data['access_token']}"

    def _fetch_claims(self, auth_token):
        """Get x-claims-token from subscription API."""
        # Try different device types â€” some return Premium, others don't
        for device in ["android", "androidtv", "web", "ios"]:
            r = requests.get(
                "https://services.radio-canada.ca/ott/subscription/v2/toutv/subscriber/profile",
                params={"device": device},
                headers={"Authorization": auth_token},
            )
            resp = r.json()
            token = resp.get("claimsToken")
            if not token:
                continue
            
            # Decode and check tier
            try:
                payload = json.loads(base64.b64decode(token.split(".")[1] + "==").decode())
                tier = payload.get("Tier", "unknown")
                self.log.info(f" + Claims token tier: {tier} (device={device})")
                if tier == "Premium":
                    return token
            except Exception:
                pass

        # If no Premium tier found, return the last token we got
        if token:
            self.log.warning(f" + Account not recognized as Premium via API. Using best available claims token.")
            return token
        
        raise ValueError(f"Could not get claims token from any device type")

    def verify_premium(self, auth_token: str) -> bool:
        decrypted_auth = base64.b64decode(auth_token.split(".")[1] + "==").decode(encoding="ascii")
        time_auth = json.loads(decrypted_auth)["exp"]
        
        if time_auth < time.time():
            print("You need to refresh your Authorization Token")
            return False
        
        return True

    def get_x_token(self, access_token: str):
        url = "https://services.radio-canada.ca/ott/subscription/v2/toutv/subscriber/profile?device=web"
        r = self.session.get(url, headers={"Authorization": access_token})
        resp = r.json()

        token = resp["claimsToken"]

        return token

    def get_access_token(self, email: str, password: str):
        params = self.GET_AUTHORISE("https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/oauth2/v2.0/authorize?client_id=ebe6e7b0-3cc3-463d-9389-083c7b24399c&redirect_uri=https%3A%2F%2Fici.tou.tv%2Fauth-changed&scope=openid%20offline_access%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Femail%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.create%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.delete%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.info%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.modify%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.reset-password%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.account.send-confirmation-email%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fid.write%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fmedia-drmt%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fmedia-meta%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fmedia-validation%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fmedia-validation.read%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fmetrik%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Foidc4ropc%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fott-profiling%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fott-subscription%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fprofile%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fsubscriptions.validate%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Fsubscriptions.write%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Ftoutv%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Ftoutv-presentation%20https%3A%2F%2Frcmnb2cprod.onmicrosoft.com%2F84593b65-0ef6-4a72-891c-d351ddd50aab%2Ftoutv-profiling&response_type=id_token%20token")

        data = {'email': email, 'request_type': 'RESPONSE'}
        valassert1 = self.GET_SELF_ASSERTED(params, data)

        tokenS1 = self.GET_ACCESS_TOKEN_MS(False, valassert1)

        data = {'email': email, 'request_type': 'RESPONSE', 'password': password}
        valassert2 = self.GET_SELF_ASSERTED(tokenS1[0], data)

        tokenS2 = self.GET_ACCESS_TOKEN_MS(True, valassert2)
        
        tokenS3 = tokenS2[1].split("access_token=")
        access_token = tokenS3[1].split("&token_type")
        accessToken = access_token[0]

        return "Bearer " + accessToken

    def handleHttpResponse(self, response):
        if sys.version_info.major >= 3:
            if response.info().get('Content-Encoding') == 'gzip':
                f = gzip.GzipFile(fileobj=response)
                data = f.read()
                return data
            else:
                data = response.read()
                return data
        else:
            if response.info().get('Content-Encoding') == 'gzip':
                buf = StringIO( response.read() )
                f = gzip.GzipFile(fileobj=buf)
                data = f.read()
                return data
            else:
                return response.read()

    def GET_AUTHORISE(self, url):
        cookiejar = cookielib.LWPCookieJar()
        cookie_handler = HTTPCookieProcessor(cookiejar)
        opener = build_opener(cookie_handler)

        request = Request(url)
        request.get_method = lambda: "GET"
        
        response = opener.open(request)
        text = self.handleHttpResponse(response)

        parts = text.split(bytes("StateProperties=", encoding='utf8'), 1)
        parts = parts[1].split(bytes("\"", encoding='utf8'), 1)
        state = parts[0]

        return cookiejar, state

    def GET_SELF_ASSERTED(self, params, data):
        csrf = None
        
        for c in params[0]:
            if c.name == "x-ms-cpim-csrf":
                csrf = c.value

        url = "https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/SelfAsserted?tx=StateProperties=" + params[1].decode('utf-8') + "&p=B2C_1A_ExternalClient_FrontEnd_Login"

        cookie_handler = HTTPCookieProcessor(params[0])
        opener = build_opener(cookie_handler)

        opener.addheaders = [('X-CSRF-TOKEN', csrf)]
        post_data = urlencode(data)

        request = Request(url, data=bytes(post_data, encoding='utf8'))
        request.get_method = lambda: "POST"
        
        response = opener.open(request)

        rawresp = self.handleHttpResponse(response)

        return params[0], params[1]

    def GET_ACCESS_TOKEN_MS(self, modeLogin, params ):
        csrf = None
        
        for c in params[0]:
            if c.name == "x-ms-cpim-csrf":
                csrf = c.value

        url = None
        if modeLogin == True:
            url = "https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/api/CombinedSigninAndSignup/confirmed?rememberMe=true&csrf_token=" + csrf + "&tx=StateProperties=" + params[1].decode('utf-8') + "&p=B2C_1A_ExternalClient_FrontEnd_Login&diags=%7B%22pageViewId%22%3A%2226127485-f667-422c-b23f-6ebc0c422705%22%2C%22pageId%22%3A%22CombinedSigninAndSignup%22%2C%22trace%22%3A%5B%7B%22ac%22%3A%22T005%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A4%7D%2C%7B%22ac%22%3A%22T021%20-%20URL%3Ahttps%3A%2F%2Fmicro-sites.radio-canada.ca%2Fb2cpagelayouts%2Flogin%2Fpassword%3Fui_locales%3Dfr%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A36%7D%2C%7B%22ac%22%3A%22T019%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A5%7D%2C%7B%22ac%22%3A%22T004%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A2%7D%2C%7B%22ac%22%3A%22T003%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A4%7D%2C%7B%22ac%22%3A%22T035%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T030Online%22%2C%22acST%22%3A1632790122%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T002%22%2C%22acST%22%3A1632790129%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T018T010%22%2C%22acST%22%3A1632790128%2C%22acD%22%3A695%7D%5D%7D"
            url = "https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/api/CombinedSigninAndSignup/confirmed?rememberMe=true&csrf_token=" + csrf + "&tx=StateProperties=" + params[1].decode('utf-8') + "&p=B2C_1A_ExternalClient_FrontEnd_Login&diags=%7B%22pageViewId%22%3A%22fef7143d-a216-4fa8-a066-cbfa7c315a93%22%2C%22pageId%22%3A%22CombinedSigninAndSignup%22%2C%22trace%22%3A%5B%7B%22ac%22%3A%22T005%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T021%20-%20URL%3Ahttps%3A%2F%2Fmicro-sites.radio-canada.ca%2Fb2cpagelayouts%2Flogin%2Fpassword%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A40%7D%2C%7B%22ac%22%3A%22T019%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A2%7D%2C%7B%22ac%22%3A%22T004%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T003%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A1%7D%2C%7B%22ac%22%3A%22T035%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T030Online%22%2C%22acST%22%3A1730670125%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T002%22%2C%22acST%22%3A1730670148%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T018T010%22%2C%22acST%22%3A1730670147%2C%22acD%22%3A348%7D%5D%7D"
        else:
            url = "https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/api/SelfAsserted/confirmed?csrf_token=" + csrf + "&tx=StateProperties=" + params[1].decode('utf-8') + "&p=B2C_1A_ExternalClient_FrontEnd_Login&diags=%7B%22pageViewId%22%3A%2222e91666-af5b-4d27-b6f0-e9999cb0b66c%22%2C%22pageId%22%3A%22SelfAsserted%22%2C%22trace%22%3A%5B%7B%22ac%22%3A%22T005%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A3%7D%2C%7B%22ac%22%3A%22T021%20-%20URL%3Ahttps%3A%2F%2Fmicro-sites.radio-canada.ca%2Fb2cpagelayouts%2Flogin%2Femail%3Fui_locales%3Dfr%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A154%7D%2C%7B%22ac%22%3A%22T019%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A3%7D%2C%7B%22ac%22%3A%22T004%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A5%7D%2C%7B%22ac%22%3A%22T003%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A1%7D%2C%7B%22ac%22%3A%22T035%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T030Online%22%2C%22acST%22%3A1633303735%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T017T010%22%2C%22acST%22%3A1633303742%2C%22acD%22%3A699%7D%2C%7B%22ac%22%3A%22T002%22%2C%22acST%22%3A1633303742%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T017T010%22%2C%22acST%22%3A1633303742%2C%22acD%22%3A700%7D%5D%7D"
            url = "https://rcmnb2cprod.b2clogin.com/rcmnb2cprod.onmicrosoft.com/B2C_1A_ExternalClient_FrontEnd_Login/api/SelfAsserted/confirmed/?csrf_token=" + csrf + "&tx=StateProperties=" + params[1].decode('utf-8') + '&p=B2C_1A_ExternalClient_FrontEnd_Login&diags=%7B%22pageViewId%22%3A%22ced09dac-0687-48c9-87de-f5a60d4ae43f%22%2C%22pageId%22%3A%22SelfAsserted%22%2C%22trace%22%3A%5B%7B%22ac%22%3A%22T005%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A1%7D%2C%7B%22ac%22%3A%22T021%20-%20URL%3Ahttps%3A%2F%2Fmicro-sites.radio-canada.ca%2Fb2cpagelayouts%2Flogin%2Femail%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A64%7D%2C%7B%22ac%22%3A%22T019%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A2%7D%2C%7B%22ac%22%3A%22T004%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A3%7D%2C%7B%22ac%22%3A%22T003%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A1%7D%2C%7B%22ac%22%3A%22T035%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T030Online%22%2C%22acST%22%3A1730670689%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T017T010%22%2C%22acST%22%3A1730671535%2C%22acD%22%3A447%7D%2C%7B%22ac%22%3A%22T002%22%2C%22acST%22%3A1730671536%2C%22acD%22%3A0%7D%2C%7B%22ac%22%3A%22T017T010%22%2C%22acST%22%3A1730671535%2C%22acD%22%3A448%7D%5D%7D'

        cookie_handler = HTTPCookieProcessor(params[0])
        opener = build_opener(cookie_handler)

        request = Request(url)
        request.get_method = lambda: "GET"
        
        response = opener.open(request)
        text = self.handleHttpResponse(response)

        return params, response.geturl()
