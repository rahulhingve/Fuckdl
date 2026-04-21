import json
import os
import time
import hmac

from hashlib import sha1
from datetime import timedelta

import click
import re

from fuckdl.objects import Title, Tracks, MenuTrack
from fuckdl.services.BaseService import BaseService

class Viki(BaseService):
    """
    Service code for the Viki streaming service (https://viki.com).

    \b
    Authorization: Cookies
    Security: UHD@L3

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """ 

    ALIASES = ["VIKI", "RakutenViki"]
    TITLE_RE = [r"^(?:https?://(?:www\.)?viki\.com/(?P<type>movies|tv)/)(?P<id>[\w]+)", r"^(?P<id>[\w]+)"]

    @staticmethod
    @click.command(name="Viki", short_help="https://viki.com")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Viki(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        m = self.parse_title(ctx, title)
        self.movie = movie or m.get("type") == "movies"
        super().__init__(ctx)

        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"] or "AAC"
        self.range = ctx.parent.params["range_"]
        self.quality = ctx.parent.params["quality"]

        self.access_token = None
        self.authorization = None
        self.device_id = "157428845d"
        self.app_id = "100531a"
        self.app_secret_key = "a4e52e9b08620b7131d1830c71cde6cf03c4a7b00d664d9dec8ee27a19d13ba0"

        self.configure()

    def get_titles(self):

        if self.movie:
            movie_data = None
            timestamp_movie = str(round(time.time()))
            movie_api = f"/v4/films/{self.title}.json?film_id={self.title}&app=100531a&token={self.authorization}"+f"&t={timestamp_movie}"
            if movie_data:
                movie_data = json.dumps(movie_data, separators=(",", ":"))
                movie_api += movie_data
            movie_signature = hmac.new(self.app_secret_key.encode("utf-8"), movie_api.encode("utf-8"), sha1).hexdigest()
            
            r = self.session.get(
                url=self.config["endpoints"]["movie_title"].format(contentID=self.title, token=self.authorization),
                headers = {
                    "accept": "*/*",
                    "user-agent": "okhttp/4.10.0",
                    "x-viki-app-ver": "23.5.0",
                    "host": "api.viki.io",
                    "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                    "signature": movie_signature,
                    "timestamp": timestamp_movie,
                    "x-viki-carrier": "",
                    "x-viki-connection-type": "ETHERNET",
                    "x-viki-device-id": self.device_id,
                    "x-viki-device-model": "AOSP TV on x86",
                    "x-viki-device-os-ver": "13",
                    "x-viki-manufacturer": "Google",
                }
            )
            try:
                res = r.json()
            except json.JSONDecodeError:
                raise ValueError(f"Failed to load title manifest: {r.text}")
            
            self.resp = res
            
            if res.get("error_msg") is not None:
                self.log.exit(f'{res["error_msg"]}')
                
            titles = Title(
                id_=self.resp["watch_now"]["id"],
                type_=Title.Types.MOVIE,
                name=self.resp["titles"]["en"],
                year=self.resp["distributors"][0]["from"].split("-")[0],
                original_lang=self.resp["origin"]["language"],
                source=self.ALIASES[0],
                service_data=self.resp
            )

        else:
            series_data = None
            timestamp_series = str(round(time.time()))
            series_api = f"https://api.viki.io/v4/series/{self.title}/episodes.json?&page=1&per_page=24&with_paging=true&app=100531a&token={self.authorization}"+f"&t={timestamp_series}"
            if series_data:
                series_data = json.dumps(series_data, separators=(",", ":"))
                series_api += series_data
            series_signature = hmac.new(self.app_secret_key.encode("utf-8"), series_api.encode("utf-8"), sha1).hexdigest()
            
            r = self.session.get(
                url=self.config["endpoints"]["series_title"].format(contentID=self.title, page=1, token=self.authorization),
                headers = {
                    "accept": "*/*",
                    "user-agent": "okhttp/4.10.0",
                    "x-viki-app-ver": "23.5.0",
                    "host": "api.viki.io",
                    "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                    "signature": series_signature,
                    "timestamp": timestamp_series,
                    "x-viki-carrier": "",
                    "x-viki-connection-type": "ETHERNET",
                    "x-viki-device-id": self.device_id,
                    "x-viki-device-model": "AOSP TV on x86",
                    "x-viki-device-os-ver": "13",
                    "x-viki-manufacturer": "Google",
                }
            )
            res = r.json()

            titles = [Title(
                id_= x["id"],
                type_=Title.Types.TV,
                name=x["container"]["titles"]["en"],
                #year=x.get("releaseYear"),
                season=1,
                episode=x["number"],
                original_lang=x["origin"]["language"],
                source=self.ALIASES[0],
                service_data=x
            ) for x in res["response"]]

        return titles
    
    def get_tracks(self, title):
        manifest_data = None
        timestamp_manifest = str(round(time.time()))
        manifest_api = f"/v5/playback_streams/{title.id}.json?drms=dt3&device_id={self.device_id}&app=100531a&token={self.authorization}"+f"&t={timestamp_manifest}"
        if manifest_data:
            manifest_data = json.dumps(manifest_data, separators=(",", ":"))
            manifest_api += manifest_data
        manifest_signature = hmac.new(self.app_secret_key.encode("utf-8"), manifest_api.encode("utf-8"), sha1).hexdigest()
        r = self.session.get(
            url=self.config["endpoints"]["manifest"].format(contentID=title.id, deviceID=self.device_id, token=self.authorization),
            headers = {
                "accept": "*/*",
                "user-agent": "okhttp/4.10.0",
                "x-viki-app-ver": "23.5.0",
                "host": "api.viki.io",
                "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                "signature": manifest_signature,
                "timestamp": timestamp_manifest,
                "x-viki-carrier": "",
                "x-viki-connection-type": "ETHERNET",
                "x-viki-device-id": self.device_id,
                "x-viki-device-model": "AOSP TV on x86",
                "x-viki-device-os-ver": "13",
                "x-viki-manufacturer": "Google",
            }
        )

        res = r.json()
        
        mpd_url = res["main"][0]["url"]

        mpd_data = self.fix_mpd(mpd_url, title)
        self.title_id = title.id
        self.stream_id = res["main"][0]["properties"]["track"]["stream_id"]

        tracks = Tracks.from_mpd(
            url=mpd_url,
            data=mpd_data,
            session=self.session,
            source=self.ALIASES[0]
        )

        return tracks

    def get_chapters(self, title):
        # TODO: Check some videos if has chapters
        cc = 1
        chaps = []
        if credits := title.service_data["credits_marker"]:
            chaps.append(MenuTrack(
                number=1,
                title=f"Scene {cc:02}",
                timecode="0:00:00.000"
            )),
            cc += 1
            chaps.append(MenuTrack(
                number=cc,
                title="Credits",
                timecode=(str(timedelta(seconds=int(credits))))
            ))
            return chaps
        else:
             return []

    def certificate(self, **_):
        return self.license(**_)
    
    def license(self, challenge, **_):
        license_data = None
        timestamp_license = str(round(time.time()))
        license_api = f"/v5/videos/{self.title_id}/drms.json?offline=false&stream_ids={self.stream_id}&dt=dt3&device_id={self.device_id}&app=100531a&token={self.authorization}"+f"&t={timestamp_license}"
        if license_data:
            license_data = json.dumps(license_data, separators=(",", ":"))
            license_api += license_data
        license_signature = hmac.new(self.app_secret_key.encode("utf-8"), license_api.encode("utf-8"), sha1).hexdigest()
        r = self.session.get(
            url=self.config["endpoints"]["get_license_api"].format(contentID=self.title_id,  stream_id= self.stream_id, deviceID=self.device_id, token=self.authorization),
            headers = {
                "accept": "*/*",
                "user-agent": "okhttp/4.10.0",
                "x-viki-app-ver": "23.5.0",
                "host": "api.viki.io",
                "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                "signature": license_signature,
                "timestamp": timestamp_license,
                "x-viki-carrier": "",
                "x-viki-connection-type": "ETHERNET",
                "x-viki-device-id": self.device_id,
                "x-viki-device-model": "AOSP TV on x86",
                "x-viki-device-os-ver": "13",
                "x-viki-manufacturer": "Google",
            }
        )

        self.license_api = r.json()["dt3"]

        headers = {
            'content-type': 'application/octet-stream',
            'host': 'manifest-viki.viki.io',
            'user-agent': 'Viki/23.5.0 (Linux;Android 12) ExoPlayerLib/2.18.6',
        }

        return self.session.post(
            url=self.license_api,
            data=challenge,  # expects bytes
            headers=headers
        ).content
    
    def configure(self):
        self.log.info(f" + Using Device ID : {self.device_id}")
        self.authorization = self.get_token()

    # def signature(self, api , d):
    #     timestamp = str(round(time.time()))
    #     message =  f"{api}" + f"&t={timestamp}"
    #     data = d
    #     if data:
    #         data = json.dumps(data, separators=(",", ":"))
    #         message += data
    #     signature = hmac.new(self.app_secret_key.encode("utf-8"), message.encode("utf-8"), sha1).hexdigest()
    #     print(signature)
    #     print(timestamp)
    #     return signature
    
    def get_token(self):
        token_cache_path = self.get_cache("session_token.json")
        if os.path.isfile(token_cache_path):
            with open(token_cache_path, encoding="utf-8") as fd:
                token = json.load(fd)
                self.log.info(" + Using Cached Token...")
                return token["token"]
        # get new token
        else:
            token = self.login()
            self.save_token(token, token_cache_path)
            if os.path.isfile(token_cache_path):
                with open(token_cache_path, encoding="utf-8") as fd:
                    token = json.load(fd)
                    self.log.info(" + Using New Token...")
                    return token["token"]

    def save_token(self, token, to):
        os.makedirs(os.path.dirname(to), exist_ok=True)
        with open(to, "w", encoding="utf-8") as fd:
            json.dump(token ,fd)
        return token

    def login(self):
        login_code_data = {"type": "androidtv", "device_id": "3301c781f409497a"} or None
        timestamp_login_code = str(round(time.time()))
        code_gen_api = "/v5/devices.json?app=100531a"+f"&t={timestamp_login_code}"
        if login_code_data:
            login_code_data = json.dumps(login_code_data, separators=(",", ":"))
            code_gen_api += login_code_data
        login_code_signature = hmac.new(self.app_secret_key.encode("utf-8"), code_gen_api.encode("utf-8"), sha1).hexdigest()

        login_code_headers = {
            "accept": "*/*",
            "user-agent": "okhttp/4.10.0",
            "x-viki-app-ver": "23.5.0",
            "host": "api.viki.io",
            "signature": login_code_signature,
            "timestamp": timestamp_login_code,
            "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
            "x-viki-carrier": "",
            "x-viki-connection-type": "ETHERNET",
            "x-viki-device-id": self.device_id,
            "x-viki-device-model": "AOSP TV on x86",
            "x-viki-device-os-ver": "13",
            "x-viki-manufacturer": "Google"
        }
        
        res = self.session.post(
            url=self.config["endpoints"]["login_code"],
            headers=login_code_headers,
            data=login_code_data,
        ).json()
        code = res["device_registration_code"]
        self.log.info(f"Login Code : {code}")
        self.log.info(f"Go to https://www.viki.com/androidtv and enter the Login code")
        devicecode_choice = input("Did you enter the code as informed above? (y/n): ")

        if devicecode_choice.lower() == "y" or devicecode_choice.lower() == "yes":
            #verify code
            timestamp_verify_data = str(round(time.time()))
            verify_api = f"/v5/devices/{code}.json?device_code={code}&type=androidtv&app=100531a"+f"&t={timestamp_verify_data}"
            verify_data = None
            if verify_data:
                verify_data = json.dumps(verify_data, separators=(",", ":"))
                verify_api += verify_data
            verify_data_signature = hmac.new(self.app_secret_key.encode("utf-8"), verify_api.encode("utf-8"), sha1).hexdigest()
            verify_code_headers = {
                "accept": "*/*",
                "user-agent": "okhttp/4.10.0",
                "x-viki-app-ver": "23.5.0",
                "host" : "api.viki.io",
                "signature": verify_data_signature,
                "timestamp": timestamp_verify_data,
                "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                "x-viki-carrier": "",
                "x-viki-connection-type": "ETHERNET",
                "x-viki-device-id": self.device_id,
                "x-viki-device-model": "AOSP TV on x86",
                "x-viki-device-os-ver": "13",
                "x-viki-manufacturer": "Google"
            }
            r = self.session.get(
                url=self.config["endpoints"]["verify"].format(code=code),
                headers=verify_code_headers,
            ).json()
            device_token = r["device_token"]

            #get_session_token
            timestamp_token_api = str(round(time.time()))
            token_api = "/v5/sessions.json?app=100531a"+f"&t={timestamp_token_api}"
            token_data = {"device_token": f"{device_token}", "type": "androidtv"}
            if token_data:
                token_data = json.dumps(token_data, separators=(",", ":"))
                token_api += token_data
            token_api_signature = hmac.new(self.app_secret_key.encode("utf-8"), token_api.encode("utf-8"), sha1).hexdigest()
            token_headers = {
                "accept": "*/*",
                "user-agent": "okhttp/4.10.0",
                "x-viki-app-ver": "23.5.0",
                "host" : "api.viki.io",
                "signature": token_api_signature,
                "timestamp": timestamp_token_api,
                "x-viki-as-id": f"{self.app_id}-{round(time.time())}-5648",
                "x-viki-carrier": "",
                "x-viki-connection-type": "ETHERNET",
                "x-viki-device-id": self.device_id,
                "x-viki-device-model": "AOSP TV on x86",
                "x-viki-device-os-ver": "13",
                "x-viki-manufacturer": "Google"
            }
            t = self.session.post(
                url=self.config["endpoints"]["session_token"],
                headers=token_headers,
                data=token_data,
            ).json()
            return t
        else:
            self.log.exit(f" - Failed to get session token, response was not JSON")
            
    def fix_mpd(self, manifest_url, title):
        mpd_contents = self.session.get(manifest_url).text
        mpd_str = re.sub(r'<AdaptationSet lang="en" mimeType="audio/mp4"', r'<AdaptationSet lang="{}" mimeType="audio/mp4"'.format(title.original_lang), mpd_contents)
        return mpd_str



