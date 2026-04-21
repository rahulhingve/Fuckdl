import click
import hashlib
import re
import uuid
import json
import os
import time
import requests

from datetime import datetime
from requests import Request
from fuckdl.objects import VideoTrack, AudioTrack, MenuTrack, TextTrack, Title, Track, Tracks
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.io import get_ip_info
from fuckdl.utils.widevine.device import LocalDevice

class HULUJP(BaseService):
    """
    Service code for HULU(JP) Streaming Service (https://hulu.jp).
    Version: 26.04.03

    Original creator (CodeName393)

    \n

    Authorization: Credentials or Cookies\n
    Security: UHD@L3/SL2000 FHD@L3/SL2000

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["HULUJP", "HJ", "HULUJAPAN"]
    TITLE_RE = [
        r"^(?:https?://(?:www\.)?hulu\.jp/)?(?P<id>[a-zA-Z0-9-]+)(?:\?.*)?$"
    ]

    VIDEO_CODEC_MAP = {
        'H264': 'avc',
        'H265': 'hevc',
        'VP9': 'vp9'
    }
    
    VIDEO_RANGE_MAP = {
        'SDR': 'sdr',
        'HDR10': 'hdr',
        'DV': 'dv'
    }
    
    @staticmethod
    @click.command(name="HULUJP", short_help="https://hulu.jp", help=__doc__)
    @click.argument("title", type=str)
    @click.option(
        "--dub-type",
        type=click.Choice(['sub', 'dub'], case_sensitive=False),
        default='dub',
        help="Prefer subtitle or dubbing version for content type if available."
    )
    @click.pass_context
    def cli(ctx, **kwargs):
        return HULUJP(ctx, **kwargs)

    def __init__(self, ctx, title, dub_type):
        super().__init__(ctx)
        m = self.parse_title(ctx, title)
        self.title_id = m["id"]
        self.dub_type = dub_type

        self.vcodec = ctx.parent.params["vcodec"] or "H264"
        self.acodec = ctx.parent.params["acodec"]
        self.range = ctx.parent.params["range_"]
        self.wanted = ctx.parent.params["wanted"]
        self.quality = ctx.parent.params["quality"] or 1080
        cdm = ctx.obj.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY) or \
                         hasattr(cdm, "certificate_chain")

        self.account_tokens = {}
        self.active_session = {}
        self.playback_data = {}
        self.token_expires_at = 0
        self.video_cache = {}

        assert ctx.parent is not None

        self.configure()

    def _get_language_name(self, lang_code):
        """Get readable language name"""
        lang_names = {
            "ja": "Japanese",
            "en": "English",
            "ko": "Korean",
            "zh": "Chinese",
            "fr": "French",
            "de": "German",
            "es": "Spanish",
            "it": "Italian",
            "pt": "Portuguese",
            "ru": "Russian",
            "ar": "Arabic",
            "hi": "Hindi",
            "th": "Thai",
            "vi": "Vietnamese",
            "id": "Indonesian",
            "ms": "Malay",
            "tl": "Tagalog",
            "my": "Burmese",
            "km": "Khmer",
            "lo": "Lao",
        }
        return lang_names.get(lang_code[:2], lang_code)

    def configure(self):
        self.log.info("Preparing...")

        if (self.range != "SDR" or self.quality > 1080) and self.vcodec != "H265":
            self.vcodec = "H265"
            self.log.info(f" + Switched video codec to H265 to be able to get {self.range} dynamic range.")

        language = self.config.get("preferences", {}).get("language", "ja")
        self.session.headers.update({
            "User-Agent": self.config["device"]["user_agent"].format(version=self.config["device"]["app_version"]),
            "Accept-Language": language,
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        })

        ip_info = get_ip_info(self.session, fresh=True)
        
        if isinstance(ip_info, dict):
            if "countryCode" in ip_info:
                region = ip_info["countryCode"].upper()
            elif "country_code" in ip_info:
                region = ip_info["country_code"].upper()
            elif "country" in ip_info:
                country_value = ip_info["country"]
                if isinstance(country_value, str) and len(country_value) == 2:
                    region = country_value.upper()
                else:
                    region = "JP"
                    self.log.warning(f" + IP Region: Could not determine country code from: {country_value}")
            else:
                region = "JP"
                self.log.warning(" + IP Region: No country code found in IP info")
        else:
            region = "JP"
            self.log.warning(f" + IP Region: Unexpected IP info format: {type(ip_info)}")
        
        self.log.info(f" + IP Region: {region}")
        if region != "JP":
            raise self.log.exit("  - It is not currently available in the country.")
        
        self.log.info("Logging into HULU JP...")
        self._login()

        self.log.info(" + Login successful")

    def _login(self):
        """Main login method - prioritizes credentials over cookies"""
        use_cookies = False
        use_credentials = False
        
        # Check if there are cookies in the session
        if len(self.session.cookies) > 0:
            cookie_names = [c.name for c in self.session.cookies]
            self.log.debug(f"Cookies found: {cookie_names}")
            if any(name.startswith('__cf') or name.startswith('_cf') for name in cookie_names):
                self.log.info(" + Cookies detected, trying cookie login...")
                use_cookies = True
            else:
                self.log.debug(" + Cookies present but don't look like Hulu cookies, ignoring...")
                self.session.cookies.clear()
        
        # Check if credentials are available
        if self.credentials and self.credentials.username and self.credentials.password:
            self.log.debug(" + Credentials detected")
            use_credentials = True
        
        # Determine cache key
        if use_credentials:
            self.cache_key = f"tokens_{self.credentials.sha1}"
        elif use_cookies:
            self.cache_key = "tokens_cookie_session"
        else:
            raise self.log.exit(" - No credentials or valid cookies found for authentication.")
        
        # Try to load from cache
        cache_path = self.get_cache(f"{self.cache_key}.json")
        
        if os.path.isfile(cache_path):
            self._login_with_cache(cache_path, use_cookies)
        else:
            self.log.info(" + Getting new tokens...")
            if use_credentials:
                self._perform_full_login()
            else:
                self._perform_cookie_login()
        
        self.log.info(" + Fetching session data...")
        session_data = self._get_user_info()
        
        if not session_data:
            session_data = {}
        
        if "account_id" not in session_data:
            session_data["account_id"] = "unknown"
            
        if "profile_id" not in session_data:
            session_data["profile_id"] = "unknown"
        
        session_data["session_id"] = uuid.uuid4()
        
        self.active_session = session_data
        
        self.log.info("Session data setup successfully.")
        self.log.debug(f"Active session: {self.active_session}")
        
        account_id = self.active_session.get("account_id", "unknown")
        profile_id = self.active_session.get("profile_id", "unknown")
        session_id = str(self.active_session['session_id'])
        
        self.log.info(f' + Account ID (UUID): {account_id}')
        self.log.info(f' + Profile ID: {profile_id}')
        self.log.info(f" + Session ID: {session_id}")

    def _login_with_cache(self, path, is_cookie):
        """Login using cached tokens"""
        try:
            self.log.info(" + Using cached tokens...")
            with open(path, encoding="utf-8") as fd:
                self.account_tokens = json.load(fd)
            
            cache_mod_time = os.path.getmtime(path)
            expires_in = self.account_tokens["token"].get("expiresIn") or 43200
            self.token_expires_at = cache_mod_time + expires_in - 60

            try:
                self._refresh()
            except Exception as e:
                self.log.warning(f" - Failed to refresh token from cache ({e}). Getting new tokens...")
                if is_cookie:
                    self._perform_cookie_login()
                else:
                    self._perform_full_login()

        except (KeyError, ValueError, TypeError) as e:
            self.log.warning(f" - Cached token data is invalid or corrupted ({e}). Getting new tokens...")
            if is_cookie:
                self._perform_cookie_login()
            else:
                self._perform_full_login()

    def _perform_cookie_login(self):
        """Login using browser cookies"""
        token_data = self._get_cookie_token()
        new_token_data = {
            "token": {
                "token_id": token_data["id"],
                "accessToken": token_data["token"],
            },
            "session": {
                "gaiaToken": token_data["gaiaToken"],
                "sessionToken": token_data["sessionToken"]
            }
        }
        self._apply_new_tokens(new_token_data)

    def _perform_full_login(self):
        """Full login with email and password"""
        temp_session = self._create_session()
        login_data = self._login_with_password(self.credentials.username, self.credentials.password, temp_session)
        
        profile_list_data = self._get_profile_list(login_data)
        profiles = profile_list_data.get("profiles", [])
        
        selected_profile = None
        
        # Check config for profile preference
        if self.config.get("preferences") and "profile" in self.config["preferences"]:
            try:
                profile_index = int(self.config["preferences"]["profile"])
                if not 0 <= profile_index < len(profiles):
                    raise ValueError(f"Index out of range (0-{len(profiles)-1})")
                
                selected_profile = profiles[profile_index]
            except (ValueError, TypeError):
                raise self.log.exit(" - Profile index in configuration is invalid.")
        else:
            # Auto-select: prefer profiles without PIN and not kids
            selected_profile = next(
                (p for p in profiles if p.get("values", {}).get("cluster_id") is not None and not p.get("values", {}).get("has_pin")),
                None
            )
            if not selected_profile and profiles:
                selected_profile = next(
                    (p for p in profiles if p.get("values", {}).get("is_owner")),
                    None
                )
            if not selected_profile and profiles:
                selected_profile = profiles[0]
            if not selected_profile:
                raise self.log.exit(" - Auto-selection failed: No suitable profile found. Please configure a specific profile.")

        self.log.info(f" + Selected Profile: {selected_profile['display_name']}({selected_profile['uuid']})")
        profile_pin = ""
        if selected_profile.get("values", {}).get("has_pin"):
            self.log.warning("  - This profile is PIN protected.")
            try:
                profile_pin = input("Enter a profile pin: ")
                if not profile_pin:
                    raise self.log.exit("  - PIN is required, but no value was entered.")
                if not profile_pin.isdigit():
                    raise self.log.exit("  - Invalid PIN. Please enter only numbers.")
                if len(profile_pin) < 4:
                    raise self.log.exit("  - PIN is too short. Please enter at least 4 digits.")
                if len(profile_pin) > 4:
                    self.log.warning("  - PIN is longer than 4 digits. Using the first 4 digits.")
                    profile_pin = profile_pin[:4]
            except KeyboardInterrupt:
                raise self.log.exit("\n - PIN input cancelled by user.")

        profile_select_data = self._switch_profile(selected_profile["uuid_in_schema"], login_data, profile_pin)
        final_token_data = {
            "token": {
                "token_id": login_data["token_id"],
                "accessToken": login_data["access_token"],
                "expiresIn": login_data["expires_in"],
                "refreshToken": login_data["refresh_token"]
            },
            "session": {
                "gaiaToken": login_data["gaia_token"],
                "sessionToken": profile_select_data["session_token"]
            }
        }
        self._apply_new_tokens(final_token_data)

    def _refresh(self):
        """Refresh access token"""
        if hasattr(self, 'token_expires_at') and time.time() < self.token_expires_at:
            self.log.debug(f" + Token is valid until: {datetime.fromtimestamp(self.token_expires_at).strftime('%Y-%m-%d %H:%M:%S')}")
            return

        self.log.warning(" + Token expired. Refreshing...")
        try:
            refreshed_data = self._refresh_token()

            new_token_data = {
                "token": {
                    "token_id": refreshed_data["token_id"],
                    "accessToken": refreshed_data["access_token"],
                    "expiresIn": refreshed_data["expires_in"],
                    "refreshToken": refreshed_data["refresh_token"]
                },
                "session": self.account_tokens["session"]
            }

            self._apply_new_tokens(new_token_data)
        except Exception as e:
            raise self.log.exit(f"Refresh Token Expired: {e}")

    def _apply_new_tokens(self, token_data, is_cookie=False):
        """Save new tokens to cache"""
        self.account_tokens = token_data
        
        expires_in = self.account_tokens["token"].get("expiresIn") or 43200
        self.token_expires_at = time.time() + expires_in - 60
        self.log.debug(f"  + New Token is valid until: {datetime.fromtimestamp(self.token_expires_at).strftime('%Y-%m-%d %H:%M:%S')}")
        
        cache_path = self.get_cache(f"{self.cache_key}.json")
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as fd:
            json.dump(self.account_tokens, fd)

    def get_titles(self):
        """Get titles (movies or series)"""
        # Check if title_id is numeric (meta_id) or slug
        if str(self.title_id).isdigit():
            meta_id = self.title_id
        else:
            src = self.session.get(
                url=f"https://www.hulu.jp/{self.title_id}",
                headers={
                    "User-Agent": self.config["device"]["user_agent_browser"]
                }
            )
            pattern = r'"titleSlug":\s*{\s*"[^"]+":\s*{\s*"\$type":\s*"ref",\s*"value":\s*\["meta",\s*(\d+)]'
            match = re.search(pattern, src.text) 
            if not match:
                raise self.log.exit(" - Failed to find meta_id from page source.")
            meta_id = match.group(1)

        data = self._get_metas(meta_id)
        media_type = data.get("media_type")
        self.log.debug(f" + Content Type: {media_type.upper()}")

        if media_type == "movie":
            lead = data.get("lead_episode")
            sub_lead = data.get("sub_lead_episode")
            dub_lead = data.get("dub_lead_episode")
            if sub_lead or dub_lead or lead:
                target_data = None
                if self.dub_type == "dub":
                    target_data = dub_lead or lead or sub_lead
                elif self.dub_type == "sub":
                    target_data = sub_lead or lead or dub_lead
                
                if not target_data:
                    target_data = lead

                item = self._get_metas(target_data["meta_id"])
                lang_val = item.get("original_audio_language", {}).get("value", "en")

                return Title(
                    id_=item["id"],
                    type_=Title.Types.MOVIE,
                    name=item.get("header") or item.get("name"),
                    year=data.get("premiere_year"),
                    original_lang=lang_val,
                    source=self.ALIASES[0],
                    service_data=item
                )
            
        elif media_type == "tv":
            return self._get_series(data)
        
        else:
            raise self.log.exit(f" - Unsupported content type: {media_type}")

    def _get_series(self, series_data):
        """Get series episodes"""
        seasons = series_data.get("seasons")
        episodes = []

        hierarchy_data = series_data.get('hierarchy_types', [])
        available_types = {h['key'] for h in hierarchy_data}
        
        hierarchy_targets = {}
        default_target_ids = [s["id"] for s in seasons] if seasons else [series_data["id"]]

        # Handle episode types
        if "episode" in available_types or "episode_sub" in available_types or "episode_dub" in available_types:
            req_hierarchy = "episode"
            if self.dub_type == "sub":
                if "episode_sub" in available_types:
                    req_hierarchy = "episode_sub"
                elif "episode" not in available_types and "episode_dub" in available_types:
                    req_hierarchy = "episode_dub"
            elif self.dub_type == "dub":
                if "episode_dub" in available_types:
                    req_hierarchy = "episode_dub"
                elif "episode" not in available_types and "episode_sub" in available_types:
                    req_hierarchy = "episode_sub"
            
            hierarchy_targets[req_hierarchy] = default_target_ids
        else:
            # Handle other hierarchy types
            for h in hierarchy_data:
                k = h["key"]
                s_ids = h.get("season_ids", [])
                if k and "clip" not in k.lower():
                    hierarchy_targets[k] = s_ids if s_ids else default_target_ids
        
        # Handle clips
        if "clip" in available_types:
            hierarchy_targets["clip"] = [series_data["id"]]

        for h_type, target_ids in hierarchy_targets.items():
            is_clip = h_type == "clip"
            
            for parent_id in target_ids:
                pack = self._get_metas_children(parent_id, h_type=h_type)
                items = pack.get("metas", [])
                for item in items:
                    lang_val = item.get("original_audio_language", {}).get("value") or "en"
                    season_num = 0 if is_clip else int(item.get("season_number") or 1)

                    episodes.append(
                        Title(
                            id_=item["id"],
                            type_=Title.Types.TV,
                            name=series_data["name"],
                            season=season_num,
                            episode=int(item.get("episode_number") or 0),
                            episode_name=item.get("header") or item.get("name"),
                            year=series_data.get("premiere_year"),
                            original_lang=lang_val,
                            source=self.ALIASES[0],
                            service_data=item
                        )
                    )

        if not episodes:
            attempted_keys = list(hierarchy_targets.keys())
            raise self.log.exit(f"Failed to retrieve episodes. API returned empty list for types: {attempted_keys}")

        return episodes

    def _get_tracks_for_variants(self, title, fetch_variant_func):
        """Get tracks for different codec/range variants"""
        all_tracks = Tracks()
        
        # Define variants to try based on settings
        variants = []
        
        # Handle HDR vs SDR
        if self.range and self.range != "SDR":
            variants.append((self.vcodec, self.range))
        else:
            variants.append((self.vcodec, "SDR"))
        
        # Always try SDR as fallback if not already included
        if self.range != "SDR":
            variants.append((self.vcodec, "SDR"))
        
        # Try HEVC first for better quality
        if self.vcodec != "H265":
            variants.insert(0, ("H265", self.range if self.range else "SDR"))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variants = []
        for v in variants:
            key = (v[0], v[1])
            if key not in seen:
                seen.add(key)
                unique_variants.append(v)
        
        # Try each variant
        for codec, range_val in unique_variants:
            self.log.debug(f"Trying variant: codec={codec}, range={range_val}")
            try:
                tracks = fetch_variant_func(title, codec, range_val)
                if tracks and tracks.videos:
                    self.log.info(f" + Found tracks for {codec} {range_val}")
                    all_tracks.add(tracks, warn_only=True)
                    break
                elif tracks and tracks.audios:
                    all_tracks.add(tracks, warn_only=True)
            except Exception as e:
                self.log.debug(f"Variant {codec} {range_val} failed: {e}")
                continue
        
        return all_tracks
    
    def get_tracks(self, title):
        """Get tracks for a title"""
        medias_data = self._get_metas_media(title.id)
        
        def fetch_variant(title, codec, range_val):
            target_codec = self.VIDEO_CODEC_MAP.get(codec, 'avc')
            target_range = self.VIDEO_RANGE_MAP.get(range_val, 'sdr')
            
            self.log.debug(f"Fetching {codec} {range_val} manifest...")
            
            selected_media = None
            for media in medias_data:
                file_type = media["values"].get("file_type", "")
                dynamic_range = media["values"].get("dynamic_range_type", "sdr")
                if file_type in ("video/4k", "video") and dynamic_range == target_range:
                    selected_media = media
                    break
            
            if not selected_media:
                self.log.debug(f" - No {codec} {range_val} stream found. Falling back to Default SDR.")
                for media in medias_data:
                    if media["values"].get("file_type") == "video" and media["values"].get("dynamic_range_type") == "sdr":
                        selected_media = media
                        break
            
            if not selected_media:
                self.log.debug(" - No stream found")
                return Tracks()
            
            self.log.debug(f" + Media ID: {selected_media['media_id']}")
            tracks = self._fetch_manifest_tracks(title, selected_media['media_id'], target_codec)
            
            if range_val == "HDR10" and selected_media["values"].get("dynamic_range_type") == "hdr":
                for video in tracks.videos:
                    video.hdr10 = True
            
            return tracks
        
        tracks = self._get_tracks_for_variants(title, fetch_variant)
        
        # Add surround audio if available
        for media in medias_data:
            if media["values"].get("sub_file_type") == "stereo/surround":
                self.log.debug("Fetching Surround audio tracks...")
                self.log.debug(f" + Media ID: {media['media_id']}")
                surround_tracks = self._fetch_manifest_tracks(title, media['media_id'], 'hevc')
                for audio in surround_tracks.audios:
                    tracks.add(audio, warn_only=True)
                break

        # Add subtitles
        if media_values := self.playback_data[title.id]["media_auth"]["values"]:
            for sub_lang in ["en", "ja"]:
                for sub_type in ["normal", "forced", "cc"]:
                    sub_key_standard = f"caption_{sub_lang}_{sub_type}_standard"
                    sub_key_basic = f"caption_{sub_lang}_{sub_type}"
                    
                    sub_url = None
                    if sub_lang == "en":
                        sub_url = media_values.get(sub_key_standard) or media_values.get(sub_key_basic)
                    else:
                        sub_url = media_values.get(sub_key_standard)
                    
                    if sub_url:
                        tracks.add(
                            TextTrack(
                                id_=hashlib.md5(sub_url.encode()).hexdigest()[0:6],
                                source=self.ALIASES[0],
                                url=sub_url,
                                codec="vtt",
                                language=sub_lang,
                                forced=sub_type == "forced",
                                sdh=sub_type == "cc"
                            )
                        )

        # Add thumbnail track if available
        if title.service_data and "thumbnail" in title.service_data:
            thumb_url = title.service_data["thumbnail"]
            self.log.debug(f" + Adding thumbnail: {thumb_url}")
            tracks.thumbnail = thumb_url

        # Add language names to tracks
        for audio in tracks.audios:
            # Obtener nombre legible del lenguaje
            if hasattr(audio.language, 'display_name'):
                lang_name = audio.language.display_name()
            else:
                lang_name = self._get_language_name(str(audio.language))
            
            audio.name = f"{lang_name}"
            if hasattr(audio, 'is_original_lang') and audio.is_original_lang:
                audio.name += " [Original]"
                
        for subtitle in tracks.subtitles:
            if hasattr(subtitle.language, 'display_name'):
                lang_name = subtitle.language.display_name()
            else:
                lang_name = self._get_language_name(str(subtitle.language))
            
            subtitle.name = f"{lang_name}"
            if hasattr(subtitle, 'is_original_lang') and subtitle.is_original_lang:
                subtitle.name += " [Original]"

        return self._post_process_tracks(tracks)
    
    def _fetch_manifest_tracks(self, title, media_id=None, codec=None):
        """Fetch manifest and extract tracks"""
        if codec is None:
            codec = self.VIDEO_CODEC_MAP.get(self.vcodec, 'avc')
        
        auth = self._playback_auth(title, media_id)
        video_id = auth["media"]["ovp_video_id"]
        self.log.debug(f' + Video ID: {video_id}')
        
        # Check cache
        cache_key = f"{video_id}_{codec}"
        if cache_key in self.video_cache:
            self.log.debug(" + Using cached media data")
            media = self.video_cache[cache_key]
        else:
            media = self._playback_media(title, auth, codec)
            self.video_cache[cache_key] = media
        self._playback_close(auth)

        dash_sources = [x for x in media.get("sources", []) if x.get("label") == "dash_cenc"]
        if not dash_sources:
            self.log.warning(" - Playable stream not found.")
            return Tracks()

        best_source = max(dash_sources, key=lambda x: int(x.get('resolution', '0x0').split('x')[1]))

        key_systems = best_source["key_systems"]
        target_drm_key = "com.microsoft.playready" if self.playready else "com.widevine.alpha"
        drm_data = key_systems.get(target_drm_key)

        if drm_data and drm_data.get("license_url"):
            license_url = drm_data["license_url"]
        else:
            self.log.warning(f" - Unable to handle with invalid DRM type. {target_drm_key} info not found or license_url is empty in response.")
            return Tracks()

        self.playback_data[title.id] = {
            "media_auth": auth["media"],
            "license_url": license_url,
            "certificate_url": drm_data.get("certificate_url")
        }

        manifest_url = best_source["src"]
        log_level = self.config.get("preferences", {}).get("manifest_log", "debug")
        if log_level == "info":
            self.log.info(f" + Manifest URL: {manifest_url}")
        else:
            self.log.debug(f" + Manifest URL: {manifest_url}")
        
        tracks = Tracks.from_mpd(
            url=manifest_url,
            session=self.session,
            source=self.ALIASES[0]
        )
        
        # Mark original language tracks
        for audio in tracks.audios:
            if audio.language == title.original_lang:
                audio.is_original_lang = True
                
        for subtitle in tracks.subtitles:
            if subtitle.language == title.original_lang:
                subtitle.is_original_lang = True
                
        return tracks
    
    def _post_process_tracks(self, tracks):
        """Post-process tracks"""
        for video in tracks.videos:
            video.size = 0
        
        for audio in tracks.audios:
            if audio.channels == 6.0:
                audio.channels = 5.1
            audio.size = 0
            
        for subtitle in tracks.subtitles:
            subtitle.codec = "vtt"

        return tracks
    
    def get_chapters(self, title):
        """Get chapter markers"""
        content_data = self.playback_data[title.id]["media_auth"]['values']
    
        opening_skip_start = content_data.get("opening_start_position", 0)
        opening_skip_end = content_data.get("opening_end_position", 0)
        credits_sec = content_data.get("ending_start_position", 0)

        video_skips = content_data.get("video_skips", [])
        if opening_skip_end == 0 and video_skips:
            first_skip = video_skips[0]
            opening_skip_start = first_skip.get("start_position", 0)
            opening_skip_end = first_skip.get("end_position", 0)

        pre_chapter = []

        if opening_skip_end > 1:
            if opening_skip_start > 1: 
                pre_chapter.append(("Scene", 0))
                pre_chapter.append(("Intro", opening_skip_start))
                pre_chapter.append(("Scene", opening_skip_end))
            else:
                pre_chapter.append(("Intro", 0))
                pre_chapter.append(("Scene", opening_skip_end))
        else:
            pre_chapter.append(("Scene", 0))

        if credits_sec > 0:
            pre_chapter.append(("Credits", credits_sec))
        
        if len(pre_chapter) == 1 and pre_chapter[0] == ("Scene", 0):
            return []
            
        chapters = []
        scene_counter = 1
        
        for i, (title_template, time_sec) in enumerate(pre_chapter):
            chapter_number = i + 1

            if title_template == "Scene":
                chapter_title = f"Scene {scene_counter:02d}"
                scene_counter += 1
            else:
                chapter_title = title_template

            timecode = datetime.utcfromtimestamp(float(time_sec)).strftime('%H:%M:%S.%f')[:-3]
            
            chapters.append(
                MenuTrack(
                    number=chapter_number,
                    title=chapter_title,
                    timecode=timecode
                )
            )

        return chapters

    def certificate(self, **kwargs):
        """Return service certificate"""
        cert_url = self.playback_data.get(kwargs.get('title', {}).id, {}).get("certificate_url")
        if cert_url:
            try:
                res = self.session.get(cert_url)
                res.raise_for_status()
                return res.content
            except Exception as e:
                self.log.warning(f" - Failed to fetch certificate: {e}. Using default.")
        return None if self.playready else self.config["certificate"]

    def license(self, challenge, title, **_):
        """Get license"""
        headers = {
            "User-Agent": self.config["device"]["user_agent_playback"],
        }

        if self.playready:
            headers.update({
                "Accept": "application/xml",
                "Content-Type": "text/xml; charset=utf-8",
            })
        else:
            headers.update({"Content-Type": "application/octet-stream"})

        try:
            res = self.session.post(self.playback_data[title.id]["license_url"], headers=headers, data=challenge)
            res.raise_for_status()
            return res.content
        except Exception as e:
            raise self.log.exit(f"License request failed: {e}")
    
    def _get_search(self, title):
        """Search for content"""
        query_dict = {
            "page": "1",
            "limit": "20",
            "with_total_count": "true",
            "keyword": title,
            "condition": {"meta_schema_id": [3, 4, 8, 11]},
            "datasource": "decorator",
            "device_code": self.config["device"]["device_code"],
            "app_id": self.config["device"]["app_id"],
        }
        params = {
            "q": json.dumps(query_dict, separators=(",", ":")),
            "with_total_count": "true",
            "ignore_search_time_range": "true",
        }
        endpoint = self.config["endpoints"]["metas"].format(id="dsearch")
        data = self._request("GET", endpoint, params=params)
        return data.get("metas", [])
    
    def _get_metas(self, title_id):
        """Get metadata for a title"""
        params = {
            "expand_object_flag": "0",
            "app_id": self.config["device"]["app_id"],
            "device_code": self.config["device"]["device_code"],
            "datasource": "decorator"
        }
        data = self._request("GET", self.config["endpoints"]["metas"].format(id=title_id), params=params)
        return data
    
    def _get_metas_children(self, season_id, h_type="episode"):
        """Get children metadata (episodes)"""
        params = {
            "expand_object_flag": "0",
            "sort": "sort:asc,id_in_schema:asc",
            "order": "asc",
            "app_id": self.config["device"]["app_id"],
            "device_code": self.config["device"]["device_code"],
            "datasource": "decorator",
            "limit": "999",
            "page": "1",
            "with_total_count": "true",
            "hierarchy_type": h_type,
            "only_searchable": "true"
        }
        data = self._request("GET", self.config["endpoints"]["metas"].format(id=season_id) + "/children", params=params)
        return data
    
    def _get_metas_media(self, title_id):
        """Get media information"""
        params = {
            "fields": "values",
            "app_id": self.config["device"]["app_id"],
            "device_code": self.config["device"]["device_code"],
            "datasource": "decorator"
        }
        data = self._request("GET", self.config["endpoints"]["metas"].format(id=title_id) + "/medias", params=params)
        return data["medias"]
    
    def _playback_auth(self, title, media_id=None):
        """Get playback authorization"""
        payload = {
            "service": title.service_data["service"],
            "meta_id": f'asset:{title.service_data["id_in_schema"]}',
            "device_code": int(self.config["device"]["device_code"]),
            "with_resume_point": True,
            "vuid": self.active_session["session_id"].hex,
            "user_id": self.account_tokens["token"]["token_id"],
            "app_id": int(self.config["device"]["app_id"])
        }
        
        if media_id:
            payload["media_id"] = media_id
            
        headers = {
            "X-User-Id": str(self.account_tokens["token"]["token_id"]),
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
            "X-Session-Token": self.account_tokens["session"]["sessionToken"]
        }
        
        data = self._request("POST", self.config["endpoints"]["playback_auth"], headers=headers, payload=payload)
        return data
    
    def _playback_media(self, title, auth, codec):
        """Get playback media information"""
        endpoint = self.config["endpoints"]["playback_media"].format(
            service_id=title.service_data["service"],
            video_id=auth["media"]["ovp_video_id"]
        )
        params = {
            "device_code": self.config["device"]["device_code"],
            "codecs": codec,
            "viewing_url": f'https://www.hulu.jp/watch/{auth["media"]["values"]["asset_id"]}',
            "app_id": self.config["device"]["app_id"]
        }
        headers = {
            "X-Playback-Session-Id": auth["playback_session_id"],
            "X-User-Id": str(self.account_tokens['token']["token_id"]),
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
            "X-Session-Token": self.account_tokens['session']['sessionToken']
        }
        data = self._request("GET", endpoint, params=params, headers=headers)
        return data
    
    def _playback_close(self, auth):
        """Close playback session"""
        endpoint = self.config["endpoints"].get("playback_close")
        if not endpoint:
            return
        headers = {
            "X-Playback-Session-Id": auth["playback_session_id"],
            "X-User-Id": str(self.account_tokens['token']["token_id"]),
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
            "X-Session-Token": self.account_tokens['session']['sessionToken']
        }
        try:
            self.session.post(endpoint, headers=headers)
        except Exception:
            pass  # Ignore close errors

    def _get_cookie_token(self):
        """Get token from cookies"""
        headers = {
            "User-Agent": self.config["device"]["user_agent_browser"],
            "Cookie": "; ".join([f"{name}={value}" for name, value in self.session.cookies.items()])
        }
        data = self._request("POST", self.config["endpoints"]["cookies_token"], headers=headers)
        return data["authContext"]
    
    def _create_session(self):
        """Create a new session"""
        params = {
            "app_version": self.config["device"]["app_version"],
            "system_version": self.config["device"]["os_version"],
            "device_code": self.config["device"]["device_code"],
            "manufacturer": self.config["device"]["manufacturer"],
            "is_mobile": "false",
            "os_version": self.config["device"]["os_version"],
            "os_build_id": self.config["device"]["os_build_id"],
            "device_manufacturer": self.config["device"]["manufacturer"],
            "device_model": self.config["device"]["model"],
            "device_name": self.config["device"]["name"],
            "user_agent": "",
            "device_higher_category": self.config["device"]["category_h"],
            "device_lower_category": self.config["device"]["category_l"]
        }
        data = self._request("GET", self.config["endpoints"]["create_session"], params=params)
        return data
    
    def _login_with_password(self, email, password, temp_session):
        """Login with email and password"""
        headers = {
            "X-Gaia-Authorization": f"extra {temp_session['gaia_token']}",
            "X-Session-Token": temp_session['session_token']
        }
        payload = {
            "mail_address": email,
            "password": password,
            "app_id": int(self.config["device"]["app_id"]),
            "device_code": int(self.config["device"]["device_code"])
        }
        data = self._request("POST", self.config["endpoints"]["credentials_login"], headers=headers, payload=payload)
        return data
    
    def _get_profile_list(self, login_data):
        """Get profile list"""
        params = {
            "with_profiles": "true",
            "app_id": self.config["device"]["app_id"],
            "device_code": self.config["device"]["device_code"]
        }
        headers = {
            "X-User-Id": str(login_data["id"]),
            "Authorization": f"Bearer {login_data['access_token']}",
            "X-Session-Token": login_data['session_token']
        }
        data = self._request("GET", self.config["endpoints"]["user_profile"], params=params, headers=headers)
        return data
    
    def _switch_profile(self, profile_id, login_data, pin=""):
        """Switch to selected profile"""
        headers = {
            "X-Gaia-Authorization": f"extra {login_data['gaia_token']}",
            "X-Session-Token": login_data['session_token']
        }
        payload = {
            "pin": pin,
            "profile_id": profile_id
        }
        data = self._request("PUT", self.config["endpoints"]["auth_profile"], headers=headers, payload=payload)
        return data
    
    def _get_user_info(self):
        """Get user information"""
        params = {
            "app_version": self.config["device"]["app_version"],
            "system_version": self.config["device"]["os_version"],
            "device_code": self.config["device"]["device_code"],
            "manufacturer": self.config["device"]["manufacturer"],
            "is_mobile": "true",
            "os_version": self.config["device"]["os_version"],
            "os_build_id": self.config["device"]["os_build_id"],
            "device_manufacturer": self.config["device"]["manufacturer"],
            "device_model": self.config["device"]["model"],
            "device_name": self.config["device"]["name"],
            "user_agent": "",
            "device_higher_category": self.config["device"]["category_h"],
            "device_lower_category": self.config["device"]["category_l"]
        }
        headers = {
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
            "X-Session-Token": self.account_tokens["session"]["sessionToken"]
        }
        
        data = self._request("GET", self.config["endpoints"]["user_info"], params=params, headers=headers)
        
        return {
            "account_id": data.get("account_id", "unknown"),
            "profile_id": data.get("profile_id", "unknown"),
        }
    
    def _check_token(self):
        """Check if token is valid"""
        headers = {
            "X-Token-Id": str(self.account_tokens['token']['token_id']),
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
        }
        data = self._request("POST", self.config["endpoints"]["token_check"], headers=headers)
        return data
    
    def _refresh_token(self):
        """Refresh access token"""
        headers = {
            "X-Token-Id": str(self.account_tokens['token']['token_id']),
            "Authorization": f"Bearer {self.account_tokens['token']['accessToken']}",
        }
        payload = {
            "refresh_token": self.account_tokens["token"]["refreshToken"],
            "app_id": int(self.config["device"]["app_id"]),
            "device_code": int(self.config["device"]["device_code"])
        }
        data = self._request("POST", self.config["endpoints"]["token_refresh"], headers=headers, payload=payload)
        return data
    
    def _request(self, method, endpoint, params=None, headers=None, payload=None):
        """Make HTTP request with error handling"""
        _headers = self.session.headers.copy()
        if headers: 
            _headers.update(headers)
        
        req = Request(method, endpoint, headers=_headers, params=params, json=payload)
        prepped = self.session.prepare_request(req)

        try:
            res = self.session.send(prepped)
            res.raise_for_status()
            
            try:
                return res.json()
            except json.JSONDecodeError:
                return res.text
                
        except Exception as e:
            ignore_keys = ["token_check", "cookies_token"]
            if any(self.config["endpoints"].get(key, "") in endpoint for key in ignore_keys):
                raise e
            else:
                raise self.log.exit(f"API Request failed: {e}")