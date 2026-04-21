import json
import uuid
import re
from urllib.parse import urlparse
from typing import Optional
from datetime import datetime, timedelta
import click
import requests
from langcodes import Language
from fuckdl.objects.titles import Title
from fuckdl.objects.tracks import Tracks, AudioTrack, TextTrack, VideoTrack
from fuckdl.services.BaseService import BaseService


class VIX(BaseService):
    """
    Service code for VIX streaming service (https://vix.com)

    \b
    Authorization: Cookies, Credentials, or Anonymous
    Security:
      Widevine:
        L3: 1080p

    Original Script by @Dex 

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["VIX", "vix", "vixcom"]
    GEOFENCE = []
    TITLE_RE = r"(?:https?://)?(?:www\.)?vix\.com/(?:detail/)?(?:series|video)-(\d+)"

    # Language mapping for consistent output
    LANG_MAP = {
        "es": "es-419",
        "pt": "pt-PT",
        "en": "en-US",
        "es-419": "es-419",
        "es-es": "es-419",
        "pt-br": "pt-PT",
        "pt-pt": "pt-PT",
    }

    @staticmethod
    @click.command(name="VIX", short_help="https://www.vix.com")
    @click.argument("title", type=str, required=True)
    @click.option("--force-anonymous", is_flag=True, help="Force anonymous authentication")
    @click.pass_context
    def cli(ctx, **kwargs):
        return VIX(ctx, **kwargs)

    def __init__(self, ctx, title: str, force_anonymous: bool = False):
        super().__init__(ctx)
        self.title_input = title
        self.install_id = str(uuid.uuid4())
        self.cdm = ctx.obj.cdm
        self.force_anonymous = force_anonymous

        # Authentication state
        self._access_token = None
        self._user_token = None
        self._refresh_token = None
        self._token_expiry = None
        self.auth_method = None
        
        # Service data
        self.license_urls = {}
        self.license_headers = {}
        self.original_lang = Language.get("es-419")
        
        self.configure()

    def configure(self):
        """Configure session with proper headers."""
        self.session.headers.update({
            "User-Agent": self.config.get("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.vix.com",
            "Referer": "https://www.vix.com/",
            "x-vix-app-version": "4.40.3",
            "x-vix-device-type": "web",
            "x-vix-platform": "web",
            "x-vix-platform-version": "14",
            "x-vix-install-id": self.install_id,
        })
        
        self.authenticate()

    def authenticate(self):
        """Unified authentication method with fallbacks."""
        
        # Force anonymous if flag is set
        if self.force_anonymous:
            if self._authenticate_anonymous():
                self.auth_method = "anonymous_forced"
                self.log.info(" + Using forced anonymous authentication")
                return
            raise RuntimeError("VIX: Forced anonymous authentication failed")
        
        # Priority 1: Cookies (best quality) - with refresh capability
        if self._authenticate_with_cookies():
            self.auth_method = "cookie"
            self.log.info(" + Authenticated with cookies")
            return
            
        # Priority 2: Credentials from config
        if self.credentials and self.credentials.username and self.credentials.password:
            if self._authenticate_with_credentials():
                self.auth_method = "credentials"
                self.log.info(" + Authenticated with credentials")
                return
                
        # Priority 3: Anonymous (fallback)
        if self._authenticate_anonymous():
            self.auth_method = "anonymous"
            self.log.warning(" + Using anonymous authentication - video quality may be limited")
            return
            
        raise RuntimeError(
            "VIX: No valid authentication method available.\n"
            "Please provide either:\n"
            "  â€¢ Cookies with vix_user_token\n"
            "  â€¢ Credentials (email/password) in config\n"
            "  â€¢ Or use --force-anonymous flag"
        )

    def _authenticate_with_cookies(self) -> bool:
        """Authenticate using cookies with refresh support."""
        cookies = self.session.cookies.get_dict()
        vix_user_token_json = cookies.get("vix_user_token")
        
        if not vix_user_token_json:
            return False
            
        try:
            token_data = json.loads(vix_user_token_json)
            self._access_token = token_data.get("accessToken")
            self._user_token = token_data.get("userToken", "")
            self._refresh_token = token_data.get("refreshToken")
            
            # Check token expiry from cookie if available
            expires_at = token_data.get("expiresAt")
            if expires_at:
                try:
                    expiry_time = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    if datetime.now(expiry_time.tzinfo) >= expiry_time:
                        self.log.info(" + Cookie token expired, attempting refresh...")
                        if self._refresh_token():
                            self.log.info(" + Token refreshed successfully")
                            # Update cookie with new token data
                            self._update_cookie_token()
                            return True
                        else:
                            self.log.warning(" + Token refresh failed, cookie may be invalid")
                            return False
                except Exception as e:
                    self.log.debug(f"Error parsing expiry: {e}")
            
            if self._access_token:
                self.session.headers.update({
                    "Authorization": f"Bearer {self._access_token}",
                    "x-vix-user-token": self._user_token,
                })
                return True
        except json.JSONDecodeError as e:
            self.log.error(f"Error parsing vix_user_token JSON: {e}")
            
        return False

    def _update_cookie_token(self):
        """Update the cookie with refreshed token data."""
        token_data = {
            "accessToken": self._access_token,
            "userToken": self._user_token,
            "refreshToken": self._refresh_token
        }
        if self._token_expiry:
            token_data["expiresAt"] = self._token_expiry.isoformat()
        
        # Update the cookie in the session
        self.session.cookies.set("vix_user_token", json.dumps(token_data), domain=".vix.com")

    def _authenticate_with_credentials(self) -> bool:
        """Authenticate using email/password."""
        try:
            anon_at, anon_ut = self._anonymous_login()
            if not anon_at:
                return False
                
            headers = {
                "Authorization": f"Bearer {anon_at}",
                "x-vix-user-token": anon_ut,
                "x-vix-install-id": self.install_id,
                "Content-Type": "application/json",
            }
            
            json_data = {
                "email": self.credentials.username,
                "password": self.credentials.password,
            }
            
            resp = self.session.post(
                self.config["endpoints"]["login"],
                headers=headers,
                json=json_data,
            )
            resp.raise_for_status()
            
            data = resp.json()
            self._access_token = data.get("accessToken")
            self._user_token = data.get("userToken")
            self._refresh_token = data.get("refreshToken")
            
            if self._access_token:
                self.session.headers.update({
                    "Authorization": f"Bearer {self._access_token}",
                    "x-vix-user-token": self._user_token,
                })
                return True
                
        except Exception as e:
            self.log.debug(f"Credential authentication failed: {e}")
            
        return False

    def _authenticate_anonymous(self) -> bool:
        """Authenticate anonymously."""
        try:
            at, ut = self._anonymous_login()
            if at and ut:
                self._access_token = at
                self._user_token = ut
                self._refresh_token = None
                self.session.headers.update({
                    "Authorization": f"Bearer {self._access_token}",
                    "x-vix-user-token": self._user_token,
                })
                return True
        except Exception as e:
            self.log.debug(f"Anonymous authentication failed: {e}")
            
        return False

    def _anonymous_login(self):
        """Get anonymous access token."""
        try:
            json_data = {"installationId": self.install_id}
            
            # Use minimal headers for anonymous request
            headers = {
                "User-Agent": self.session.headers.get("User-Agent", "Mozilla/5.0"),
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            
            resp = self.session.post(
                self.config["endpoints"]["anon-user"],
                json=json_data,
                headers=headers,
                timeout=30
            )
            resp.raise_for_status()
            
            data = resp.json()
            access_token = data.get("accessToken")
            user_token = data.get("userToken")
            
            if access_token and user_token:
                self.log.debug("Anonymous login successful")
                return access_token, user_token
            else:
                self.log.error(f"Anonymous login missing tokens: {data.keys()}")
                return None, None
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"Anonymous login request failed: {e}")
            return None, None
        except Exception as e:
            self.log.error(f"Anonymous login failed: {e}")
            return None, None

    def _refresh_token(self) -> bool:
        """Refresh expired tokens."""
        if not self._refresh_token:
            return False
            
        try:
            json_data = {"refreshToken": self._refresh_token}
            
            resp = self.session.post(
                self.config["endpoints"]["refresh"],
                headers={
                    "Authorization": f"Bearer {self._access_token}",
                    "x-vix-user-token": self._user_token,
                    "Content-Type": "application/json",
                },
                json=json_data,
                timeout=30
            )
            
            if resp.status_code == 401:
                self.log.debug("Refresh token expired, need new login")
                return False
                
            resp.raise_for_status()
            
            data = resp.json()
            self._access_token = data.get("accessToken")
            self._user_token = data.get("userToken")
            self._refresh_token = data.get("refreshToken")
            
            # Set expiry from response if available
            expires_in = data.get("expiresIn")
            if expires_in:
                self._token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            self.session.headers.update({
                "Authorization": f"Bearer {self._access_token}",
                "x-vix-user-token": self._user_token,
            })
            return True
            
        except Exception as e:
            self.log.debug(f"Token refresh failed: {e}")
            return False

    def _check_auth_error(self, response: requests.Response) -> bool:
        """Check if response indicates authentication error."""
        if response.status_code in [401, 403]:
            try:
                error_data = response.json()
                error_msg = str(error_data).lower()
                if any(keyword in error_msg for keyword in 
                       ['unauthorized', 'invalid token', 'expired', 'authentication']):
                    return True
            except:
                if any(keyword in response.text.lower() for keyword in 
                       ['unauthorized', 'invalid token', 'expired', 'authentication']):
                    return True
        return False

    def _safe_json(self, resp: requests.Response):
        """Safely parse JSON response."""
        try:
            return resp.json()
        except Exception as e:
            text_snippet = (resp.text or "")[:400].replace("\n", " ")
            raise RuntimeError(
                f"VIX: error decoding JSON (status={resp.status_code}): {e}. "
                f"Response snippet: {text_snippet}"
            )

    def _get_video_token(self, video_id: str) -> str:
        """Get playback token for a video."""
        token_url = self.config["endpoints"]["token"]
        
        resp = self.session.get(
            token_url,
            params={"videoId": video_id},
            headers={"x-video-type": "VOD"}
        )
        resp.raise_for_status()
        
        data = self._safe_json(resp)
        token = data.get("token")
        
        if not token:
            raise RuntimeError(f"VIX: missing token for video {video_id}")
            
        return token

    def _extract_numeric_id(self, vid: str) -> str:
        """Extract numeric ID from full ID string."""
        return vid.split(":")[-1]

    def _normalize_language(self, lang_code: str) -> Language:
        """Convert language code to proper Language object."""
        if not lang_code:
            return Language.get("es-419")
        
        # Apply mapping
        lang_code = self.LANG_MAP.get(lang_code.lower(), lang_code.lower())
        
        try:
            return Language.get(lang_code)
        except:
            return Language.get("es-419")

    def get_titles(self) -> list[Title]:
        """Get titles from URL."""
        titles: list[Title] = []
        
        if not self._access_token:
            raise RuntimeError("VIX: No access token available. Authentication failed.")
        
        parsed = urlparse(self.title_input)
        path = parsed.path or ""
        path = path.rstrip("/")
        
        if "/video/" in path or "/video-" in path:
            return self._get_movie_titles(path)
        elif "/detail/" in path and "series-" in path:
            return self._get_series_titles(path)
        else:
            raise RuntimeError(f"VIX: Unrecognized URL format: {self.title_input}")

    def _get_movie_titles(self, path: str) -> list[Title]:
        """Get movie title information."""
        try:
            video_id = path.split("/video-")[-1].split("/")[0]
            if not video_id:
                raise ValueError()
        except Exception:
            raise RuntimeError(f"Could not extract video ID from URL: {self.title_input}")
        
        try:
            token = self._get_video_token(video_id)
        except Exception as e:
            raise RuntimeError(f"VIX: error getting movie token: {e}")
        
        try:
            play_url = self.config["endpoints"]["play"].format(play_id=video_id)
            r = self.session.post(play_url, data={"token": token})
            r.raise_for_status()
            info = self._safe_json(r)
            content = info.get("content") or info
            content["token"] = token
            
            content_id = content.get("id") or video_id
            title_name = content.get("title") or content.get("name") or f"Video_{video_id}"
            
            # Set original language
            lang_code = content.get("language", "es")
            self.original_lang = self._normalize_language(lang_code)
            
            # Get year from copyrightYear or release date
            year = content.get("copyrightYear")
            if not year:
                # Try to extract year from dateReleased
                date_released = content.get("dateReleased")
                if date_released:
                    try:
                        year = int(date_released[:4])
                    except (ValueError, TypeError):
                        pass
            
            titles = [Title(
                id_=content_id,
                name=title_name,
                type_=Title.Types.MOVIE,
                source=self.ALIASES[0],
                year=year,  # Added year support
                service_data=content,
            )]
            
            return titles
            
        except Exception as e:
            raise RuntimeError(f"VIX: error getting playback metadata for movie: {e}")

    def _get_series_titles(self, path: str) -> list[Title]:
        """Get series and episode titles using GraphQL."""
        try:
            show_id = path.split("/series-")[-1].split("/")[0]
            if not show_id:
                raise ValueError()
        except Exception:
            raise RuntimeError(f"Could not extract series ID from URL: {self.title_input}")
        
        series_id = f"series:mcp:{show_id}"
        graphql_url = self.config["endpoints"]["graphql"]
        
        series_vars = {
            "videoByIdId": series_id,
            "navigationSection": {
                "urlPath": f"/detail/series-{show_id}",
                "pageItemId": ""
            },
            "seasonPagination": {}
        }
        
        data = {
            "operationName": "VideoQuery",
            "variables": series_vars,
            "query": """
                query VideoQuery($videoByIdId: ID!, $navigationSection: TrackingNavigationSectionInput!, $seasonPagination: PaginationParams!) { 
                    videoById(id: $videoByIdId) { 
                        __typename 
                        ...videoContentFragment 
                    } 
                }
                fragment imageAssetFragment on ImageAsset { 
                    link 
                    imageRole 
                }
                fragment pageInfoFragment on PageInfo { 
                    hasPreviousPage 
                    hasNextPage 
                    startCursor 
                    endCursor 
                }
                fragment seasonContentFragment on Season { 
                    id 
                    title 
                    yearReleased 
                    episodesConnection(pagination: $seasonPagination) { 
                        totalCount 
                        edges { 
                            node { 
                                id 
                            } 
                            cursor 
                        } 
                        pageInfo { 
                            __typename 
                            ...pageInfoFragment 
                        } 
                    } 
                }
                fragment videoTypeSeriesFragment on VideoTypeSeriesData { 
                    seasonsConnection(pagination: $seasonPagination) { 
                        totalCount 
                        edges { 
                            node { 
                                __typename 
                                ...seasonContentFragment 
                            } 
                            cursor 
                        } 
                        pageInfo { 
                            __typename 
                            ...pageInfoFragment 
                        } 
                    } 
                    seasonsCount 
                    episodesCount 
                    seriesSubType 
                    hasReverseOrder 
                }
                fragment videoContentFragment on VideoContent { 
                    id 
                    title 
                    copyrightYear
                    dateReleased
                    genresV2 { 
                        name 
                    } 
                    videoTypeData { 
                        __typename 
                        ... on VideoTypeSeriesData { 
                            __typename 
                            ...videoTypeSeriesFragment 
                        } 
                    } 
                }
            """
        }
        
        try:
            resp = self.session.post(graphql_url, json=data)
            
            if self._check_auth_error(resp):
                if self._refresh_token():
                    resp = self.session.post(graphql_url, json=data)
            
            resp.raise_for_status()
            series_json = resp.json()
            
            series_data = series_json["data"]["videoById"]
            series_title = series_data["title"]
            series_year = series_data.get("copyrightYear") or series_data.get("dateReleased", "")[:4]
            
            seasons = series_data["videoTypeData"]["seasonsConnection"]["edges"]
            
            titles = []
            season_number = 0
            
            for s_edge in seasons:
                season_number += 1
                season_node = s_edge["node"]
                season_id = season_node["id"]
                season_year = season_node.get("yearReleased")
                
                season_query = {
                    "operationName": "SeasonById",
                    "variables": {
                        "seriesId": series_id,
                        "seasonId": season_id,
                        "navigationSection": {
                            "urlPath": f"/detail/series-{show_id}",
                            "pageItemId": ""
                        },
                        "seasonPagination": {}
                    },
                    "query": """query SeasonById($seriesId: ID!, $seasonId: ID!, $navigationSection: TrackingNavigationSectionInput!, $seasonPagination: PaginationParams!) { 
                        seasonById(seriesId: $seriesId, seasonId: $seasonId) { 
                            title
                            yearReleased
                            episodesConnection(pagination: $seasonPagination) { 
                                edges { 
                                    node { 
                                        id 
                                        title 
                                        language
                                        copyrightYear
                                        dateReleased
                                        videoTypeData { 
                                            ... on VideoTypeEpisodeData { 
                                                episodeNumber 
                                                season { title } 
                                                series { title } 
                                            } 
                                        } 
                                    } 
                                } 
                            } 
                        } 
                    }"""
                }
                
                season_resp = self.session.post(graphql_url, json=season_query)
                
                if self._check_auth_error(season_resp):
                    if self._refresh_token():
                        season_resp = self.session.post(graphql_url, json=season_query)
                
                season_resp.raise_for_status()
                season_data = season_resp.json()
                season_info = season_data["data"]["seasonById"]
                episodes_full = season_info["episodesConnection"]["edges"]
                
                for ep_edge in episodes_full:
                    ep_node = ep_edge["node"]
                    
                    raw_ep_id = ep_node["id"]
                    ep_id = self._extract_numeric_id(raw_ep_id)
                    
                    ep_title = ep_node.get("title", f"Episode {ep_id}")
                    ep_num = ep_node["videoTypeData"]["episodeNumber"]
                    series_title = ep_node["videoTypeData"]["series"]["title"]
                    
                    # Get episode year
                    ep_year = ep_node.get("copyrightYear")
                    if not ep_year:
                        ep_year = ep_node.get("dateReleased", "")[:4]
                    if not ep_year or ep_year == "":
                        ep_year = season_year or series_year
                    
                    ep_token = self._get_video_token(ep_id)
                    
                    titles.append(
                        Title(
                            id_=ep_id,
                            name=series_title,
                            type_=Title.Types.TV,
                            season=season_number,
                            episode=ep_num,
                            episode_name=ep_title,
                            year=ep_year,  # Added year support
                            source=self.ALIASES[0],
                            service_data={
                                **ep_node,
                                "series_title": series_title,
                                "episode_number": ep_num,
                                "token": ep_token,
                            },
                        )
                    )
            
            return titles
            
        except Exception as e:
            if self._check_auth_error(getattr(e, 'response', None)):
                raise RuntimeError(
                    "VIX: Authentication failed - session appears to be expired or invalid.\n"
                    "Please update your cookies, credentials, or try again."
                )
            raise RuntimeError(f"VIX: GraphQL request failed: {e}")

    def get_tracks(self, title) -> Tracks:
        """Get tracks for the title."""
        content_id = title.id
        if not content_id:
            raise RuntimeError("VIX title has no content ID")
        
        play_url = self.config["endpoints"]["play"].format(play_id=content_id)
        token = title.service_data.get("token")
        
        if not token:
            raise RuntimeError("VIX: missing token for /play/ request")
        
        r = self.session.post(
            play_url,
            data=f"token={token}",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "Mozilla/5.0 (Linux; Android 10; LuraPlayer)",
                "Origin": "https://vix.com",
                "Referer": "https://vix.com/",
            }
        )
        
        if r.status_code == 404:
            raise RuntimeError("VIX: Content is not available in your region")
        
        r.raise_for_status()
        
        info = r.json()
        content = info.get("content") or info
        media = content.get("media", [])
        
        mpd_url = next((m["url"] for m in media if m.get("type") == "application/dash+xml"), None)
        
        if not mpd_url:
            raise RuntimeError("VIX: No MPD streams available")
        
        self.log.info(f"Manifest URL: {mpd_url}")
        
        # Extract query parameters from MPD URL to add to segment requests
        from urllib.parse import urlparse, parse_qs
        parsed_mpd = urlparse(mpd_url)
        mpd_query = parsed_mpd.query
        mpd_base = f"{parsed_mpd.scheme}://{parsed_mpd.netloc}{parsed_mpd.path}"
        
        # Find license URL
        for m in media:
            lurl = m.get("licenseUrl")
            if lurl and not self.license_urls.get("widevine"):
                self.license_urls["widevine"] = lurl
        
        for p in content.get("protections", []):
            if not self.license_urls.get("widevine") and p.get("type") == "widevine":
                self.license_urls["widevine"] = p.get("licenseUrl")
        
        sdrm = content.get("stream", {}).get("drm", {})
        if not self.license_urls.get("widevine") and "widevine" in sdrm:
            self.license_urls["widevine"] = sdrm["widevine"].get("licenseUrl")
        
        # Create a hook to add query parameters to all segment requests
        if mpd_query:
            def add_query_params(response, *args, **kwargs):
                """Add MPD query parameters to segment requests."""
                req = response.request
                url = req.url
                
                # Only add params to segment/init requests (not to the MPD itself)
                if (url.endswith((".m4s", ".mp4", "-init.mp4")) and "?" not in url):
                    req.url = url + "?" + mpd_query
                
                return response
            
            # Add hook to session if not already present
            if not hasattr(self.session, '_vix_hook_added'):
                if not isinstance(self.session.hooks.get("response"), list):
                    self.session.hooks["response"] = []
                self.session.hooks["response"].append(add_query_params)
                self.session._vix_hook_added = True
        
        # Parse MPD and create tracks
        tracks = Tracks.from_mpd(
            url=mpd_url,
            session=self.session,
            source=self.ALIASES[0],
        )
        
        # Fix language codes - convert strings to Language objects
        for track in tracks.audios + tracks.subtitles:
            if isinstance(track.language, str):
                lang_str = track.language
                lang_str = self.LANG_MAP.get(lang_str.lower(), lang_str)
                track.language = Language.get(lang_str)
            elif track.language is None:
                track.language = self.original_lang
        
        # Set original language flag on audio tracks
        for track in tracks.audios:
            if str(track.language) == str(self.original_lang):
                track.is_original_lang = True
        
        return tracks

    def license(self, challenge: bytes, title=None, track=None, session_id=None, **_):
        """Get Widevine license."""
        lic_url = self.license_urls.get("widevine")
        if not lic_url:
            raise RuntimeError("VIX: No Widevine license URL available.")
        
        headers = {"Content-Type": "application/octet-stream"}
        
        if "widevine" in self.license_headers:
            headers.update(self.license_headers["widevine"])
        
        r = self.session.post(lic_url, data=challenge, headers=headers)
        r.raise_for_status()
        return r.content