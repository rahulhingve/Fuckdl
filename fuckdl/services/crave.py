import base64
import json
import os
import re
import time
import urllib.parse
import random
import click

from fuckdl.objects import TextTrack, Title, Tracks
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.widevine.device import LocalDevice

class Crave(BaseService):
    """
    Service code for Bell Media's Crave streaming service (https://crave.ca).

    Recoded by @AnotherBigUserHere 
    Original script by @droctavius3902

    + Querry Updated to new and old titles
    + Licence Updated
    + Updated Endpoints, of login, graph and header
   
    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
 
    \b
    Authorization: Cookies

    Security: UHD@-- HD@SL3000
    Region: Canada (CA)
    """

    ALIASES = ["CRAV", "crave"]
    GEOFENCE = ["ca"]
    TITLE_RE = [
        r"^(?:https?://(?:www\.)?crave\.ca(?:/[a-z]{2})?/(?:series|movie|special)/)?(?P<id>[a-zA-Z0-9-]+)-(?P<axis_id>\d+)$",
        r"^(?:https?://(?:www\.)?crave\.ca(?:/[a-z]{2})?/(?:movies|tv-shows|special)/)?(?P<id>[a-zA-Z0-9-]+)$"
    ]

    VIDEO_CODEC_MAP = {
        "H264": ["avc1"],
        "H265": ["hvc1", "dvh1"]
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "AC3": "ac-3",
        "EC3": "ec-3"
    }

    # Constants for better maintainability
    MAX_RETRY_ATTEMPTS = 15
    RETRY_BASE_WAIT = 3
    RETRY_MAX_WAIT = 30
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0"

    @staticmethod
    @click.command(name="Crave", short_help="https://crave.ca")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return Crave(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        super().__init__(ctx)
        
        # Parse the title to extract the axis ID if it's in the URL
        self.axis_id_from_url = None
        if title:
            match = re.search(r'-(\d+)$', title)
            if match:
                self.axis_id_from_url = match.group(1)
                self.log.info(f"Extracted Axis ID from URL: {self.axis_id_from_url}")
        
        self.parse_title(ctx, title)
        self.movie = movie
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self._is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                             (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                              self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        self.profile = ctx.obj.profile

        self.ce_access_token = None
        self.refresh_token = None
        self.content_id = None
        self.package_id = None
        self.destination_id = None
        self.manifest_url = None
        
        self.configure()
                
    def get_titles(self):
        """Fetch title information from the Crave GraphQL API."""
        wrapper_token = self._generate_wrapper_token()
        
        title_response = self.session.post(
            url="https://rte-api.bellmedia.ca/graphql",
            headers=self._get_graphql_headers(wrapper_token),
            json={
                "query": """
                query GetShowpage($sessionContext: SessionContext!, $ids: [String!]!) {
                    medias(sessionContext: $sessionContext, ids: $ids) {
                        originalLanguage {
                            id
                            displayName
                        }
                        path
                        title
                        id
                        mediaType
                        shortDescription
                        description
                        productionYear
                        seasons {
                            id
                            title
                            seasonNumber
                        }
                        firstContent {
                            title
                            id
                            seasonNumber
                            episodeNumber
                            contentType
                            path
                        }
                    }
                }
                """,
                "variables": {
                    "ids": [self.title],
                    "sessionContext": {
                        "userMaturity": "ADULT",
                        "userLanguage": "EN"
                    }
                }
            }
        ).json()
        
        self._validate_title_response(title_response)
        
        title_information = title_response["data"]["medias"][0]
        
        if self.movie or title_information["mediaType"] == "MOVIE":
            return self._process_movie_title(title_information)
        else:
            return self._process_tv_title(title_information)
    
    def _process_movie_title(self, title_info):
        """Process movie title information."""
        first_content = title_info.get("firstContent", {})
        if not first_content:
            raise Exception("No playable content found for this movie")
        
        first_content["axisId"] = first_content["id"]
        
        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title_info["title"],
            year=title_info.get("productionYear"),
            original_lang=title_info.get("originalLanguage", {}).get("id"),
            source=self.ALIASES[0],
            service_data=first_content
        )
    
    def _process_tv_title(self, title_info):
        """Process TV series title information."""
        titles = []
        
        for season in title_info["seasons"]:
            season_id = self._extract_season_id(season["id"])
            self.log.debug(f"Using season ID: {season_id} (original: {season['id']})")

            wrapper_token = self._generate_wrapper_token()
            
            season_response = self.session.post(
                url="https://rte-api.bellmedia.ca/graphql",
                headers=self._get_graphql_headers(wrapper_token),
                json={
                    "query": """
                    query GetContentBySeasonId($sessionContext: SessionContext!, $id: String!, $contentFormat: ContentFormatRequest) {
                        contentsBySeasonId(
                            sessionContext: $sessionContext
                            id: $id
                            contentFormat: $contentFormat
                        ) {
                            id
                            title
                            episodeNumber
                            path
                            media {
                                id
                                title
                                path
                            }
                        }
                    }
                    """,
                    "variables": {
                        "id": season_id,
                        "sessionContext": {
                            "userMaturity": "ADULT",
                            "userLanguage": "EN"
                        },
                        "contentFormat": {
                            "format": "LONGFORM"
                        }
                    }
                }
            ).json()
            
            if not self._validate_season_response(season_response):
                self.log.warning(f"Unexpected response for season {season['seasonNumber']}")
                continue
                
            episodes = season_response["data"]["contentsBySeasonId"]
            
            if not episodes:
                self.log.warning(f"No episodes found for season {season['seasonNumber']}")
                continue
            
            # Process episodes for this season
            for episode in episodes:
                episode["axisId"] = episode["id"]
                episode["seasonNumber"] = season["seasonNumber"]
                episode["axisPlaybackLanguages"] = [{"language": "EN"}]
                self.log.debug(f"   - S{season['seasonNumber']}E{episode.get('episodeNumber')}: {episode.get('title')}")
            
            titles.extend(episodes)
        
        # Create Title objects for all episodes
        return [Title(
            id_=self.title,
            type_=Title.Types.TV,
            name=title_info["title"],
            year=title_info.get("productionYear"),
            season=episode.get("seasonNumber"),
            episode=episode.get("episodeNumber"),
            episode_name=episode.get("title"),
            original_lang=title_info.get("originalLanguage", {}).get("id"),
            source=self.ALIASES[0],
            service_data=episode
        ) for episode in titles]
            
    def get_tracks(self, title):
        """Fetch video, audio, and subtitle tracks for a title."""
        try:
            # Step 1: Get content information
            content_data = self._get_content_info(title.service_data['axisId'])
            
            # Step 2: Get manifest metadata
            meta_data = self._get_manifest_metadata(
                content_data["content_id"],
                content_data["package_id"],
                content_data["destination_id"]
            )
            
            # Step 3: Process manifest URL
            manifest_url = self._process_manifest_url(meta_data.get("playback", ""))
            self.log.info(f"Manifest URL: {manifest_url}")
            
            # Step 4: Fetch and parse MPD
            mpd_data = self._fetch_mpd_manifest(manifest_url)
            
        except Exception as e:
            raise Exception(f"Failed to fetch playback information: {e}")
        
        # Step 5: Parse tracks from MPD
        tracks = Tracks.from_mpd(
            data=mpd_data,
            url=manifest_url,
            source=self.ALIASES[0]
        )
        
        # Step 6: Filter tracks based on codec preferences
        tracks = self._filter_tracks_by_codec(tracks)
        
        # Step 7: Detect descriptive audio tracks
        self._detect_descriptive_audio(tracks)
        
        # Step 8: Detect forced subtitles
        self._detect_forced_subtitles(tracks)
        
        # Store manifest URL in track extras
        for track in tracks:
            track.extra = manifest_url
                
        return tracks
    
    def _get_content_info(self, axis_id):
        """Fetch content information including IDs and destination."""
        content_response = self.session.get(
            url=f"https://playback.rte-api.bellmedia.ca/contents/{axis_id}",
            headers={
                "Authorization": f"Bearer {self.ce_access_token}",
                "X-Client-Platform": "platform_jasper_web",
                "X-Playback-Language": "EN",
                "Accept-Language": "EN",
                "Origin": "https://www.crave.ca",
                "Referer": "https://www.crave.ca/"
            }
        ).json()
        
        content_id = content_response.get("contentId")
        content_package = content_response.get("contentPackage", {})
        package_id = content_package.get("id")
        destination_id = content_response.get("destinationId", 1880)
        
        if not content_id or not package_id:
            raise Exception("Missing content or package ID in API response")
        
        self.content_id = content_id
        self.package_id = package_id
        self.destination_id = destination_id
        
        self.log.info(f"Content ID: {content_id}, Package ID: {package_id}, Destination: {destination_id}")
        
        return {
            "content_id": content_id,
            "package_id": package_id,
            "destination_id": destination_id
        }
    
    def _get_manifest_metadata(self, content_id, package_id, destination_id):
        """Fetch manifest metadata with retry logic."""
        meta_url = self._build_metadata_url(content_id, package_id, destination_id)
        
        for attempt in range(1, self.MAX_RETRY_ATTEMPTS + 1):
            try:
                headers = self._get_metadata_headers()
                headers["Authorization"] = f"Bearer {self.ce_access_token}"
                
                response = self.session.get(meta_url, headers=headers, timeout=20)
                
                if response.status_code == 200:
                    self.log.info(f"Successfully fetched metadata on attempt {attempt}")
                    return response.json()
                
                elif response.status_code == 403:
                    error_msg = response.text
                    if "proxy" in error_msg.lower() or "940" in error_msg:
                        self.log.error("9c9media blocked request - detected as proxy/VPN")
                        raise Exception("9c9media API blocked request - detected as proxy/VPN")
                    else:
                        raise Exception(f"403 Forbidden: {error_msg[:200]}")
                
                elif response.status_code == 500:
                    wait_time = self._calculate_wait_time(attempt)
                    if attempt < self.MAX_RETRY_ATTEMPTS:
                        self.log.warning(f"Server error (attempt {attempt}/{self.MAX_RETRY_ATTEMPTS}), waiting {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Server errors persisted after {self.MAX_RETRY_ATTEMPTS} attempts")
                
                else:
                    self.log.error(f"Unexpected status {response.status_code}")
                    if attempt < self.MAX_RETRY_ATTEMPTS:
                        time.sleep(5)
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")
                        
            except Exception as e:
                if "500" in str(e) or attempt == self.MAX_RETRY_ATTEMPTS:
                    if attempt < self.MAX_RETRY_ATTEMPTS:
                        wait_time = self._calculate_wait_time(attempt)
                        self.log.warning(f"Network error (attempt {attempt}/{self.MAX_RETRY_ATTEMPTS}), waiting {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Failed after {self.MAX_RETRY_ATTEMPTS} attempts: {e}")
                else:
                    raise
        
        raise Exception(f"Could not get valid response after {self.MAX_RETRY_ATTEMPTS} attempts")
    
    def _fetch_mpd_manifest(self, manifest_url):
        """Fetch MPD manifest from URL."""
        try:
            response = self.session.get(
                manifest_url,
                headers={
                    "User-Agent": self.USER_AGENT,
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Referer": "https://www.crave.ca/",
                    "Origin": "https://www.crave.ca"
                }
            )
            response.raise_for_status()
            
            mpd_data = response.text
            
            if not mpd_data or not mpd_data.strip().startswith("<?xml"):
                raise Exception(f"Invalid MPD response. Content: {mpd_data[:200]}")
                
            return mpd_data
                
        except Exception as e:
            raise Exception(f"Failed to fetch MPD manifest: {e}")
    
    def _process_manifest_url(self, playback_url):
        """Process and validate the manifest URL."""
        if not playback_url:
            raise Exception("No playback URL in metadata response")
        
        # Replace widevine with playready and optimize URL
        manifest_url = playback_url.replace("widevine", "playready")
        manifest_url = re.sub(r'zbest', 'zultimate', manifest_url)
        
        return manifest_url
    
    def _filter_tracks_by_codec(self, tracks):
        """Filter tracks based on user's codec preferences."""
        if self.vcodec:
            tracks.videos = [
                x for x in tracks.videos 
                if (x.codec or "")[:4] in self.VIDEO_CODEC_MAP[self.vcodec]
            ]
        
        if self.acodec:
            tracks.audios = [
                x for x in tracks.audios 
                if (x.codec or "")[:4] == self.AUDIO_CODEC_MAP[self.acodec]
            ]
        
        return tracks
    
    def _detect_descriptive_audio(self, tracks):
        """Mark audio tracks as descriptive if they contain descriptions."""
        for track in tracks.audios:
            role = track.extra[1].find("Role")
            if role is not None and role.get("value") in ["description", "alternative", "alternate"]:
                track.descriptive = True
    
    def _detect_forced_subtitles(self, tracks):
        """Detect and mark forced subtitles."""
        for track in tracks.subtitles:
            adaptation_set = track.extra[1]
            roles = adaptation_set.findall("Role")
            
            for role in roles:
                role_value = role.get("value", "")
                scheme_id = role.get("schemeIdUri", "")
                
                if (role_value == "forced-subtitle" or 
                    (scheme_id == "urn:mpeg:dash:role:2011" and role_value == "forced-subtitle")):
                    track.forced = True
                    self.log.info(f"Detected forced subtitle: {track.language} - {role_value}")
                    break
    
    def get_chapters(self, title):
        """Get chapter information for a title."""
        return []

    def certificate(self, **_):
        """Get Widevine certificate (not required for Crave)."""
        return None

    def license(self, challenge, **_):
        """Request license for decryption."""
        return self.session.post(
            url=self.config["endpoints"]["license_pr"],
            data=challenge
        ).content
    
    def configure(self):
        """Configure service with tokens and title resolution."""
        self.cache_path = self.get_cache(f"tokens_{self.profile}.json")
        self.load_tokens()

        # Try to get tokens from cookies if not loaded from cache
        if not self.ce_access_token:
            self.ce_access_token = self.session.cookies.get('ce_access')
            self.refresh_token = self.session.cookies.get('ce_refresh')
            if self.ce_access_token:
                self.save_tokens()
        
        # Refresh token if expired
        if self.ce_access_token and self.is_expired(self.ce_access_token):
            self.log.info("Access token expired, refreshing...")
            self.refresh_tokens()
        
        # Determine title ID
        if self.axis_id_from_url:
            self.title = self.axis_id_from_url
            self.log.info(f"Using Axis ID from URL: {self.title}")
        else:
            self.log.info(f"Fetching Axis ID for: {self.title}")
            axis_id = self.get_axis_id(f"/tv-shows/{self.title}") or self.get_axis_id(f"/movies/{self.title}")
            
            if not axis_id:
                raise self.log.exit(f"Could not obtain Axis ID for '{self.title}', please verify the title")
            
            self.title = axis_id
            self.log.info(f"Obtained Axis ID: {self.title}")

    def load_tokens(self):
        """Load authentication tokens from cache file."""
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r") as f:
                    data = json.load(f)
                    self.ce_access_token = data.get("access_token")
                    self.refresh_token = data.get("refresh_token")
                    self.log.debug("Tokens loaded from cache")
            except Exception as e:
                self.log.warning(f"Failed to load tokens from cache: {e}")

    def save_tokens(self):
        """Save authentication tokens to cache file."""
        try:
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            with open(self.cache_path, "w") as f:
                json.dump({
                    "access_token": self.ce_access_token,
                    "refresh_token": self.refresh_token
                }, f, indent=4)
            self.log.debug("Tokens saved to cache")
        except Exception as e:
            self.log.warning(f"Failed to save tokens to cache: {e}")

    def is_expired(self, token):
        """Check if JWT token is expired."""
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return True
                
            payload = parts[1]
            payload += "=" * ((4 - len(payload) % 4) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(payload))
            exp = decoded.get("exp")
            
            if exp and time.time() > exp:
                return True
            return False
        except Exception:
            return True

    def refresh_tokens(self):
        """Refresh authentication tokens using refresh token."""
        if not self.refresh_token:
            self.log.error("No refresh token available for renewal")
            return False

        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'Basic Y3JhdmUtd2ViOmRlZmF1bHQ=',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://www.crave.ca',
            'referer': 'https://www.crave.ca/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }

        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
        }

        try:
            response = self.session.post(
                self.config["endpoints"]["refresh"], 
                headers=headers, 
                data=data
            )
            response.raise_for_status()
            res_json = response.json()
            
            self.ce_access_token = res_json.get("access_token")
            self.refresh_token = res_json.get("refresh_token")
            
            if self.ce_access_token:
                self.session.cookies.update({
                    "ce_access": self.ce_access_token, 
                    "ce_refresh": self.refresh_token
                })
            
            self.save_tokens()
            self.log.info("Access token refreshed successfully")
            return True
            
        except Exception as e:
            self.log.error(f"Failed to refresh token: {e}")
            return False
    
    def get_axis_id(self, path):
        """Get Axis ID from content path using GraphQL API."""
        wrapper_token = self._generate_wrapper_token()
        
        res = self.session.post(
            url="https://rte-api.bellmedia.ca/graphql",
            headers=self._get_graphql_headers(wrapper_token),
            json={
                "query": """
                query resolvePath($path: String!) {
                    resolvedPath(path: $path) {
                        lastSegment {
                            content {
                                id
                            }
                        }
                    }
                }
                """,
                "variables": {
                    "path": path
                }
            }
        ).json()
        
        if "errors" in res:
            if res.get("errors") and len(res["errors"]) > 0:
                error_code = res["errors"][0].get("extensions", {}).get("code")
                if error_code == "NOT_FOUND":
                    return None
            self.log.error(f"GraphQL error for path {path}: {res.get('errors')}")
            return None
        
        try:
            return res["data"]["resolvedPath"]["lastSegment"]["content"]["id"]
        except (KeyError, TypeError) as e:
            self.log.debug(f"Could not extract Axis ID from response: {e}")
            return None
    
    # Helper methods
    def _generate_wrapper_token(self):
        """Generate wrapper token for GraphQL requests."""
        wrapper_payload = {
            "platform": "platform_web",
            "accessToken": self.ce_access_token
        }
        return base64.b64encode(json.dumps(wrapper_payload).encode()).decode()
    
    def _get_graphql_headers(self, wrapper_token):
        """Get headers for GraphQL API requests."""
        return {
            "Authorization": f"Bearer {wrapper_token}",
            "x-client-platform": "platform_web",
            "Content-Type": "text/plain;charset=UTF-8",
            "Origin": "https://www.crave.ca",
            "Referer": "https://www.crave.ca/"
        }
    
    def _get_metadata_headers(self):
        """Get headers for metadata requests."""
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://www.crave.ca",
            "Connection": "keep-alive",
            "Referer": "https://www.crave.ca/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "TE": "trailers"
        }
    
    def _build_metadata_url(self, content_id, package_id, destination_id):
        """Build metadata URL based on video codec preference."""
        base_url = (
            f"https://stream.video.9c9media.com/meta/content/{content_id}/"
            f"contentpackage/{package_id}/destination/{destination_id}/platform/1"
            "?format=mpd&filter=ff&hd=true&mcv=true&mca=true&mta=true&stt=true"
        )
        
        if self.vcodec == "H265":
            return f"{base_url}&uhd=true"
        else:
            return f"{base_url}&uhd=false"
    
    def _calculate_wait_time(self, attempt):
        """Calculate exponential backoff wait time."""
        return min(self.RETRY_BASE_WAIT + (attempt * 2), self.RETRY_MAX_WAIT) + random.uniform(0, 2)
    
    def _extract_season_id(self, season_id):
        """Extract season ID from various formats."""
        if "/" in season_id:
            season_id = season_id.split("/")[-1]
        if "-" in season_id:
            season_id = season_id.split("-")[-1]
        return season_id
    
    def _validate_title_response(self, response):
        """Validate title response from GraphQL API."""
        if "data" not in response or "medias" not in response["data"]:
            raise Exception(f"Invalid title response: {response}")
        
        if not response["data"]["medias"]:
            raise Exception(f"No media found for ID: {self.title}")
    
    def _validate_season_response(self, response):
        """Validate season response from GraphQL API."""
        return ("data" in response and 
                "contentsBySeasonId" in response["data"] and 
                response["data"]["contentsBySeasonId"] is not None)