import base64
import json
import os.path
import re
import sys
import time
import uuid
from datetime import datetime, timedelta
from hashlib import md5
import glob
from typing import Dict, List, Optional, Union, Any, Tuple


import click
import httpx
import isodate
import requests
import xmltodict
from langcodes import Language

from fuckdl.objects import TextTrack, Title, Tracks, VideoTrack
from fuckdl.objects.tracks import AudioTrack, MenuTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.utils import is_close_match, short_hash, try_get
from fuckdl.utils.widevine.device import LocalDevice
from fuckdl.objects import VideoTrack, AudioTrack

# Import Path for file handling
from pathlib import Path

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class HBOMax(BaseService):
    """
    Service code for HBOMAX's streaming service (https://hbomax.com).

    \b
    Authorization: Cookies
    Security: UHD@L1 FHD@L1 HD@L3

    ---------------------------Updated by: @AnotherBigUserHere----------------------------------

    + Solved Subtitles handling, thanks to @kamiloprody_46016 by the solution
    that i used, but all the credits goes to he, that helps me to develop an solution
    with my developer experience

    + Adjusted the structure of the script with my style

    + Complements this solution with the txt of tracks.py that solved some things necessary to 
    make it work
    
    + Fixed 404 subtitles downloading

    + @thefallenrat77 thanks for the HDR False Fix, put in this script
 
    V3

    -------------------------------------------------------------------------------------------
    """

    ALIASES = ["HBOMAX", "hbomax"]
    TITLE_RE = r"^(?:https?://(?:www\.|play\.)?hbomax\.com/)?(?P<type>[^/]+)/(?P<id>[^/]+)"

    VIDEO_CODEC_MAP = {
        "H264": ["avc1"],
        "H265": ["hvc1", "dvh1", "hev1"]
    }

    AUDIO_CODEC_MAP = {
        "AAC": "mp4a",
        "AC3": "ac-3",
        "EC3": "ec-3"
    }

    @staticmethod
    @click.command(name="HBOMax", short_help="https://hbomax.com")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return HBOMax(ctx, **kwargs)

    def __init__(self, ctx, title):
        super().__init__(ctx)
        self.title = self.parse_title(ctx, title)
        
        cdm = ctx.obj.cdm
        self.playready = (hasattr(cdm, '__class__') and 'PlayReady' in cdm.__class__.__name__) or \
                         (hasattr(cdm, 'device') and hasattr(cdm.device, 'type') and 
                          cdm.device.type == LocalDevice.Types.PLAYREADY) or \
                         hasattr(cdm, "certificate_chain")
        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.range = ctx.parent.params["range_"]
        self.alang = ctx.parent.params["alang"]
        self.quality = ctx.parent.params["quality"] or 1080
        self.no_subs = any(ctx.parent.params[k] for k in (
            "no_subs", "audio_only", "chapters_only", "keys", "list_"
        ))
        
        # Adjust codec for higher quality if needed
        if self.range != 'SDR' or self.quality > 1080:
            self.log.info(" + Setting VideoCodec to H265 to enable 2160p video track")
            self.vcodec = "H265"
        
        self._configure_session()

    def _configure_session(self) -> None:
        """Configure session headers and authentication."""
        try:
            token = self.session.cookies.get_dict()["st"]
            device_id = json.loads(self.session.cookies.get_dict()["session"])
        except (KeyError, json.JSONDecodeError) as e:
            self.log.exit(f"Failed to parse session cookies: {e}")
            return

        # Set base headers based on DRM type
        base_headers = self._get_base_headers(device_id)
        self.session.headers.update(base_headers)

        # Get and set authentication token
        auth_token = self._get_device_token()
        if auth_token:
            self.session.headers.update({"x-wbd-session-state": auth_token})

    def _get_base_headers(self, device_id: Dict) -> Dict:
        """Get base headers based on DRM type."""
        trace_id = f"00-{uuid.uuid4().hex[:32]}-{uuid.uuid4().hex[:16]}-01"
        
        if self.playready:
            return {
                'User-Agent': 'BEAM-Android/5.0.0 (motorola/moto g(6) play)',
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json',
                'x-disco-client': 'ANDROID:9:beam:5.0.0',
                'x-disco-params': 'realm=bolt,bid=beam,features=ar,rr',
                'x-device-info': f'BEAM-Android/5.0.0 (motorola/moto g(6) play; ANDROID/9; {device_id})',
                'traceparent': trace_id,
                'tracestate': f'wbd=session:{device_id}',
                'Origin': 'https://play.hbomax.com',
                'Referer': 'https://play.hbomax.com/',
            }
        else:
            return {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0',
                'Accept': 'application/json, text/plain, */*',
                'Content-Type': 'application/json',
                'x-disco-client': 'WEB:NT 10.0:beam:0.0.0',
                'x-disco-params': 'realm=bolt,bid=beam,features=ar',
                'x-device-info': f'beam/0.0.0 (desktop/desktop; Windows/NT 10.0; {device_id})',
                'traceparent': trace_id,
                'tracestate': f'wbd=session:{device_id}',
                'Origin': 'https://play.hbomax.com',
                'Referer': 'https://play.hbomax.com/',
            }

    def _get_device_token(self) -> Optional[str]:
        """Get device token from bootstrap endpoint."""
        try:
            response = self.session.post(
                'https://default.any-any.prd.api.hbomax.com/session-context/headwaiter/v1/bootstrap',
                timeout=10
            )
            response.raise_for_status()
            return response.headers.get('x-wbd-session-state')
        except (requests.RequestException, KeyError) as e:
            self.log.error(f"Failed to get device token: {e}")
            return None

    def get_titles(self) -> List[Title]:
        """Fetch and parse titles based on content type."""
        content_type = self.title.get('type', '')
        external_id = self.title.get('id', '')
        
        if not content_type or not external_id:
            self.log.exit("Missing content type or ID in title data")
            return []
        
        # Map content types to API endpoints
        content_type_map = {
            "sport": ("video/watch-sport", "generic-sportvideo-edit-blueprint-page"),
            "event": ("video/watch-event", "generic-eventvideo-edit-blueprint-page"),
            "movie": ("movie", "generic-movie-blueprint-page"),
            "standalone": ("standalone", "generic-standalone-blueprint-page"),
            "show": ("show", "generic-show-blueprint-page"),
            "mini-series": ("mini-series", "generic-miniseries-blueprint-page"),
            "topical": ("topical", "generic-topical-show-blueprint-page")
        }
        
        # Safe mapping lookup
        mapped = content_type_map.get(content_type)
        if mapped:
            content_type_s, content_type_j = mapped
        else:
            # Default fallback
            content_type_s = content_type
            content_type_j = f"generic-{content_type}-blueprint-page"
        
        try:
            # Fetch content data
            url = f"https://default.any-any.prd.api.hbomax.com/cms/routes/{content_type_s}/{external_id}?include=default"
            self.log.debug(f"Fetching content from: {url}")
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            response_json = response.json()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Network error fetching content: {e}")
            
            # Fallback: try direct video endpoint
            try:
                fallback_url = f"https://default.any-any.prd.api.hbomax.com/content/videos/{external_id}"
                response = self.session.get(fallback_url, timeout=10)
                response.raise_for_status()
                response_json = response.json()
                content_type_s = "video"
                content_type_j = "generic-video-blueprint-page"
            except:
                self.log.exit(f"Failed to fetch content data: {e}")
                return []
        except json.JSONDecodeError as e:
            self.log.exit(f"Invalid JSON response: {e}")
            return []
    
        # Parse content title with safe method
        content_title = self._parse_content_title_safe(response_json, content_type_j, external_id, content_type)
        if not content_title:
            self.log.warning("Could not parse content title, using ID as fallback")
            content_title = f"Title-{external_id[:8]}"
    
        # Handle different content types
        try:
            if content_type in ["sport", "event"]:
                return self._handle_single_video(external_id, content_title)
            elif content_type in ["movie", "standalone"]:
                return self._handle_movie(external_id, content_title)
            elif content_type in ["show", "mini-series", "topical"]:
                return self._handle_series(content_type, external_id, content_title, response_json)
            else:
                # Try to determine type from response
                if "data" in response_json:
                    data = response_json["data"]
                    if isinstance(data, dict):
                        attrs = data.get("attributes", {})
                        if attrs.get("videoType") == "EPISODE":
                            return self._handle_series("show", external_id, content_title, response_json)
                        else:
                            return self._handle_movie(external_id, content_title)
                
                self.log.warning(f"Unsupported content type: {content_type}, trying as movie")
                return self._handle_movie(external_id, content_title)
        except Exception as e:
            self.log.error(f"Error handling content type {content_type}: {e}")
            return self._handle_movie(external_id, content_title)  # Final fallback

    def _parse_content_title_safe(self, response_json: Dict, content_type_j: str, external_id: str, content_type: str) -> Optional[str]:
        """Safe version of content title parsing with better error handling."""
        try:
            # First try: direct title from data
            if "data" in response_json and isinstance(response_json["data"], dict):
                data_attrs = response_json["data"].get("attributes", {})
                if data_attrs.get("title"):
                    return data_attrs["title"]
                if data_attrs.get("name"):
                    return data_attrs["name"]
                if data_attrs.get("originalName"):
                    return data_attrs["originalName"]
            
            # Second try: search in included
            included = response_json.get("included", [])
            if not isinstance(included, list):
                included = []
            
            # Safe type for regex
            safe_type = str(content_type) if content_type else ""
            clean_type = re.sub(r"-", "", safe_type) if safe_type else ""
            
            # Look for matching alias
            for item in included:
                if not isinstance(item, dict):
                    continue
                    
                attrs = item.get("attributes", {})
                if not isinstance(attrs, dict):
                    continue
                
                # Check alias with safe string formatting
                alias = attrs.get("alias")
                if alias and isinstance(alias, str) and clean_type:
                    try:
                        expected = content_type_j % clean_type
                        if alias == expected:
                            return attrs.get("title") or attrs.get("name")
                    except:
                        pass
                
                # Check alternateId
                if attrs.get("alternateId") == external_id:
                    return attrs.get("originalName") or attrs.get("title") or attrs.get("name")
            
            # Third try: any item with title
            for item in included:
                if not isinstance(item, dict):
                    continue
                attrs = item.get("attributes", {})
                if isinstance(attrs, dict):
                    if attrs.get("title"):
                        return attrs["title"]
                    if attrs.get("name"):
                        return attrs["name"]
            
            return None
            
        except Exception as e:
            self.log.debug(f"Safe title parsing failed: {e}")
            return None

    def _parse_content_title(self, response_json: Dict, content_type_j: str, external_id: str) -> Optional[str]:
        """Parse content title from response."""
        try:
            # Try to get title from blueprint page
            for item in response_json.get("included", []):
                attributes = item.get("attributes", {})
                
                # Safe string formatting - handle potential None or wrong type
                if attributes.get("alias"):
                    try:
                        # Get the base content type safely
                        base_type = self.title.get('type', '')
                        if base_type:
                            # Remove hyphens safely
                            clean_type = re.sub(r"-", "", str(base_type))
                            expected_alias = content_type_j % clean_type
                            
                            if attributes["alias"] == expected_alias:
                                return attributes.get("title")
                    except (TypeError, ValueError) as e:
                        self.log.debug(f"Alias comparison failed: {e}")
                        continue
            
            # Fallback to alternateId match
            for item in response_json.get("included", []):
                attributes = item.get("attributes", {})
                if attributes.get("alternateId") == external_id and attributes.get("originalName"):
                    return attributes.get("originalName")
                    
            # Additional fallback: try to find any title
            for item in response_json.get("included", []):
                attributes = item.get("attributes", {})
                if attributes.get("title") and attributes.get("name"):
                    return attributes.get("title")
                    
        except Exception as e:
            self.log.debug(f"Error parsing content title: {e}")
        
        # Final fallback: try to get from the main data
        try:
            if "data" in response_json and "attributes" in response_json["data"]:
                return response_json["data"]["attributes"].get("title") or \
                       response_json["data"]["attributes"].get("name")
        except:
            pass
            
        return None

    def _handle_single_video(self, external_id: str, content_title: str) -> List[Title]:
        """Handle sport or event videos."""
        try:
            response = self.session.get(
                f"https://default.any-any.prd.api.hbomax.com/content/videos/{external_id}",
                timeout=10
            )
            response.raise_for_status()
            metadata = response.json().get('data', {})
            
            release_date = metadata.get("attributes", {}).get("airDate") or metadata.get("attributes", {}).get("firstAvailableDate")
            year = datetime.strptime(release_date, '%Y-%m-%dT%H:%M:%SZ').year if release_date else datetime.now().year
            
            return [Title(
                id_=external_id,
                type_=Title.Types.MOVIE,
                name=content_title,
                year=year,
                source=self.ALIASES[0],
                service_data=metadata,
            )]
        except (requests.RequestException, KeyError, ValueError) as e:
            self.log.exit(f"Failed to fetch video data: {e}")
            return []

    def _handle_movie(self, external_id: str, content_title: str) -> List[Title]:
        """Handle movie content."""
        try:
            response = self.session.get(
                f"https://default.any-any.prd.api.hbomax.com/content/videos/{external_id}/activeVideoForShow?&include=edit",
                timeout=10
            )
            response.raise_for_status()
            metadata = response.json().get('data', {})
            
            release_date = metadata.get("attributes", {}).get("airDate") or metadata.get("attributes", {}).get("firstAvailableDate")
            year = datetime.strptime(release_date, '%Y-%m-%dT%H:%M:%SZ').year if release_date else datetime.now().year
            
            return [Title(
                id_=external_id,
                type_=Title.Types.MOVIE,
                name=content_title,
                year=year,
                source=self.ALIASES[0],
                service_data=metadata,
            )]
        except (requests.RequestException, KeyError, ValueError) as e:
            self.log.exit(f"Failed to fetch movie data: {e}")
            return []

    def _handle_series(self, content_type: str, external_id: str, content_title: str, response_json: Dict) -> List[Title]:
        """Handle series content (show, mini-series, topical)."""
        try:
            # Parse season data
            season_data = self._parse_season_data(response_json, content_type)
            if not season_data:
                self.log.exit("Failed to parse season data")
                return []
            
            # Get season parameters
            season_parameters = self._get_season_parameters(season_data)
            if not season_parameters:
                self.log.exit("No seasons found")
                return []
            
            # Fetch episodes for each season
            episodes = self._fetch_episodes(external_id, season_parameters)
            if not episodes:
                self.log.exit("No episodes found")
                return []
            
            # Create Title objects
            return self._create_episode_titles(episodes, content_title, season_parameters)
            
        except Exception as e:
            self.log.exit(f"Failed to process series: {e}")
            return []

    def _parse_season_data(self, response_json: Dict, content_type: str) -> Optional[Dict]:
        """Parse season data from response."""
        alias_map = {
            "mini-series": "generic-miniseries-page-rail-episodes",
            "topical": "generic-topical-show-page-rail-episodes",
            "show": "-show-page-rail-episodes-tabbed-content"
        }
        
        alias = alias_map.get(content_type, f"-{content_type}-page-rail-episodes-tabbed-content")
        
        for item in response_json.get("included", []):
            attributes = item.get("attributes", {})
            if alias in str(attributes).lower():
                return attributes.get("component", {}).get("filters", [{}])[0]
        
        return None

    def _get_season_parameters(self, season_data: Dict) -> List[tuple]:
        """Extract season parameters from season data."""
        parameters = []
        for season in season_data.get("options", []):
            try:
                value = int(season.get("value", 0))
                parameter = season.get("parameter", "")
                if value and parameter:
                    parameters.append((value, parameter))
            except (ValueError, TypeError):
                continue
        return parameters

    def _fetch_episodes(self, external_id: str, season_parameters: List[tuple]) -> List[Dict]:
        """Fetch episodes for all seasons."""
        episodes = []
        for value, parameter in season_parameters:
            try:
                response = self.session.get(
                    f"https://default.any-any.prd.api.hbomax.com/cms/collections/generic-show-page-rail-episodes-tabbed-content"
                    f"?include=default&pf[show.id]={external_id}&{parameter}",
                    timeout=10
                )
                response.raise_for_status()
                data = response.json()
                
                season_episodes = [
                    dt for dt in data.get("included", [])
                    if dt.get("attributes", {}).get("videoType") == "EPISODE"
                    and int(dt["attributes"].get("seasonNumber", 0)) == value
                ]
                episodes.extend(sorted(season_episodes, key=lambda x: x["attributes"]["episodeNumber"]))
            except (requests.RequestException, KeyError, ValueError) as e:
                self.log.error(f"Failed to fetch episodes for season {value}: {e}")
                continue
        
        return episodes

    def _create_episode_titles(self, episodes: List[Dict], content_title: str, season_parameters: List[tuple]) -> List[Title]:
        """Create Title objects for episodes."""
        if not episodes:
            return []
        
        # Create season map
        season_map = {int(item[1].split("=")[-1]): item[0] for item in season_parameters}
        
        # Parse year from first episode
        release_date = episodes[0]["attributes"].get("airDate") or episodes[0]["attributes"].get("firstAvailableDate")
        year = datetime.strptime(release_date, '%Y-%m-%dT%H:%M:%SZ').year if release_date else datetime.now().year
        
        titles = []
        for episode in episodes:
            attributes = episode["attributes"]
            titles.append(
                Title(
                    id_=episode['id'],
                    type_=Title.Types.TV,
                    name=content_title,
                    year=year,
                    season=season_map.get(attributes.get('seasonNumber')),
                    episode=attributes['episodeNumber'],
                    episode_name=attributes.get('name', ''),
                    source=self.ALIASES[0],
                    service_data=episode
                )
            )
        
        return titles

    def get_tracks(self, title: Title) -> Tracks:
        """Fetch and parse tracks for the title."""
        try:
            edit_id = title.service_data['relationships']['edit']['data']['id']
        except (KeyError, TypeError) as e:
            self.log.exit(f"Failed to get edit ID: {e}")
            return Tracks()
    
        # Get playback info
        playback_data = self._get_playback_info(edit_id)
        if not playback_data:
            return Tracks()
    
        # Parse video info
        video_info = self._parse_video_info(playback_data)
        if not video_info:
            return Tracks()
    
        # Set original language
        title.original_lang = Language.get(video_info['defaultAudioSelection']['language'])
    
        # Get manifest
        manifest_url, manifest_data = self._get_manifest(playback_data)
        if not manifest_url or not manifest_data:
            return Tracks()
    
        # Parse tracks from manifest
        tracks = Tracks.from_mpd(
            url=manifest_url,
            data=manifest_data,
            source=self.ALIASES[0]
        )
        
        # Clear partial subs and add proper subtitles
        tracks.subtitles.clear()
        subtitles = self.get_subtitles(manifest_url, manifest_data)
        
        subs = []
        for subtitle in subtitles:
            first_url = subtitle["url"][0] if isinstance(subtitle["url"], list) else subtitle["url"]
            track_id = md5(f"{first_url}_{subtitle['language']}_{subtitle['name']}".encode()).hexdigest()
            
            is_external = not isinstance(subtitle["url"], list)
            mpd_rep_id = None if is_external else subtitle.get("representation_id")
            
            subs.append(
                TextTrack(
                    id_=track_id,
                    source=self.ALIASES[0],
                    url=subtitle["url"],
                    codec=subtitle['format'],
                    language=subtitle["language"],
                    forced=subtitle['name'] == 'Forced',
                    sdh=subtitle['name'] == 'SDH',
                    mpd_representation_id=mpd_rep_id,
                    external=is_external,
                )
            )
        
        tracks.add(subs)
    
        # Filter tracks based on user preferences
        tracks = self._filter_tracks(tracks)
    
        # Process track metadata
        self._process_track_metadata(tracks)
    
        # Store video info for chapters
        title.service_data['info'] = video_info

        from pathlib import Path
        import re
        
        for track in tracks:
            # Set source to trigger the HBOMax downloader in tracks.py
            track.source = "HBOMax"
            # Store manifest URL for N_m3u8DL-RE to use
            track.manifest_url = manifest_url
            
            # Video / Audio track processing
            if isinstance(track, (VideoTrack, AudioTrack)):
                # Try to get representation ID from the track's extra data
                rep_id = None
                if hasattr(track, 'extra') and track.extra and len(track.extra) >= 2:
                    representation, adaptation = track.extra[:2]
                    if hasattr(representation, 'attrib'):
                        rep_id = representation.attrib.get("id")
                
                # Store MPD representation ID for selective downloading
                track.mpd_representation_id = rep_id
                
                # Create a safe filename for N_m3u8DL-RE output
                safe_title = re.sub(r'[\\/*?:"<>|]', "_", title.name)
                lang = str(track.language) if track.language else "und"
                safe_lang = lang.replace("-", "_")
                if rep_id:
                    track.re_name = f"{safe_title}_{rep_id}_{safe_lang}"
                else:
                    track.re_name = f"{safe_title}_{safe_lang}"
                
                # Log for debugging
                self.log.debug(f"Track: {track.__class__.__name__} | ID: {rep_id} | Lang: {lang}")
            
            # TextTrack processing
            elif isinstance(track, TextTrack):
                # Try to get representation ID from the track if available
                rep_id = None
                if hasattr(track, 'extra') and track.extra and len(track.extra) >= 2:
                    representation, adaptation = track.extra[:2]
                    if hasattr(representation, 'attrib'):
                        rep_id = representation.attrib.get("id")
                
                # Store MPD representation ID if found
                track.mpd_representation_id = rep_id
                
                # Set external flag based on whether this is segmented or single VTT
                if not hasattr(track, 'external'):
                    is_external = isinstance(track.url, str) and track.url.endswith('.vtt')
                    track.external = is_external
                
                # Create safe filename for subtitle track
                safe_title = re.sub(r'[\\/*?:"<>|]', "_", title.name)
                track.re_name = f"{safe_title}_{track.language}"
                
                self.log.debug(f"Subtitle: {track.language} | Forced: {track.forced} | SDH: {track.sdh} | External: {getattr(track, 'external', False)}")
    
        return tracks

    def _get_playback_info(self, edit_id: str) -> Optional[Dict]:
        """Get playback information from API."""
        try:
            response = self.session.post(
                url=self.config['endpoints']['playbackInfo'],
                json=self._build_playback_request(edit_id),
                timeout=15
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            self.log.error(f"Failed to get playback info: {e}")
            return None

    def _build_playback_request(self, edit_id: str) -> Dict:
        """Build playback request payload."""
        session_id = str(uuid.uuid4())
        return {
            "appBundle": "com.wbd.stream",
            "applicationSessionId": session_id,
            "capabilities": {
                "codecs": {
                    "audio": {
                        "decoders": [
                            {
                                "codec": "eac3",
                                "profiles": ["lc", "he", "hev2", "xhe", "atmos"]
                            },
                            {
                                "codec": "ac3",
                                "profiles": []
                            }
                        ]
                    },
                    "video": {
                        "decoders": [
                            {
                                "codec": "h264",
                                "levelConstraints": {
                                    "framerate": {"max": 960, "min": 0},
                                    "height": {"max": 2200, "min": 64},
                                    "width": {"max": 3900, "min": 64}
                                },
                                "maxLevel": "6.2",
                                "profiles": ["baseline", "main", "high"]
                            },
                            {
                                "codec": "h265",
                                "levelConstraints": {
                                    "framerate": {"max": 960, "min": 0},
                                    "height": {"max": 2200, "min": 144},
                                    "width": {"max": 3900, "min": 144}
                                },
                                "maxLevel": "6.2",
                                "profiles": ["main", "main10"]
                            }
                        ],
                        "hdrFormats": [
                            'dolbyvision8', 'dolbyvision5', 'dolbyvision',
                            'hdr10plus', 'hdr10', 'hlg'
                        ]
                    }
                },
                "contentProtection": {
                    "contentDecryptionModules": [{
                        "drmKeySystem": 'playready',
                        "maxSecurityLevel": 'sl3000'
                    }] if self.playready else []
                },
                "devicePlatform": {
                    "network": {
                        "capabilities": {
                            "protocols": {"http": {"byteRangeRequests": True}}
                        },
                        "lastKnownStatus": {"networkTransportType": "wifi"}
                    },
                    "videoSink": {
                        "capabilities": {
                            "colorGamuts": ["standard"],
                            "hdrFormats": []
                        },
                        "lastKnownStatus": {"height": 2200, "width": 3900}
                    }
                },
                "manifests": {"formats": {"dash": {}}}
            },
            "consumptionType": "streaming",
            "deviceInfo": {
                "browser": {
                    "name": "Discovery Player Android androidTV",
                    "version": "1.8.1-canary.102"
                },
                "deviceId": "",
                "deviceType": "androidtv" if self.playready else "web",
                "make": "NVIDIA" if self.playready else "Generic",
                "model": "SHIELD Android TV" if self.playready else "Desktop",
                "os": {
                    "name": "ANDROID" if self.playready else "WINDOWS",
                    "version": "10" if self.playready else "10.0"
                },
                "platform": "android" if self.playready else "web",
                "player": {
                    "mediaEngine": {
                        "name": "exoPlayer",
                        "version": "1.2.1"
                    },
                    "playerView": {"height": 2160, "width": 3840},
                    "sdk": {
                        "name": "Discovery Player Android androidTV",
                        "version": "1.8.1-canary.102"
                    }
                }
            },
            "editId": edit_id,
            "firstPlay": True,
            "gdpr": False,
            "playbackSessionId": session_id,
            "userPreferences": {"uiLanguage": "en"}
        }

    def _parse_video_info(self, playback_data: Dict) -> Optional[Dict]:
        """Extract video info from playback data."""
        try:
            # Get license URLs
            drm_schemes = playback_data.get("drm", {}).get("schemes", {})
            self.pr_license_url = drm_schemes.get("playready", {}).get("licenseUrl")
            self.wv_license_url = drm_schemes.get("widevine", {}).get("licenseUrl")
            
            # Get video info
            for video in playback_data.get('videos', []):
                if video.get('type') == 'main':
                    return video
            
            self.log.error("No main video found in playback data")
            return None
        except (KeyError, IndexError) as e:
            self.log.error(f"Failed to parse video info: {e}")
            return None

    def _get_manifest(self, playback_data: Dict) -> tuple[Optional[str], Optional[str]]:
        """Get manifest URL and data."""
        try:
            fallback_url = playback_data["fallback"]["manifest"]["url"]
            manifest_url = fallback_url.replace('_fallback', '')
            
            self.log.debug(f"Manifest URL: {manifest_url}")

            manifest_headers = {
                'User-Agent': 'BEAM-Android/5.0.0 (motorola/moto g(6) play)',
                'Accept': 'application/dash+xml, application/xml, text/xml, */*',
                'Origin': 'https://play.hbomax.com',
                'Referer': 'https://play.hbomax.com/',
            }
            
            response = requests.get(
                manifest_url, 
                headers=manifest_headers,
                timeout=15,
                verify=False 
            )
            response.raise_for_status()
            
            return manifest_url, response.text
        except (KeyError, requests.RequestException) as e:
            self.log.error(f"Failed to get manifest: {e}")
            if hasattr(e, 'response') and e.response is not None:
                self.log.error(f"Response status: {e.response.status_code}")
                self.log.error(f"Response body: {e.response.text[:500]}")
            return None, None

    def get_subtitles(self, manifest_url, manifest_data):
        """Extract subtitle information from MPD manifest."""
        import xmltodict
        from hashlib import md5
        
        xml = xmltodict.parse(manifest_data)
        periods = xml["MPD"]["Period"]
        if isinstance(periods, dict):
            periods = [periods]
        
        base_url = manifest_url.rsplit('/', 1)[0]
        
        subtitles = []
        
        for period in periods:
            adaptation_sets = period.get("AdaptationSet", [])
            if isinstance(adaptation_sets, dict):
                adaptation_sets = [adaptation_sets]
    
            for adaptation_set in adaptation_sets:
                if adaptation_set.get("@contentType") != "text":
                    continue
    
                rep = adaptation_set["Representation"]
                if isinstance(rep, list):
                    rep = rep[0]
    
                rep_id = rep.get("@id", "")
                
                segment_template = rep.get("SegmentTemplate", {})
                media_template = segment_template.get("@media", "")
                
                start_number = int(segment_template.get("@startNumber", "1"))
                
                timeline = segment_template.get("SegmentTimeline", {})
                if isinstance(timeline, dict):
                    s_elem = timeline.get("S", [])
                    if isinstance(s_elem, list):
                        total_segments = len(s_elem)
                    else:
                        total_segments = 1
                else:
                    total_segments = 1
                
                language = adaptation_set.get("@lang", "und")
                
                role = adaptation_set.get("Role", {})
                if isinstance(role, list):
                    role = role[0] if role else {}
                role_value = role.get("@value", "") if role else ""
                
                label = adaptation_set.get("Label", "")
                if isinstance(label, dict):
                    label = label.get("#text", "")
                
                name = "Full"
                if "forced" in role_value or "forced" in label.lower():
                    name = "Forced"
                elif "sdh" in label.lower() or "caption" in role_value:
                    name = "SDH"
                
                urls = []
                for seg_num in range(start_number, start_number + total_segments):
                    # Reemplazar $Number$ en el template
                    segment_url = media_template.replace("$Number$", str(seg_num))
                    full_url = f"{base_url}/{segment_url}"
                    urls.append(full_url)
                
                subtitles.append({
                    "url": urls,
                    "format": "vtt",
                    "language": language,
                    "name": name,
                    "representation_id": rep_id,
                })
    
        seen = set()
        unique_subs = []
        for sub in subtitles:
            key = f"{sub['language']}_{sub['name']}_{sub['representation_id']}"
            if key not in seen:
                unique_subs.append(sub)
                seen.add(key)
        
        return unique_subs

    def _process_subtitle_adaptation(self, adaptation_set: Dict, manifest_url: str) -> Optional[TextTrack]:
        """Process a subtitle adaptation set into a TextTrack."""
        try:
            rep = adaptation_set["Representation"]
            if isinstance(rep, list):
                rep = rep[0]
            
            sub_types = {
                "sdh": "_sdh.vtt",
                "caption": "_cc.vtt",
                "subtitle": "_sub.vtt",
                "forced-subtitle": "_forced.vtt",
            }
            
            is_sdh = "sdh" in adaptation_set.get("Label", "").lower()
            language = adaptation_set["@lang"]
            sub_type = "sdh" if is_sdh else adaptation_set.get("Role", {}).get("@value", "subtitle")
            
            if sub_type not in sub_types:
                return None
            
            suffix = sub_types[sub_type]
            base_url = manifest_url.rsplit('/', 1)[0]
            sub_path = rep.get("SegmentTemplate", {}).get("@media", "")
            
            if not sub_path:
                return None
            
            path = "/".join(sub_path.split("/", 2)[:2])
            url = f"{base_url}/{path}/{language}{suffix}"
            
            # Test if URL is accessible
            try:
                self.session.head(url, timeout=5).raise_for_status()
            except requests.RequestException:
                # Fallback to segmented URL
                url = f"{base_url}/{sub_path}".replace("$Number$", "1")
            
            return TextTrack(
                id_=md5(url.encode()).hexdigest(),
                source=self.ALIASES[0],
                url=url,
                codec="vtt",
                language=language,
                forced=sub_type == "forced-subtitle",
                sdh=is_sdh
            )
        except Exception as e:
            self.log.debug(f"Failed to process subtitle adaptation: {e}")
            return None

    def _filter_tracks(self, tracks: Tracks) -> Tracks:
        """Filter tracks based on user preferences."""
        if self.vcodec:
            tracks.videos = [
                x for x in tracks.videos 
                if any(x.codec.startswith(c) for c in self.VIDEO_CODEC_MAP[self.vcodec])
            ]
        
        if self.acodec:
            codec_prefix = self.AUDIO_CODEC_MAP[self.acodec]
            tracks.audios = [
                x for x in tracks.audios 
                if x.codec and x.codec.startswith(codec_prefix)
            ]
        
        return tracks

    def _process_track_metadata(self, tracks: Tracks) -> None:
        """Process and enhance track metadata."""
        for track in tracks:
            track.needs_proxy = False
            
            if isinstance(track, VideoTrack):
                self._process_video_track(track)
            elif isinstance(track, AudioTrack):
                self._process_audio_track(track)
            elif isinstance(track, TextTrack) and not track.codec:
                track.codec = "webvtt"

    def _process_video_track(self, track: VideoTrack) -> None:
        """Process video track metadata using precise Profile 8 detection."""
        if track.extra:
            extra = track.extra[0]
            codec = extra.get("codecs", "")

            # Retrieve supplemental metadata
            supp_keys = [
                "{urn:scte:dash:scte214-extensions}supplementalCodecs",
                "scte214:supplementalCodecs",
                "supplementalCodecs",
                "segmentProfiles"
            ]
            supplemental_data = " ".join([str(extra.get(k, "")) for k in supp_keys])

            # Detect Dolby Vision
            is_dv = codec.startswith(("dvh1", "dvhe")) or "dvh1" in supplemental_data or "dvhe" in supplemental_data
            track.dv = is_dv

            # Precise detection for Dolby Vision Profile 8 (Hybrid HDR10)
            is_profile_8 = "dvhe.08" in codec or "dvh1.08" in codec or \
                           "dvhe.08" in supplemental_data or "dvh1.08" in supplemental_data

            if is_dv and is_profile_8:
                track.dvhdr = True
                track.hdr10 = True

            # Fallback if the track is already natively flagged by the base system
            if track.dv and getattr(track, 'hdr10', False):
                track.dvhdr = True

    def _process_audio_track(self, track: AudioTrack) -> None:
        """Process audio track metadata."""
        if len(track.extra) > 1:
            role = track.extra[1].find("Role")
            if role is not None and role.get("value") in ["description", "alternative", "alternate"]:
                track.descriptive = True

    def get_chapters(self, title: Title) -> List[MenuTrack]:
        """Extract chapters from video info."""
        chapters = []
        video_info = title.service_data.get('info', {})
        
        if 'annotations' in video_info and video_info['annotations']:
            chapters.append(MenuTrack(
                number=1,
                title='Chapter 1',
                timecode='00:00:00.0000'
            ))
            
            chapters.append(MenuTrack(
                number=2,
                title='Credits',
                timecode=self._convert_timecode(video_info['annotations'][0]['start'])
            ))
            
            chapters.append(MenuTrack(
                number=3,
                title='Chapter 2',
                timecode=self._convert_timecode(video_info['annotations'][0]['end'])
            ))
        
        return chapters

    def certificate(self, challenge: bytes, **_) -> Optional[bytes]:
        """Handle DRM certificate request."""
        if self.playready:
            return None  # PlayReady handles certificate differently
        return self.license(challenge)

    def license(self, challenge: bytes, **_) -> Optional[bytes]:
        """Handle license request."""
        try:
            url = self.pr_license_url if self.playready else self.wv_license_url
            if not url:
                self.log.error("No license URL available")
                return None
            
            response = self.session.post(
                url=url,
                data=challenge,
                timeout=30
            )
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            self.log.error(f"License request failed: {e}")
            return None

    @staticmethod
    def _convert_timecode(time_seconds: float) -> str:
        """Convert seconds to timecode format."""
        hours = int(time_seconds // 3600)
        minutes = int((time_seconds % 3600) // 60)
        seconds = int(time_seconds % 60)
        milliseconds = int((time_seconds - int(time_seconds)) * 10000)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:04d}"

    @staticmethod
    def _remove_duplicate_subtitles(subtitles: List[TextTrack]) -> List[TextTrack]:
        """Remove duplicate subtitle tracks."""
        seen = set()
        unique = []
        
        for sub in subtitles:
            key = (sub.language, sub.forced, sub.sdh)
            if key not in seen:
                unique.append(sub)
                seen.add(key)
        
        return unique

    @staticmethod
    def remove_dupe(items):
        seen = set()
        unique_items = []
        
        for item in items:
            first_url = item['url'][0] if isinstance(item['url'], list) else item['url']
            key = f"{item['language']}_{item['name']}_{first_url}"
            
            if key not in seen:
                unique_items.append(item)
                seen.add(key)
        
        return unique_items