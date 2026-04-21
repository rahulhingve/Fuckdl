import base64
import itertools
import json
import os
import re
import requests
from enum import Enum
from urllib.parse import unquote
from click import Context
import click
import m3u8
from datetime import datetime
from fuckdl.objects import AudioTrack, TextTrack, Title, Tracks, VideoTrack, MenuTrack
from fuckdl.services.BaseService import BaseService
from fuckdl.vendor.pymp4.parser import Box
import plistlib
from fuckdl.utils.widevine.device import LocalDevice


class iTunes(BaseService):
    """
    Service code for Apple's VOD streaming service (https://tv.apple.com).

    \b
    Authorization: Cookies
    Security: UHD@L1 FHD@L1 HD@L1 SD@L3

    Used port of @.P R E D A T O R 
    Updated by @AnotherBigUserHere

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["iT", "itunes"]
    TITLE_RE = r"^(?:https?://tv\.apple\.com(?:/[a-z]{2})?/(?:movie|show|episode)/[a-z0-9-]+/)?(?P<id>umc\.cmc\.[a-z0-9]+)"

    VIDEO_CODEC_MAP = {
        "H264": ["avc"],
        "H265": ["hvc", "hev", "dvh"]
    }
    AUDIO_CODEC_MAP = {
        "AAC": ["HE", "stereo"],
        "AC3": ["ac3"],
        "EC3": ["ec3", "atmos"]
    }

    # Storefront mapping from Unshackle
    STOREFRONT_MAP = {
        "DZ": "143563", "AO": "143564", "AI": "143538", "AG": "143540", "AR": "143505", "AM": "143524", "AU": "143460",
        "AT": "143445", "AZ": "143568", "BH": "143559", "BD": "143490", "BB": "143541", "BY": "143565", "BE": "143446",
        "BZ": "143555", "BM": "143542", "BO": "143556", "BW": "143525", "BR": "143503", "VG": "143543", "BN": "143560",
        "BG": "143526", "CA": "143455", "KY": "143544", "CL": "143483", "CN": "143465", "CO": "143501", "CR": "143495",
        "CI": "143527", "HR": "143494", "CY": "143557", "CZ": "143489", "DK": "143458", "DM": "143545", "DO": "143508",
        "EC": "143509", "EG": "143516", "SV": "143506", "EE": "143518", "FI": "143447", "FR": "143442", "DE": "143443",
        "GH": "143573", "GR": "143448", "GD": "143546", "GT": "143504", "GY": "143553", "HN": "143510", "HK": "143463",
        "HU": "143482", "IS": "143558", "IN": "143467", "ID": "143476", "IE": "143449", "IL": "143491", "IT": "143450",
        "JM": "143511", "JP": "143462", "JO": "143528", "KZ": "143517", "KE": "143529", "KR": "143466", "KW": "143493",
        "LV": "143519", "LB": "143497", "LI": "143522", "LT": "143520", "LU": "143451", "MO": "143515", "MK": "143530",
        "MG": "143531", "MY": "143473", "MV": "143488", "ML": "143532", "MT": "143521", "MU": "143533", "MX": "143468",
        "MD": "143523", "MS": "143547", "NP": "143484", "NL": "143452", "NZ": "143461", "NI": "143512", "NE": "143534",
        "NG": "143561", "NO": "143457", "OM": "143562", "PK": "143477", "PA": "143485", "PY": "143513", "PE": "143507",
        "PH": "143474", "PL": "143478", "PT": "143453", "QA": "143498", "RO": "143487", "RU": "143469", "SA": "143479",
        "SN": "143535", "RS": "143500", "SG": "143464", "SK": "143496", "SI": "143499", "ZA": "143472", "ES": "143454",
        "LK": "143486", "KN": "143548", "LC": "143549", "VC": "143550", "SR": "143554", "SE": "143456", "CH": "143459",
        "TW": "143470", "TZ": "143572", "TH": "143475", "BS": "143539", "TT": "143551", "TN": "143536", "TR": "143480",
        "TC": "143552", "UG": "143537", "GB": "143444", "UA": "143492", "AE": "143481", "UY": "143514", "US": "143441",
        "UZ": "143566", "VE": "143502", "VN": "143471", "YE": "143571"
    }

    @staticmethod
    @click.command(name="iTunes", short_help="https://itunes.apple.com")
    @click.argument("title", type=str, required=False)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a Movie.")
    @click.option("-ca", "--checkall", is_flag=True, default=False, help="Check all storefront manifests for additional audios and subs.")
    @click.option("-sf", "--storefront", type=int, default=None, help="Override storefront int if needed.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return iTunes(ctx, **kwargs)

    def __init__(self, ctx, title: str, movie, checkall, storefront):
        super().__init__(ctx)
        self.parse_title(ctx, title)

        self.vcodec = ctx.parent.params["vcodec"]
        self.acodec = ctx.parent.params["acodec"]
        self.cdm = ctx.obj.cdm
        self.profile = ctx.obj.profile

        self.extra_server_parameters = {}
        self.rental_id = None
        self.rentals_supported = False
        self.movie = movie
        self.checkall = checkall
        self.storefront = storefront
        self.configure()

    def get_titles(self):
        titles = []

        contentId = re.findall('(umc.[a-z0-9]*.[a-z0-9]*)', self.title)[0]
        self.params = {
            'utsk': '6e3013c6d6fae3c2::::::9318c17fb39d6b9c',
            'caller': 'web',
            'sf': self.storefront,
            'v': '46',
            'pfm': 'appletv',
            'mfr': 'Apple',
            'locale': 'en-US',
            'l': 'en',
            'ctx_brand': 'tvs.sbd.9001',
            'count': '100',
            'skip': '0',
        }

        if self.movie:
            res = self.session.get(
                url=f'https://tv.apple.com/api/uts/v2/view/product/{contentId}',
                params=self.params
            )
            information = res.json()['data']['content']
            titles.append(Title(
                id_=self.title,
                type_=Title.Types.MOVIE,
                name=information['title'],
                original_lang="en",
                source=self.ALIASES[0],
                service_data=information
            ))
        else:
            res = self.session.get(
                url=f'https://tv.apple.com/api/uts/v2/view/show/{contentId}/episodes',
                params=self.params
            )
            episodes = res.json()["data"]["episodes"]
            for episode in episodes:
                titles.append(Title(
                    id_=self.title,
                    type_=Title.Types.TV,
                    name=episode["showTitle"],
                    season=episode["seasonNumber"],
                    episode=episode["episodeNumber"],
                    episode_name=episode.get("title"),
                    original_lang="en",
                    source=self.ALIASES[0],
                    service_data=episode
                ))
        return titles

    def get_tracks(self, title: Title) -> Tracks:
        content_id = title.service_data.get("id")
        
        stream_data = self.session.get(
            url=f'https://tv.apple.com/api/uts/v2/view/product/{content_id}/personalized',
            params={
                'utscf': 'OjAAAAAAAAA~',
                'utsk': '6e3013c6d6fae3c2::::::235656c069bb0efb',
                'caller': 'web',
                'sf': self.storefront,
                'v': '46',
                'pfm': 'web',
                'locale': 'en-US'
            }
        ).json()
        
        playables = stream_data.get('data', {}).get('content', {}).get('playables', [])
        if not playables:
            raise self.log.exit("No playables found in API response")
            
        # Store extra server parameters for license requests
        self.extra_server_parameters = playables[0].get('assets', {}).get('fpsKeyServerQueryParameters', {})
        
        # Collect all HLS URLs
        candidate_urls = set()
        for playable in playables:
            # Get rental ID if available
            try:
                self.rental_id = playable.get('itunesMediaApiData', {}).get('personalizedOffers', [{}])[0].get('rentalId')
            except (IndexError, KeyError):
                pass
                
            for offer in playable.get('itunesMediaApiData', {}).get('offers', []):
                hls_url = offer.get('hlsUrl')
                if not hls_url:
                    continue
                    
                # Only add the best quality, not all variants
                hls_url = hls_url.replace("SD", "UHD").replace("HD", "UHD").replace("UUHD", "UHD")
                candidate_urls.add(hls_url)
    
        if not candidate_urls:
            raise self.log.exit("Could not find any HLS URLs in API response")
    
        # Select ONLY the best URL (prefer UHD/HD over SD)
        best_url = None
        for url in candidate_urls:
            if "UHD" in url or "HD" in url:
                best_url = url
                break
        
        if not best_url:
            best_url = next(iter(candidate_urls))
        
        # Process ONLY the selected URL
        try:
            r = self.session.get(best_url)
            if not r.ok:
                raise self.log.exit(f"HTTP Error {r.status_code}: {r.reason}")
    
            master_hls_manifest = r.text
            master_playlist = m3u8.loads(master_hls_manifest, best_url)
            
            tracks = Tracks.from_m3u8(
                master_playlist,
                source=self.ALIASES[0]
            )
            
            # Extract chapters if available
            if 'chapter' in master_hls_manifest:
                try:
                    chapter_link = master_hls_manifest.rsplit('chapters.plist"', 1)[0].rsplit(',URI="', 1)[1] + 'chapters.plist'
                    title.service_data['chapters'] = plistlib.loads(self.session.get(chapter_link).content)['chapters']['chapter-list']
                except Exception:
                    pass
                    
        except Exception as e:
            self.log.debug(f"Failed to process manifest {best_url}: {e}")
            raise self.log.exit("Failed to get tracks from manifest")
    
        # Check extra storefronts for additional tracks
        if self.checkall:
            self.log.info("Checking extra storefronts for additional tracks...")
            for sf_code, sf_id in self.STOREFRONT_MAP.items():
                if str(sf_id) == str(self.storefront):
                    continue
                    
                try:
                    res_extra = self.session.get(
                        url=f'https://tv.apple.com/api/uts/v2/view/product/{content_id}/personalized',
                        params={
                            'utscf': 'OjAAAAAAAAA~',
                            'utsk': '6e3013c6d6fae3c2::::::235656c069bb0efb',
                            'caller': 'web',
                            'sf': sf_id,
                            'v': '46',
                            'pfm': 'web',
                            'locale': 'en-US'
                        }
                    ).json()
                    
                    extra_playables = res_extra.get('data', {}).get('content', {}).get('playables', [])
                    extra_hls_url = None
                    
                    for ep in extra_playables:
                        e_offers = ep.get('itunesMediaApiData', {}).get('offers', [])
                        if e_offers and 'hlsUrl' in e_offers[0]:
                            extra_hls_url = e_offers[0]['hlsUrl'].replace("SD", "UHD").replace("HD", "UHD").replace("UUHD", "UHD")
                            break
                    
                    if not extra_hls_url:
                        continue
                        
                    resp_extra = self.session.get(extra_hls_url)
                    if not resp_extra.ok:
                        continue
                        
                    extra_tracks = Tracks.from_m3u8(
                        m3u8.loads(resp_extra.text, extra_hls_url),
                        source=self.ALIASES[0]
                    )
                    
                    for extra_track in extra_tracks:
                        if isinstance(extra_track, (AudioTrack, TextTrack)):
                            tracks.add(extra_track, silent=True)
                            
                except Exception:
                    continue
    
        # Process and filter tracks
        for track in tracks:
            if isinstance(track, VideoTrack):
                track.needs_ccextractor_first = True
                track.encrypted = True
                
            if isinstance(track, AudioTrack):
                # Parse bitrate from URL
                match = re.search(r'_gr(\d+)', track.url)
                if match:
                    track.bitrate = int(match.group(1)) * 1000
                track.codec = track.codec.replace("_ak", "").replace("_ap3", "").replace("_vod", "")
                track.encrypted = True
                
            if isinstance(track, TextTrack):
                track.codec = "vtt"
    
        # Filter by codec preferences
        tracks.videos = [
            x for x in tracks.videos
            if x.codec[:3] in self.VIDEO_CODEC_MAP.get(self.vcodec, ["avc", "hvc", "hev"])
        ]
    
        if self.acodec:
            tracks.audios = [
                x for x in tracks.audios
                if x.codec.split("-")[0] in self.AUDIO_CODEC_MAP.get(self.acodec, [])
            ]
    
        # Remove duplicate SDH tracks
        sdh_tracks = [x.language for x in tracks.subtitles if x.sdh]
        tracks.subtitles = [x for x in tracks.subtitles if x.language not in sdh_tracks or x.sdh]
    
        # Filter to preferred CDN (ak-amt)
        return Tracks([
            x for x in tracks if "ak-amt" in x.url or x.url == ""
        ])

    def get_chapters(self, title):
        try:
            chapterData = title.service_data.get("chapters", [])
            chapters: list[MenuTrack] = []
            for i, chapter in enumerate(chapterData):
                chapters.append(MenuTrack(
                    number=i + 1,
                    title=f"Chapter {i + 1}",
                    timecode=datetime.utcfromtimestamp(float(chapter['start'])).strftime("%H:%M:%S.%f")[:-3]
                ))
            return chapters
        except (KeyError, TypeError):
            return []

    def certificate(self, **_):
        return None  # will use common privacy cert

    def license(self, challenge, track, session_id=None, service_name=None, **_):
        _is_playready = (hasattr(self.cdm, '__class__') and 'PlayReady' in self.cdm.__class__.__name__) or \
                        (hasattr(self.cdm, 'device') and hasattr(self.cdm.device, 'type') and 
                         self.cdm.device.type == LocalDevice.Types.PLAYREADY)
        if _is_playready:
            data = {
                "streaming-request": {
                    "version": 1,
                    "streaming-keys": [
                        {
                            "id": 1,
                            "uri": f"data:text/plain;charset=UTF-16;base64,{track.pr_pssh}",
                            "challenge": base64.b64encode(challenge).decode('utf-8'),
                            "key-system": "com.microsoft.playready",
                            "lease-action": "start",
                        }
                    ]
                }
            }
            
            # Add extra parameters if available
            if self.extra_server_parameters:
                data["streaming-request"]["streaming-keys"][0].update(self.extra_server_parameters)

            if self.rental_id:
                data["streaming-request"]["streaming-keys"][0]["rental-id"] = self.rental_id

            res = self.session.post(
                url=self.config["endpoints"]["license"],
                json=data
            ).json()
            
            status = res["streaming-response"]["streaming-keys"][0]["status"]
            if status != 0:
                self.log.debug(res)
                raise self.log.exit(f" - License request failed. Error: {status}")
                
            return res["streaming-response"]["streaming-keys"][0]["license"]
        else:
            data = {
                "streaming-request": {
                    "version": 1,
                    "streaming-keys": [
                        {
                            "id": 1,
                            "uri": f"data:text/plain;base64,{base64.b64encode(Box.build(track.pssh)).decode()}",
                            "challenge": base64.b64encode(challenge).decode(),
                            "key-system": "com.widevine.alpha",
                            "lease-action": "start",
                        }
                    ]
                }
            }
            
            # Add extra parameters if available
            if self.extra_server_parameters:
                data["streaming-request"]["streaming-keys"][0].update(self.extra_server_parameters)

            if self.rental_id:
                data["streaming-request"]["streaming-keys"][0]["rental-id"] = self.rental_id

            res = self.session.post(
                url=self.config["endpoints"]["license"],
                json=data
            ).json()
            
            status = res["streaming-response"]["streaming-keys"][0]["status"]
            if status != 0:
                self.log.debug(res)
                raise self.log.exit(f" - License request failed. Error: {status}")
                
            return res["streaming-response"]["streaming-keys"][0]["license"]

    def configure(self):
        """Configure session with necessary headers and tokens."""
        self.log.info("Configuring Apple TV+ session...")
        
        # Set storefront
        self._set_storefront()
        
        # Get environment config with improved token acquisition
        environment = self.get_environment_config()
        if not environment:
            raise self.log.exit("Failed to get iTunes' WEB TV App Environment Configuration...")
            
        try:
            # Get media-user-token with better error handling
            cookie_dict = self.session.cookies.get_dict()
            media_user_token = cookie_dict.get("media-user-token")
            
            if not media_user_token:
                self.log.warning("No 'media-user-token' cookie found, trying alternative methods...")
                # Try to get from other domains
                for domain in [".apple.com", "tv.apple.com", "music.apple.com"]:
                    try:
                        media_user_token = self.session.cookies.get("media-user-token", domain=domain)
                        if media_user_token:
                            self.log.info(f"Found media-user-token on domain: {domain}")
                            break
                    except Exception:
                        continue
            
            if not media_user_token:
                raise KeyError("media-user-token")
            
            self.session.headers.update({
                "User-Agent": self.config.get("user_agent", "AppleTV6,2/11.1"),
                "Authorization": f"Bearer {environment['MEDIA_API']['token']}",
                "media-user-token": media_user_token,
                "x-apple-music-user-token": media_user_token
            })
            self.log.info("Session headers updated successfully")
            
        except KeyError as e:
            raise self.log.exit(f" - No {e} cookie found, cannot log in. Please ensure you're logged into Apple TV+.")
    
    def _set_storefront(self):
        """Set storefront based on cookies or default."""
        if not self.storefront:
            self.log.info("Setting storefront...")
            
            # Try to get from cookie checking multiple domains
            try:
                itua_value = None
                # Check all possible Apple domains where itua might be set
                for domain in [".tv.apple.com", ".music.apple.com", ".apps.apple.com", ".apple.com"]:
                    try:
                        # Use get_dict() to access all cookies and filter manually
                        # because requests' get() with domain doesn't always work correctly
                        for cookie in self.session.cookies:
                            if cookie.name == 'itua' and cookie.domain == domain:
                                itua_value = cookie.value
                                self.log.info(f"Found itua cookie on domain: {domain} with value: {itua_value}")
                                break
                        if itua_value:
                            break
                    except Exception as e:
                        self.log.debug(f"Error checking domain {domain}: {e}")
                        continue
                
                if itua_value:
                    cc = itua_value.upper()
                    if cc in self.STOREFRONT_MAP:
                        self.storefront = self.STOREFRONT_MAP[cc]
                        self.log.info(f"Auto-detected storefront: {self.storefront} (from cookie itu: {cc})")
                    else:
                        self.log.warning(f"Country code {cc} not in storefront map, using default")
                else:
                    self.log.warning("No 'itua' cookie found in any domain")
                    
            except Exception as e:
                self.log.debug(f"Error getting storefront from cookie: {e}")
            
            # Fallback to default US storefront
            if not self.storefront:
                self.storefront = "143441"  # US
                self.log.info(f"Using default storefront: {self.storefront}")
    
    def get_environment_config(self):
        """Loads environment config data from WEB App's <meta> tag."""
        self.log.info("Getting environment configuration...")
        
        # Method 1: Try Apple TV+ meta tag
        try:
            res = self.session.get("https://tv.apple.com")
            if res.ok:
                # Try multiple patterns for environment config
                patterns = [
                    r'web-tv-app/config/environment"[\s\S]*?content="([^"]+)',
                    r'data-env="([^"]+)"',
                    r'__NEXT_DATA__["\']?\s*[:=]\s*({.*?});',
                    r'window\.__ENV__\s*=\s*({.*?});'
                ]
                
                for pattern in patterns:
                    env = re.search(pattern, res.text, re.DOTALL)
                    if env:
                        try:
                            data = json.loads(unquote(env[1]))
                            if data and ('MEDIA_API' in data or 'token' in str(data)):
                                self.log.info("âœ“ Got environment config from Apple TV+")
                                return data
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            self.log.debug(f"Apple TV+ config failed: {e}")
        
        # Method 2: Try Apple Music fallback
        self.log.info("Trying Apple Music fallback for token...")
        music_config = self.get_music_config()
        if music_config:
            self.log.info("âœ“ Got token from Apple Music")
            return music_config
        
        self.log.error("Failed to get environment configuration from all sources")
        return None
    
    def get_music_config(self):
        """Fallback method to get token from Apple Music with improved extraction."""
        try:
            r = self.session.get("https://music.apple.com/us/browse")
            if not r.ok:
                return None
                
            # Try multiple JavaScript file patterns
            js_patterns = [
                r'src="(/assets/index~[^"]+\.js)"',
                r'src="(/assets/index\.[^"]+\.js)"',
                r'src="(https://[^"]+apple\.com/[^"]+\.js)"'
            ]
            
            js_url = None
            for pattern in js_patterns:
                match = re.search(pattern, r.text)
                if match:
                    js_url = match.group(1)
                    if not js_url.startswith('http'):
                        js_url = "https://music.apple.com" + js_url
                    break
            
            if not js_url:
                # Look for inline tokens if no JS file found
                self.log.info("Looking for inline tokens...")
                inline_tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{100,}', r.text)
                if inline_tokens:
                    token = max(inline_tokens, key=len)
                    return {"MEDIA_API": {"token": token}}
            
            if js_url:
                self.log.info(f"Fetching JavaScript from: {js_url}")
                r2 = self.session.get(js_url)
                if r2.ok:
                    # Look for tokens in the JavaScript file
                    tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{100,}', r2.text)
                    if tokens:
                        token = max(tokens, key=len)
                        self.log.info(f"âœ“ Extracted token from JavaScript (length: {len(token)})")
                        return {"MEDIA_API": {"token": token}}
            
            # Try alternative endpoints
            alt_endpoints = [
                "https://music.apple.com/assets/index.js",
                "https://amp-api.music.apple.com/v1/catalog/us/songs",
                "https://api.music.apple.com/v1/storefronts"
            ]
            
            for endpoint in alt_endpoints:
                try:
                    self.log.debug(f"Trying alternative endpoint: {endpoint}")
                    r3 = self.session.get(endpoint)
                    if r3.ok:
                        tokens = re.findall(r'eyJh[A-Za-z0-9\._-]{100,}', r3.text)
                        if tokens:
                            token = max(tokens, key=len)
                            self.log.info(f"âœ“ Got token from {endpoint}")
                            return {"MEDIA_API": {"token": token}}
                except Exception:
                    continue
                    
        except Exception as e:
            self.log.debug(f"Apple Music fallback failed: {e}")
        
        return None