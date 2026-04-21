import base64
import hashlib
import requests
from langcodes import Language, closest_match
from typing import Optional, Dict, Any
from fuckdl.utils.monalisa import MonaLisa, MonaLisaCDM
from fuckdl.constants import LANGUAGE_MAX_DISTANCE
from fuckdl.utils.widevine.cdm import Cdm  # noqa: F401
from fuckdl.utils.widevine.protos.widevine_pb2 import WidevineCencHeader  # noqa: F401
from fuckdl.vendor.pymp4.parser import Box


# MonaLisa DRM imports
try:
    from fuckdl.utils.monalisa import MonaLisa
    from fuckdl.utils.monalisa_cdm import MonaLisaCDM
    MONALISA_AVAILABLE = True
except ImportError as e:
    MONALISA_AVAILABLE = False
    MonaLisa = None
    MonaLisaCDM = None
    # log.debug(f"MonaLisa DRM not available: {e}")

def get_boxes(data, box_type, as_bytes=False):
    """Scan a byte array for a wanted box, then parse and yield each find."""
    if not isinstance(data, (bytes, bytearray)):
        raise ValueError("data must be bytes")
    while True:
        try:
            index = data.index(box_type)
        except ValueError:
            break
        if index < 0:
            break
        if index > 4:
            index -= 4
        data = data[index:]
        try:
            box = Box.parse(data)
        except IOError:
            break
        if as_bytes:
            box = Box.build(box)
        yield box


def is_close_match(language, languages):
    if not (language and languages and all(languages)):
        return False
    languages = list(map(str, [x for x in languages if x]))
    return closest_match(language, languages)[1] <= LANGUAGE_MAX_DISTANCE


def get_closest_match(language, languages):
    match, distance = closest_match(language, list(map(str, languages)))
    if distance > LANGUAGE_MAX_DISTANCE:
        return None
    return Language.get(match)


def numeric_quality(quality):
    if not quality:
        return 0
    if quality == "SD":
        return 576
    return int(quality)


def try_get(obj, func):
    try:
        return func(obj)
    except (AttributeError, IndexError, KeyError, TypeError):
        return None


def short_hash(input):
    return base_encode(int(hashlib.md5(input).hexdigest(), 16))


def base_encode(num, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'):
    """Convert a number to a base string using the given alphabet."""
    if num == 0:
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        num, rem = divmod(num, base)
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def get_ip_info(session: Optional[requests.Session] = None) -> Dict[str, Any]:
    """
    Get IP geolocation information.
    
    Args:
        session: Optional requests session to use for the request.
        
    Returns:
        Dictionary with IP information including country_code, country, etc.
    """
    try:
        if session is None:
            session = requests.Session()
        
        # Try multiple IP geolocation services
        services = [
            ("https://ipapi.co/json/", lambda x: {
                "ip": x.get("ip"),
                "country_code": x.get("country_code", "").upper(),
                "country": x.get("country_name", ""),
                "region": x.get("region", ""),
                "city": x.get("city", ""),
            }),
            ("https://ip-api.com/json/", lambda x: {
                "ip": x.get("query"),
                "country_code": x.get("countryCode", "").upper(),
                "country": x.get("country", ""),
                "region": x.get("regionName", ""),
                "city": x.get("city", ""),
            }),
            ("http://ip-api.com/json/", lambda x: {
                "ip": x.get("query"),
                "country_code": x.get("countryCode", "").upper(),
                "country": x.get("country", ""),
                "region": x.get("regionName", ""),
                "city": x.get("city", ""),
            }),
        ]
        
        for url, parser in services:
            try:
                response = session.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    result = parser(data)
                    if result.get("country_code"):
                        return result
            except Exception:
                continue
        
        # Fallback: return default values
        return {
            "ip": "unknown",
            "country_code": "US",
            "country": "United States",
            "region": "",
            "city": ""
        }
        
    except Exception as e:
        # Log error but return default
        import logging
        logging.getLogger(__name__).warning(f"Failed to get IP info: {e}")
        return {
            "ip": "unknown",
            "country_code": "US",
            "country": "United States",
            "region": "",
            "city": ""
        }


# Export public API
__all__ = [
    # Box utilities
    "get_boxes",
    # Language utilities
    "is_close_match",
    "get_closest_match",
    "numeric_quality",
    "try_get",
    "short_hash",
    "base_encode",
    # IP utilities
    "get_ip_info",
    # MonaLisa DRM
    "MonaLisa",
    "MonaLisaCDM",
    "MONALISA_AVAILABLE",
    # Subtitle Converter
    "SubtitleConverter",
    "get_converter",
    "convert_subtitle_file",
    "convert_track_subtitles",
    "ensure_all_subtitles_are_srt",
    "convert_all_tracks_to_srt",
    "SUBTITLE_CONVERTER_AVAILABLE",
    # Widevine
    "Cdm",
    "WidevineCencHeader",
]