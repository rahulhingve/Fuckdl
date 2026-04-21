import shutil
import asyncio
import base64
import logging
import math
import os
import re
import json
import subprocess
import sys
import uuid
from collections import defaultdict
from enum import Enum
from io import BytesIO, TextIOWrapper
from typing import Optional, List, Dict, Any, Tuple, Union
from pathlib import Path
import humanfriendly
import m3u8
import pycaption
import requests
import pysubs2
import time
from subby import WebVTTConverter, SMPTEConverter, WVTTConverter, ISMTConverter, CommonIssuesFixer
from langcodes import Language
from requests import Session
from fuckdl import config
from fuckdl.constants import LANGUAGE_MUX_MAP, TERRITORY_MAP
from fuckdl.utils import Cdm, get_boxes, get_closest_match, is_close_match, try_get
from fuckdl.utils.collections import as_list
from fuckdl.utils.io import aria2c, download_range, m3u8re
from fuckdl.utils.subprocess import ffprobe
from fuckdl.utils.widevine.protos.widevine_pb2 import WidevineCencHeader
from fuckdl.utils.xml import load_xml
from fuckdl.vendor.pymp4.parser import Box, MP4

logging.getLogger("srt").setLevel(logging.ERROR)
logging.getLogger("pycaption").setLevel(logging.WARNING)


def format_duration(seconds):
    """Helper function to format duration for Hybrid logging."""
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:06.3f}"


logger = logging.getLogger("Tracks")
logger.setLevel(logging.INFO)

CODEC_MAP = {
    # Video
    "avc1": "H.264",
    "avc3": "H.264",
    "hev1": "H.265",
    "hvc1": "H.265",
    "dvh1": "H.265",
    "dvhe": "H.265",
    "av01": "AV1",
    # Audio
    "aac": "AAC",
    "mp4a": "AAC",
    "stereo": "AAC",
    "HE": "HE-AAC",
    "ac3": "AC3",
    "ac-3": "AC3",
    "dd": "DD",           # Dolby Digital
    "eac": "E-AC3",       # Dolby Digital Plus
    "eac3": "E-AC3",      # Dolby Digital Plus
    "eac-3": "E-AC3",     # Dolby Digital Plus
    "ec-3": "DD+",        # Dolby Digital Plus (alias)
    "ddp": "DD+",         # Dolby Digital Plus
    "dd+": "DD+",         # Dolby Digital Plus
    "atmos": "DD+ Atmos", # Dolby Atmos sobre DD+
    "ec3": "DD+",         # Dolby Digital Plus
    # Subtitles
    "srt": "SRT",
    "vtt": "VTT",
    "wvtt": "WVTT",
    "dfxp": "TTML",
    "stpp": "TTML",
    "ttml": "TTML",
    "tt": "TTML",
    "ass": "ASS",
    "ssa": "SSA",
}


class Track:
    class Descriptor(Enum):
        URL = 1
        M3U = 2
        MPD = 3
        ISM = 4
        DASH = 5
        HLS = 6

    def __init__(self, id_, source, url, codec, language=None, descriptor=Descriptor.URL,
                 needs_proxy=False, needs_repack=False, encrypted=False, pssh=None, pr_pssh=None,
                 smooth=False, note=None, kid=None, key=None, extra=None, monalisa=False, mls_pssh=None):
        self.id = id_
        self.source = source
        self.url = url
        self.note = note
        self.codec = codec
        self.language = Language.get(language or "en")
        self.is_original_lang = False
        self.descriptor = descriptor
        self.needs_proxy = bool(needs_proxy)
        self.needs_repack = bool(needs_repack)
        self.encrypted = bool(encrypted)
        self.pssh = pssh
        self.pr_pssh = pr_pssh
        self.smooth = smooth
        self.kid = kid
        self.key = key
        self.drm_objects = []
        self.monalisa = bool(monalisa)
        self.mls_pssh = mls_pssh
        self.extra = extra or {}
        self._location = None
        self.segment_durations: List[float] = []
        self.timescale: int = 1
        self._cached_init_data = None

    def __repr__(self):
        return "{name}({items})".format(
            name=self.__class__.__name__,
            items=", ".join([f"{k}={repr(v)}" for k, v in self.__dict__.items()])
        )

    def __eq__(self, other):
        return isinstance(other, Track) and self.id == other.id

    def get_track_name(self):
        """Return the base track name."""
        if self.language is None:
            self.language = Language.get("en")
        if ((self.language.language or "").lower() == (self.language.territory or "").lower()
                and self.language.territory not in TERRITORY_MAP):
            self.language.territory = None
        if self.language.territory == "US":
            self.language.territory = None
        language = self.language.simplify_script()
        extra_parts = []
        if language.script is not None:
            extra_parts.append(language.script_name())
        if language.territory is not None:
            territory = language.territory_name()
            extra_parts.append(TERRITORY_MAP.get(language.territory, territory))
        return ", ".join(extra_parts) or None

    def get_data_chunk(self, session=None, use_cache=True):
        """Get the data chunk from the track's stream with caching support."""
        if use_cache and hasattr(self, '_cached_init_data') and self._cached_init_data:
            return self._cached_init_data
            
        if not session:
            session = Session()
    
        if str(self.source).startswith(("HBOMax", "UNVP")):
            if hasattr(self, 'pr_pssh') and self.pr_pssh:
                try:
                    result = base64.b64decode(self.pr_pssh)
                    if use_cache:
                        self._cached_init_data = result
                    return result
                except:
                    pass
            
            if hasattr(self, 'pssh') and self.pssh:
                if isinstance(self.pssh, bytes):
                    if use_cache:
                        self._cached_init_data = self.pssh
                    return self.pssh
                if hasattr(self.pssh, 'init_data'):
                    if use_cache:
                        self._cached_init_data = self.pssh.init_data
                    return self.pssh.init_data
            
            return b""
    
        url = None
    
        if self.descriptor == self.Descriptor.M3U:
            master = m3u8.loads(session.get(as_list(self.url)[0]).text, uri=self.url)
            for segment in master.segments:
                if not segment.init_section:
                    continue
                if self.source in ["DSNP", "STRP"] and re.match(r"^[a-zA-Z0-9]{4}-(BUMPER|DUB_CARD)/", segment.init_section.uri):
                    continue
                url = ("" if re.match("^https?://", segment.init_section.uri) else segment.init_section.base_uri)
                url += segment.init_section.uri
                break
    
        if not url:
            url = as_list(self.url)[0]
    
        chunk_size = 20000
        
        try:
            with session.get(url, stream=True) as s:
                for chunk in s.iter_content(chunk_size):
                    if use_cache:
                        self._cached_init_data = chunk
                    return chunk
        except Exception as e:
            logger.debug(f"Failed to download chunk from {url}: {e}")
    
        if self.needs_proxy:
            proxy = next(iter(session.proxies.values()), None)
        else:
            proxy = None
    
        try:
            result = download_range(url, chunk_size, proxy=proxy)
            if use_cache:
                self._cached_init_data = result
            return result
        except Exception as e:
            logger.debug(f"Failed to download range from {url}: {e}")
            return b""

    def _extract_pssh_from_mp4dump(self, mp4_data, system_id_pattern):
        """Extract PSSH data from mp4dump JSON output."""
        for box in mp4_data:
            if box.get('name') == 'moov':
                for child in box.get('children', []):
                    if 'system_id' in child and child['system_id'] == system_id_pattern:
                        box_size_dec = child.get('size', 0)
                        pssh_data = child.get('data', '')
                        pssh_data_size_dec = child.get('data_size', 0)
                        if pssh_data:
                            return box_size_dec, pssh_data, pssh_data_size_dec
        return None, None, None

    def _build_pssh_hex(self, box_size_dec, pssh_data, pssh_data_size_dec, system_id_hex):
        """Build PSSH hex string from components."""
        pssh_data = pssh_data.replace('[', '').replace(']', '').replace(' ', '')
        if len(pssh_data) % 2 != 0:
            raise ValueError("Invalid hex string length. Must be even.")
        box_size_hex = format(box_size_dec, '08x')
        pssh_data_size_hex = format(pssh_data_size_dec, '08x')
        return f'{box_size_hex}7073736800000000{system_id_hex}{pssh_data_size_hex}{pssh_data}'

    def get_pr_pssh(self, session=None):
        """Get the PlayReady PSSH of the track."""
        if hasattr(self, 'kid') and self.kid:
            return True
            
        if self.pr_pssh:
            return True
            
        if self.source in ["MA", "CRAV", "ITV"]:
            mp4_file = 'init.mp4'
            data = self.get_data_chunk(session)
            with open(mp4_file, 'wb') as f:
                f.write(data)

            executable = shutil.which("mp4dump")
            mp4_data = subprocess.check_output([executable, '--format', 'json', '--verbosity', '3', mp4_file])
            mp4_data = json.loads(mp4_data)

            system_id_pattern = '[9a 04 f0 79 98 40 42 86 ab 92 e6 5b e0 88 5f 95]'
            box_size_dec, pssh_data, pssh_data_size_dec = self._extract_pssh_from_mp4dump(mp4_data, system_id_pattern)
            
            if box_size_dec and pssh_data:
                system_id_hex = '9a04f07998404286ab92e65be0885f95'
                pr_pssh_hex = self._build_pssh_hex(box_size_dec, pssh_data, pssh_data_size_dec, system_id_hex)
                self.pr_pssh = base64.b64encode(bytes.fromhex(pr_pssh_hex)).decode('utf-8')
                return True

        if not session:
            session = Session()

        boxes = []

        if self.descriptor == self.Descriptor.M3U:
            master = m3u8.loads(session.get(as_list(self.url)[0]).text, uri=self.url)
            boxes.extend([
                Box.parse(base64.b64decode(x.uri.split(",")[-1]))
                for x in (master.session_keys or master.keys)
                if x and x.keyformat and x.keyformat.lower() == Cdm.urn
            ])
            for x in master.session_keys:
                if x and x.keyformat and x.keyformat.lower() == "com.microsoft.playready":
                    self.pr_pssh = x.uri.split(",")[-1]
                    break
            for x in master.keys:
                if x and x.keyformat and "com.microsoft.playready" in str(x):
                    self.pr_pssh = str(x).split("\"")[1].split(",")[-1]
                    break

        try:
            xml_str = base64.b64decode(self.pr_pssh).decode("utf-16-le", "ignore")
            xml_str = xml_str[xml_str.index("<"):]
            xml = load_xml(xml_str).find("DATA")
            self.kid = xml.findtext("KID")
            if not self.kid:
                self.kid = next(iter(xml.xpath("PROTECTINFO/KID/@VALUE")), None)
            if not self.kid:
                self.kid = next(iter(xml.xpath("PROTECTINFO/KIDS/KID/@VALUE")), None)
            if self.kid:
                self.kid = uuid.UUID(base64.b64decode(self.kid).hex()).bytes_le.hex()
            return True
        except:
            pass

        try:
            if self.pssh or not self.encrypted:
                return True

            if not session:
                session = Session()

            boxes = []
            data = self.get_data_chunk(session)
            if data:
                boxes.extend(list(get_boxes(data, b"pssh")))

            for box in boxes:
                if box.system_ID == Cdm.uuid:
                    self.pssh = box
                    return True

            for box in boxes:
                if box.system_ID == uuid.UUID("{9a04f079-9840-4286-ab92-e65be0885f95}"):
                    xml_str = Box.build(box)
                    xml_str = xml_str.decode("utf-16-le", "ignore")
                    xml_str = xml_str[xml_str.index("<"):]
                    xml = load_xml(xml_str).find("DATA")
                    kid = xml.findtext("KID")
                    if not kid:
                        kid = next(iter(xml.xpath("PROTECTINFO/KID/@VALUE")), None)
                    if not kid:
                        kid = next(iter(xml.xpath("PROTECTINFO/KIDS/KID/@VALUE")), None)

                    init_data = WidevineCencHeader()
                    init_data.key_id.append(uuid.UUID(bytes_le=base64.b64decode(kid)).bytes)
                    init_data.algorithm = 1

                    self.pssh = Box.parse(Box.build({
                        "type": b"pssh",
                        "version": 0,
                        "flags": 0,
                        "system_ID": Cdm.uuid,
                        "init_data": init_data.SerializeToString(),
                    }))
                    return True
        except Exception as e:
            logger.debug(f"Error getting PR PSSH: {e}")
            pass

        return False

    def get_pssh(self, session=None):
        """Get the Widevine PSSH of the track."""
        if hasattr(self, 'drm_objects') and self.drm_objects:
            for drm in self.drm_objects:
                if hasattr(drm, 'pssh'):
                    self.pssh = drm.pssh
                    return True

        if self.source in ["MA"]:
            mp4_file = 'init.mp4'
            data = self.get_data_chunk(session)
            with open(mp4_file, 'wb') as f:
                f.write(data)

            executable = shutil.which("mp4dump")
            mp4_data = subprocess.check_output([executable, '--format', 'json', '--verbosity', '3', mp4_file])
            mp4_data = json.loads(mp4_data)

            system_id_pattern = '[ed ef 8b a9 79 d6 4a ce a3 c8 27 dc d5 1d 21 ed]'
            box_size_dec, pssh_data, pssh_data_size_dec = self._extract_pssh_from_mp4dump(mp4_data, system_id_pattern)
            
            if box_size_dec and pssh_data:
                system_id_hex = 'edef8ba979d64acea3c827dcd51d21ed'
                pssh_hex = self._build_pssh_hex(box_size_dec, pssh_data, pssh_data_size_dec, system_id_hex)
                self.pssh = base64.b64encode(bytes.fromhex(pssh_hex)).decode('utf-8')
                return True

        if self.pssh or not self.encrypted:
            return True

        if not session:
            session = Session()

        boxes = []

        if self.descriptor == self.Descriptor.M3U:
            master = m3u8.loads(session.get(as_list(self.url)[0]).text, uri=self.url)
            boxes.extend([
                Box.parse(base64.b64decode(x.uri.split(",")[-1]))
                for x in (master.session_keys or master.keys)
                if x and x.keyformat and x.keyformat.lower() == Cdm.urn
            ])

        data = self.get_data_chunk(session)
        if data:
            boxes.extend(list(get_boxes(data, b"pssh")))

        for box in boxes:
            if box.system_ID == Cdm.uuid:
                self.pssh = box
                return True

        for box in boxes:
            if box.system_ID == uuid.UUID("{9a04f079-9840-4286-ab92-e65be0885f95}"):
                xml_str = Box.build(box)
                xml_str = xml_str.decode("utf-16-le", "ignore")
                xml_str = xml_str[xml_str.index("<"):]
                xml = load_xml(xml_str).find("DATA")
                kid = xml.findtext("KID")
                if not kid:
                    kid = next(iter(xml.xpath("PROTECTINFO/KID/@VALUE")), None)
                if not kid:
                    kid = next(iter(xml.xpath("PROTECTINFO/KIDS/KID/@VALUE")), None)

                init_data = WidevineCencHeader()
                init_data.key_id.append(uuid.UUID(bytes_le=base64.b64decode(kid)).bytes)
                init_data.algorithm = 1

                self.pssh = Box.parse(Box.build({
                    "type": b"pssh",
                    "version": 0,
                    "flags": 0,
                    "system_ID": Cdm.uuid,
                    "init_data": init_data.SerializeToString(),
                }))
                return True

        return False

    def _extract_kid_from_mp4dump(self, mp4_data):
        """Extract KID from mp4dump JSON output."""
        for box in mp4_data:
            if box.get('name') == 'moov':
                for child in box.get('children', []):
                    if child.get('name') == 'trak':
                        for grandchild in child.get('children', []):
                            if grandchild.get('name') == 'mdia':
                                for great in grandchild.get('children', []):
                                    if great.get('name') == 'minf':
                                        for stbl in great.get('children', []):
                                            if stbl.get('name') == 'stbl':
                                                for stsd in stbl.get('children', []):
                                                    if stsd.get('name') == 'stsd':
                                                        for enc in stsd.get('children', []):
                                                            if enc.get('name') in ('encv', 'enca'):
                                                                for sinf in enc.get('children', []):
                                                                    if sinf.get('name') == 'sinf':
                                                                        for schi in sinf.get('children', []):
                                                                            if schi.get('name') == 'schi':
                                                                                for tenc in schi.get('children', []):
                                                                                    if tenc.get('name') == 'tenc':
                                                                                        kid = tenc.get('default_KID', '')
                                                                                        if kid:
                                                                                            return kid.replace(' ', '').replace('[', '').replace(']', '')
        return None

    def get_kid(self, session=None):
        """Get the KID (encryption key id) of the Track."""
        if hasattr(self, 'kid') and self.kid:
            return True
            
        if hasattr(self, 'drm_objects') and self.drm_objects:
            for drm in self.drm_objects:
                if hasattr(drm, 'kid') and drm.kid:
                    self.kid = drm.kid.hex if hasattr(drm.kid, 'hex') else str(drm.kid)
                if hasattr(drm, 'key') and drm.key:
                    self.key = drm.key
                self.monalisa = True
                if hasattr(drm, 'pssh') and drm.pssh:
                    self.mls_pssh = drm.pssh
                log = logging.getLogger(self.__class__.__name__)
                key_display = drm.key if drm.key else None
                log.info(f"âœ“ MonaLisa keys applied: KID={self.kid}, KEY={key_display}")
                return True

        if self.source in ["DSNP", "MA", "CRAV", "ITV"] and not self.kid:
            mp4_file = 'init.mp4'
            data = self.get_data_chunk(session)
            with open(mp4_file, 'wb') as f:
                f.write(data)

            executable = shutil.which("mp4dump")
            mp4_data = subprocess.check_output([executable, '--format', 'json', '--verbosity', '3', mp4_file])
            mp4_data = json.loads(mp4_data)
            os.remove(mp4_file)

            kid = self._extract_kid_from_mp4dump(mp4_data)
            if kid:
                self.kid = kid
                return True

        if self.kid or not self.encrypted:
            return True

        boxes = []
        data = self.get_data_chunk(session)

        if data:
            probe = ffprobe(data)
            if probe:
                kid = try_get(probe, lambda x: x["streams"]["tags"]["enc_key_id"])
                if kid:
                    kid = base64.b64decode(kid).hex()
                    if kid != "00" * 16:
                        self.kid = kid
                        return True
            boxes.extend(list(get_boxes(data, b"tenc")))
            boxes.extend(list(get_boxes(data, b"pssh")))

        if self.get_pssh():
            boxes.append(self.pssh)

        for box in sorted(boxes, key=lambda b: b.type == b"tenc", reverse=True):
            if box.type == b"tenc":
                kid = box.key_ID.hex
                if kid != "00" * 16:
                    self.kid = kid
                    return True
            if box.type == b"pssh":
                if box.system_ID == Cdm.uuid:
                    if getattr(box, "key_IDs", None):
                        kid = box.key_IDs[0].hex
                        if kid != "00" * 16:
                            self.kid = kid
                            return True
                    cenc_header = WidevineCencHeader()
                    cenc_header.ParseFromString(box.init_data)
                    if getattr(cenc_header, "key_id", None):
                        kid = cenc_header.key_id[0]
                        try:
                            int(kid, 16)
                        except ValueError:
                            kid = kid.hex()
                        else:
                            kid = kid.decode()
                        if kid != "00" * 16:
                            self.kid = kid
                            return True

        return False

    def _download_m3u8_track(self, out, re_name, headers, proxy):
        """Download HLS/M3U8 track using m3u8 library."""
        first_url = as_list(self.url)[0]
        if isinstance(first_url, str) and first_url.strip().startswith('#EXTM3U'):
            logger.debug(f"Track {self.id}: M3U8 content detected, parsing directly")
            master = m3u8.loads(first_url, uri=None)
        else:
            logger.debug(f"Track {self.id}: Fetching M3U8 from URL")
            master = m3u8.loads(
                requests.get(
                    first_url,
                    headers=headers,
                    proxies={"all": proxy} if self.needs_proxy and proxy else None
                ).text,
                uri=first_url
            )

        if any(master.keys + master.session_keys):
            if isinstance(self, (VideoTrack, AudioTrack)):
                self.encrypted = True
                if not self.key:
                    self.get_kid()
                    self.get_pssh()
            else:
                self.encrypted = False

        durations, duration = [], 0
        for segment in master.segments:
            if segment.discontinuity:
                durations.append(duration)
                duration = 0
            duration += segment.duration
        durations.append(duration)
        largest_continuity = durations.index(max(durations)) if durations else 0

        discontinuity, has_init, segments = 0, False, []
        for segment in master.segments:
            if segment.discontinuity:
                discontinuity += 1
                has_init = False
            if self.source in ["DSNP", "STRP"] and re.search(
                r"^[a-zA-Z0-9]{4}-(BUMPER|DUB_CARD)/",
                segment.uri + (segment.init_section.uri if segment.init_section else '')
            ):
                continue
            if self.source == "ATVP" and discontinuity != largest_continuity:
                continue
            if segment.init_section and not has_init:
                segments.append(
                    ("" if re.match("^https?://", segment.init_section.uri) else segment.init_section.base_uri) +
                    segment.init_section.uri
                )
                has_init = True
            segments.append(
                ("" if re.match("^https?://", segment.uri) else segment.base_uri) + segment.uri
            )
            if isinstance(self, TextTrack):
                self.segment_durations.append(segment.duration)
        self.url = segments
        
        save_path = os.path.join(out, re_name + ".mp4")
        asyncio.run(aria2c(self.url, save_path, headers or {}, proxy if self.needs_proxy else None, track=self))
        return save_path

    def _get_download_filename(self, re_name):
        """Determine the correct filename for download based on track type and codec."""
        is_ass = False
        is_wvtt = False
        
        if hasattr(self, 'codec') and self.codec:
            if self.codec.lower() in ['ass', 'ssa']:
                is_ass = True
            elif self.codec.lower() == 'wvtt':
                is_wvtt = True
        
        if hasattr(self, 'is_ass') and self.is_ass:
            is_ass = True
        
        if is_ass:
            return re_name + ".ass"
        elif is_wvtt:
            return re_name + ".vtt"
        elif isinstance(self, TextTrack):
            return re_name + ".vtt"
        elif isinstance(self, AudioTrack) and self.source in ["iT", "ATVP"]:
            return re_name + ".m4a"
        else:
            return re_name + ".mp4"

    def _find_downloaded_file(self, out_dir, re_name, save_path):
        """Find the downloaded file if it exists under a different name."""
        out_dir = Path(out_dir)
        base_name = os.path.splitext(save_path)[0]
        for ext in ['.mp4', '.m4a', '.vtt', '.mkv', '.ass', '.ssa', '']:
            test_path = base_name + ext
            if os.path.exists(test_path):
                return test_path

        current_time = time.time()
        files_in_dir = sorted(out_dir.glob("*"), key=lambda f: f.stat().st_mtime, reverse=True)
        for f in files_in_dir:
            if f.is_file() and (current_time - f.stat().st_mtime) < 60:
                if f.stat().st_size > 0:
                    logger.info(f" + Found downloaded file: {f.name}")
                    return str(f)
        return None

    def download(self, out, name=None, headers=None, proxy=None):        
        """
        Download the Track and apply any necessary post-edits like Subtitle conversion.
        """
        if os.path.isfile(out):
            raise ValueError("Path must be to a directory and not a file")
    
        os.makedirs(out, exist_ok=True)
    
        re_name = (name or "{type}_{id}_{enc}").format(
            type=self.__class__.__name__,
            id=self.id,
            enc="enc" if self.encrypted else "dec"
        )
        
        # ===== HBOMax DASH downloader RE =====
        if str(self.source).startswith("HBOMax"):
    
            executable = shutil.which("N_m3u8DL-RE") or shutil.which("RE")
            
            if not executable:
                # Buscar en ./binaries relativo al script
                script_dir = Path(__file__).parent.parent
                bins = script_dir / "binaries"
                executable = (
                    shutil.which("N_m3u8DL-RE", path=bins)
                    or shutil.which("RE", path=bins)
                )
            
            if not executable:
                raise EnvironmentError(
                    "N_m3u8DL-RE executable not found. "
                    "Make sure it's in one of these locations:\n"
                    f"  - In your PATH (current PATH: {os.environ.get('PATH', 'NOT SET')})\n"
                    f"  - In: {script_dir / 'binaries'}\n"
                    f"  - Or download from: https://github.com/nilaoda/N_m3u8DL-RE/releases"
                )
    
            mpd_url = getattr(self, "manifest_url", None)
            if not mpd_url:
                raise RuntimeError("MPD manifest URL missing for HX track")
    
            out_dir = Path(out)
            out_dir.mkdir(parents=True, exist_ok=True)
    
            cmd = [
                executable,
                mpd_url,
                "--save-name", re_name,
                "--save-dir", str(out_dir),
                "--tmp-dir", str(out_dir),
                "--auto-subtitle-fix", "True",
                "--log-level", "ERROR",
            ]
            
            # SELECT TRACK TYPE
            if hasattr(self, "mpd_representation_id") and self.mpd_representation_id:
                if isinstance(self, VideoTrack):
                    cmd += ["--select-video", f"id={self.mpd_representation_id}"]
                elif isinstance(self, AudioTrack):
                    cmd += ["--select-audio", f"id={self.mpd_representation_id}"]
                elif isinstance(self, TextTrack):
                    cmd += ["--select-subtitle", f"id={self.mpd_representation_id}"]
            else:
                if isinstance(self, TextTrack):
                    if self.mpd_representation_id:
                        cmd += ["--select-subtitle", f"id={self.mpd_representation_id}"]
                    else:
                        cmd += ["--select-subtitle", f"lang={self.language}"]
    
            if self.needs_proxy and proxy:
                cmd += ["--custom-proxy", proxy]
            else:
                cmd += ["--use-system-proxy", "False"]
    
            # Subprocess RE    
            subprocess.run(cmd, check=True)
    
            files_in_dir = list(out_dir.rglob(f"{re_name}*"))
            if not files_in_dir:
                raise RuntimeError(f"HBOMax downloader no generÃ³ ningÃºn archivo para {re_name}")
    
            self._location = files_in_dir[0]
            return self._location
  
        # Determine output filename
        save_path = os.path.join(out, self._get_download_filename(re_name))
    
        # Parse M3U8 if needed
        if (self.descriptor == self.Descriptor.M3U and self.source not in ["iT", "ATVP", "HS"]) or \
           (self.descriptor == self.Descriptor.M3U and self.source == "ATVP" and isinstance(self, TextTrack)):
            save_path = self._download_m3u8_track(out, re_name, headers, proxy)
    
        # Download execution
        if self.source == "AMZN" and 'ism/' in str(self.url[0] if isinstance(self.url, list) else self.url):
            bins = Path(out).parent / 'binaries'
            executable = shutil.which("N_m3u8DL-RE", path=bins) or shutil.which("RE", path=bins)
            if not executable:
                raise EnvironmentError("N_m3u8DL-RE executable not found...")
    
            uri = self.url if isinstance(self.url, list) else [self.url]
            pattern = r"(https?://.*?\.ism/)"
            match = re.search(pattern, uri[0])
            if not match:
                raise ValueError("No ISM manifest URL found")
    
            extracted_url = match.group(1) + "manifest"
            cmd = f"{executable} {extracted_url} --save-name {re_name} --save-dir {out} --tmp-dir {out} -sv best"
            if self.needs_proxy and proxy:
                cmd += f" --custom-proxy {proxy}"
            else:
                cmd += " --use-system-proxy False"
            subprocess.run(cmd, shell=True, check=True)
    
            out_dir = Path(out)
            files_in_dir = list(out_dir.rglob(f"*{re_name}*"))
            if files_in_dir:
                largest_file = max(files_in_dir, key=lambda f: f.stat().st_size)
                self._location = str(largest_file)
            else:
                raise IOError(f"Download failed, no file found matching {re_name}")
    
        elif self.source == "CORE":
            asyncio.run(saldl(self.url, save_path, headers, proxy if self.needs_proxy else None))
            if os.path.exists(save_path):
                self._location = save_path
            else:
                out_dir = Path(out)
                files_in_dir = list(out_dir.rglob(f"*{re_name}*"))
                if files_in_dir:
                    self._location = str(files_in_dir[0])
    
        elif self.source == "NF" and proxy is not None:
            asyncio.run(tqdm_downloader(self.url, save_path, headers, proxy if self.needs_proxy else None))
            if os.path.exists(save_path):
                self._location = save_path
            else:
                out_dir = Path(out)
                files_in_dir = list(out_dir.rglob(f"*{re_name}*"))
                if files_in_dir:
                    self._location = str(files_in_dir[0])
    
        elif self.source in ["iT", "ATVP", "HS", "Hotstar"] and not isinstance(self, TextTrack):
            asyncio.run(m3u8re(self.url, save_path, headers, proxy if self.needs_proxy else None))
            if os.path.exists(save_path):
                self._location = save_path
            else:
                out_dir = Path(out)
                files_in_dir = sorted(out_dir.glob("*"), key=lambda f: f.stat().st_mtime, reverse=True)
                for f in files_in_dir[:5]:
                    if f.is_file() and f.stat().st_size > 0:
                        if re_name.replace("enc", "dec") in f.name or re_name in f.name:
                            self._location = str(f)
                            break
                if not self._location:
                    files_in_dir = list(out_dir.rglob(f"*{re_name.split('_')[0]}*"))
                    if files_in_dir:
                        largest_file = max(files_in_dir, key=lambda f: f.stat().st_size)
                        self._location = str(largest_file)
        else:
            asyncio.run(aria2c(
                self.url, save_path,
                headers if self.source not in ["ATVP", "iT"] else {},
                proxy if self.needs_proxy else None,
                track=self
            ))
    
            if os.path.exists(save_path):
                self._location = save_path
            else:
                self._location = self._find_downloaded_file(out, re_name, save_path)
    
        if not self._location or not os.path.exists(self._location):
            raise IOError(f"Download failed, file not created. Expected: {save_path}")
    
        if os.path.getsize(self._location) <= 3:
            raise IOError("Download failed, the downloaded file is empty.")
    
        logger.debug(f" + File saved to: {self._location}")
    
        return self._location

    def delete(self):
        if self._location:
            os.unlink(self._location)
            self._location = None

    def repackage(self):
        if not self._location:
            raise ValueError("Cannot repackage a Track that has not been downloaded.")
        fixed_file = f"{self._location}_fixed.mkv"
        try:
            proc = subprocess.run([
                "ffmpeg", "-hide_banner",
                "-loglevel", "error",
                "-i", self._location,
                "-map_metadata", "-1",
                "-fflags", "bitexact",
                "-codec", "copy",
                fixed_file
            ], capture_output=True, text=True)

            if proc.stderr:
                for line in proc.stderr.strip().split('\n'):
                    if line.strip():
                        print(f"   ! {line.strip()}")

            if proc.returncode == 0:
                self.swap(fixed_file)
            else:
                print(f"   ! Repackage failed, using original file")
        except subprocess.CalledProcessError as e:
            if e.stderr:
                for line in e.stderr.strip().split('\n'):
                    if line.strip():
                        print(f"   ! {line.strip()}")
            print(f"   ! Repackage failed, using original file")

    def locate(self):
        return self._location

    def move(self, target):
        if not self._location:
            return False
        ok = os.path.realpath(shutil.move(self._location, target)) == os.path.realpath(target)
        if ok:
            self._location = target
        return ok

    def swap(self, target):
        if not os.path.exists(target) or not self._location:
            return False
        os.unlink(self._location)
        os.rename(target, self._location)
        return True

    @staticmethod
    def pt_to_sec(d):
        if isinstance(d, float):
            return d
        if d[0:2] == "P0":
            d = d.replace("P0Y0M0DT", "PT")
        if d[0:2] != "PT":
            raise ValueError("Input data is not a valid time string.")
        d = d[2:].upper()
        m = re.findall(r"([\d.]+.)", d)
        return sum(
            float(x[0:-1]) * {"H": 60 * 60, "M": 60, "S": 1}[x[-1].upper()]
            for x in m
        )


class VideoTrack(Track):
    def __init__(self, *args, bitrate, width, size=None, height, fps=None, hdr10=False, dvhdr=False, hlg=False, dv=False,
                 needs_ccextractor=False, needs_ccextractor_first=False, mpd_representation_id=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.bitrate = int(math.ceil(float(bitrate))) if bitrate else None
        self.width = int(width)
        self.height = int(height)
        if "/" in str(fps):
            num, den = fps.split("/")
            self.fps = int(num) / int(den)
        elif fps:
            self.fps = float(fps)
        else:
            self.fps = None

        self.fps_duration = self._get_fps_duration(self.fps)
        self.size = size if size else None
        self.hdr10 = bool(hdr10)
        self.dvhdr = bool(dvhdr)
        self.hlg = bool(hlg)
        self.dv = bool(dv)
        self.needs_ccextractor = needs_ccextractor
        self.needs_ccextractor_first = needs_ccextractor_first
        self.mpd_representation_id = mpd_representation_id

    def _get_fps_duration(self, fps):
        if not fps:
            return "24000/1001p"

        fps_rounded = round(fps, 3)

        if abs(fps_rounded - 23.976) < 0.01 or abs(fps_rounded - 24000/1001) < 0.01:
            return "24000/1001p"
        elif abs(fps_rounded - 24.0) < 0.01:
            return "24p"
        elif abs(fps_rounded - 25.0) < 0.01:
            return "25p"
        elif abs(fps_rounded - 29.97) < 0.01:
            return "30000/1001p"
        elif abs(fps_rounded - 30.0) < 0.01:
            return "30p"
        elif abs(fps_rounded - 50.0) < 0.01:
            return "50p"
        elif abs(fps_rounded - 59.94) < 0.01:
            return "60000/1001p"
        elif abs(fps_rounded - 60.0) < 0.01:
            return "60p"
        else:
            return f"{fps}fps"

    def __str__(self):
        codec = next((CODEC_MAP[x] for x in CODEC_MAP if (self.codec or "").startswith(x)), self.codec)
        fps = f"{self.fps:.3f}" if self.fps else "Unknown"
        size = f" ({humanfriendly.format_size(self.size, binary=True)})" if self.size else ""
        return " | ".join([
            "â”œâ”€ VID",
            f"[{codec}, {'DV+HDR' if self.dvhdr else 'HDR10' if self.hdr10 else 'HLG' if self.hlg else 'DV' if self.dv else 'SDR'}]",
            f"{self.width}x{self.height} @ {self.bitrate // 1000 if self.bitrate else '?'} kb/s{size}, {fps} FPS"
        ])

    def ccextractor(self, track_id, out_path, language, original=False):
        if not self._location:
            raise ValueError("You must download the track first.")

        executable = shutil.which("ccextractor") or shutil.which("ccextractorwin")
        if not executable:
            raise EnvironmentError("ccextractor executable was not found.")
        try:
            p = subprocess.Popen([
                executable,
                "-quiet", "-trim", "-noru", "-ru1",
                self._location, "-o", out_path
            ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in TextIOWrapper(p.stdout, encoding="utf-8"):
                if "[iso file] Unknown box type ID32" not in line:
                    sys.stdout.write(line)
            p.wait()

            if os.path.exists(out_path):
                if os.stat(out_path).st_size <= 3:
                    os.unlink(out_path)
                    return None
                cc_track = TextTrack(
                    id_=track_id,
                    source=self.source,
                    url="",
                    codec="srt",
                    language=language,
                    cc=True
                )
                cc_track._location = out_path
                return cc_track
        except:
            pass

        return None


class AudioTrack(Track):
    def __init__(self, *args, bitrate, size=None, channels=None,
                 descriptive: bool = False, atmos: bool = False, mpd_representation_id=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.bitrate = int(math.ceil(float(bitrate))) if bitrate else None
        self.size = size if size else None
        self.channels = self.parse_channels(channels) if channels else None
        self.atmos = bool(atmos)
        self.descriptive = bool(descriptive)
        self.mpd_representation_id = mpd_representation_id

    @staticmethod
    def parse_channels(channels):
        """Parse channel string to standard format (e.g., '2.0', '5.1', '7.1')."""
        if channels in ["A000", "a000"]:
            return "2.0"
        if channels in ["F801", "f801"]:
            return "5.1"
        try:
            channels = str(float(channels))
        except ValueError:
            channels = str(channels)
        if channels == "6.0":
            return "5.1"
        return channels

    def get_codec_display(self) -> str:
        """
        Get a user-friendly display string for the audio codec.
        Handles DD+, E-AC3, Atmos, etc. like unshackle.
        """
        codec_lower = (self.codec or "").lower()
        
        # Dolby Digital Plus / DD+ / E-AC3
        if codec_lower in ("eac3", "eac-3", "ec-3", "ddp", "dd+", "eac"):
            if self.atmos:
                return "DD+ Atmos"
            return "DD+"
        
        # Dolby Digital / AC3
        if codec_lower in ("ac3", "ac-3", "dd"):
            return "DD"
        
        # AAC
        if codec_lower in ("aac", "mp4a", "stereo"):
            if "he" in codec_lower:
                return "HE-AAC"
            return "AAC"
        
        # DTS
        if "dts" in codec_lower:
            return "DTS"
        
        # Opus
        if "opus" in codec_lower:
            return "OPUS"
        
        # Fallback
        return next((CODEC_MAP[x] for x in CODEC_MAP if codec_lower.startswith(x)), self.codec or "Unknown")

    def get_track_name(self):
        track_name = super().get_track_name() or ""
        flag = self.descriptive and "Descriptive"
        if flag:
            if track_name:
                flag = f" ({flag})"
            track_name += flag
        return track_name or None

    def __str__(self):
        size = f" ({humanfriendly.format_size(self.size, binary=True)})" if self.size else ""
        codec_display = self.get_codec_display()
        
        # Build audio codec string with Atmos indicator if present
        if self.atmos and "Atmos" not in codec_display:
            codec_display = f"{codec_display} Atmos"
        
        return " | ".join([x for x in [
            "â”œâ”€ AUD",
            f"[{codec_display}]",
            f"{self.channels}" if self.channels else None,
            f"{self.bitrate // 1000 if self.bitrate else '?'} kb/s{size}",
            f"{self.language}",
            " ".join([self.get_track_name() or "", "[Original]" if self.is_original_lang else ""]).strip()
        ] if x])


class TextTrack(Track):
    _CUE_ID_PATTERN = re.compile(r"^[A-Za-z]+\d+$")
    _TIMING_LINE_PATTERN = re.compile(r"^((?:\d+:)?\d+:\d+[.,]\d+)\s*-->\s*((?:\d+:)?\d+:\d+[.,]\d+)(.*)$")
    _LINE_POS_PATTERN = re.compile(r"line:(\d+(?:\.\d+)?%?)")
    _GHOST_TAG_RE = re.compile(r"\{[^}]*\}")
    
    _SDH_PATTERNS = [
        r'\[[^\]]*\]',      # [text]
        r'\([^\)]*\)',      # (text)
        r'\{[^\}]*\}',      # {text}
        r'<[^>]*>',         # <text>
        r'â™ª[^â™ª]*â™ª',         # â™ªtextâ™ª
        r'[\*_][^\*_]+[\*_]',  # *text* or _text_
    ]

    def __init__(self, *args, cc=False, sdh=False, forced=False, mpd_representation_id=None, **kwargs):
        external = kwargs.pop('external', False)
        
        super().__init__(*args, **kwargs)
        self.cc = bool(cc)
        self.sdh = bool(sdh)
        if self.cc and self.sdh:
            raise ValueError("A text track cannot be both CC and SDH.")
        self.forced = bool(forced)
        if (self.cc or self.sdh) and self.forced:
            raise ValueError("A text track cannot be CC/SDH as well as Forced.")
        self.mpd_representation_id = mpd_representation_id
        self.external = bool(external)

    def get_track_name(self):
        """Return the base track name without forced/sdh/cc suffixes."""
        track_name = super().get_track_name() or ""
        return track_name or None

    @staticmethod
    def _iter_boxes(data: bytes):
        """Iterate over boxes in an MP4 file."""
        offset = 0
        while offset + 8 <= len(data):
            box_size = int.from_bytes(data[offset:offset + 4], "big")
            box_type = data[offset + 4:offset + 8]
            if box_size < 8:
                break
            payload = data[offset + 8: offset + box_size]
            yield box_type, payload
            offset += box_size

    @staticmethod
    def _wvtt_mdat_has_cue(mdat_payload: bytes) -> bool:
        """Check if a WVTT MDAT payload contains actual cue data."""
        offset = 0
        while offset + 8 <= len(mdat_payload):
            inner_size = int.from_bytes(mdat_payload[offset:offset + 4], "big")
            inner_type = mdat_payload[offset + 4:offset + 8]
            if inner_size < 8:
                break
            if inner_type == b"vttc":
                inner_payload = mdat_payload[offset + 8: offset + inner_size]
                p = 0
                while p + 8 <= len(inner_payload):
                    ps = int.from_bytes(inner_payload[p:p + 4], "big")
                    pt = inner_payload[p + 4:p + 8]
                    if ps < 8:
                        break
                    if pt == b"payl":
                        text = inner_payload[p + 8: p + ps].strip(b"\x00").strip()
                        if text:
                            return True
                    p += ps
            offset += inner_size
        return False

    @classmethod
    def extract_mdat_text(cls, data: bytes, codec: str) -> bytes:
        """Extract text from MDAT boxes for WVTT and similar formats."""
        codec_lower = codec.lower()
        plain_text_codecs = {"vtt", "webvtt", "webvtt-lssdh-ios8", "ttml", "ttml2", "dfxp", "smpte"}
        
        if codec_lower in plain_text_codecs:
            return data
    
        collected = []
        mdat_count = 0
        
        try:
            for box_type, payload in cls._iter_boxes(data):
                if box_type != b"mdat":
                    continue
                
                mdat_count += 1
                
                if codec_lower in ["wvtt", "stpp"]:
                    cues_data = cls._extract_wvtt_text_from_mdat(payload)
                    if cues_data and len(cues_data) > 10:
                        collected.append(cues_data)
                    else:
                        clean = payload.lstrip(b"\x00").strip()
                        if clean:
                            try:
                                text = clean.decode('utf-8', errors='ignore')
                                lines = []
                                for line in text.split('\n'):
                                    line = line.strip()
                                    if line and not line.startswith('<?xml') and not line.startswith('<tt'):
                                        line = re.sub(r'[^\x20-\x7E\u00A0-\u00FF\u4E00-\u9FFF]', '', line)
                                        if line and len(line) > 3:
                                            lines.append(line)
                                if lines:
                                    vtt_content = "WEBVTT\n\n" + '\n'.join(lines)
                                    collected.append(vtt_content.encode('utf-8'))
                            except:
                                collected.append(clean)
                else:
                    clean = payload.lstrip(b"\x00").strip()
                    if clean and len(clean) > 10:
                        collected.append(clean)
        except Exception as e:
            logger.debug(f"Error extracting MDAT: {e}")
        
        logger.debug(f" + Extracted from {mdat_count} MDAT boxes, got {len(collected)} chunks, total size: {sum(len(c) for c in collected)} bytes")
        
        if not collected:
            clean_data = data.lstrip(b"\x00").strip()
            if clean_data and len(clean_data) > 10:
                try:
                    text = clean_data.decode('utf-8', errors='ignore')
                    if 'WEBVTT' in text or '-->' in text:
                        return clean_data
                    if '<tt>' in text or '<div>' in text:
                        return clean_data
                except:
                    pass
            return data
        
        result = b"\n".join(collected)
        
        if b"WEBVTT" not in result and b"-->" in result:
            lines = result.split(b'\n')
            for i, line in enumerate(lines):
                if b"-->" in line:
                    lines.insert(0, b"WEBVTT")
                    lines.insert(1, b"")
                    result = b'\n'.join(lines)
                    break
        
        result = result.replace(b'\x00', b'')
        
        return result

    @classmethod
    def _extract_wvtt_text_from_mdat(cls, mdat_payload: bytes) -> bytes:
        """Extract actual text content from WVTT MDAT payload and reconstruct as proper VTT."""
        cues = []
        offset = 0
        
        while offset + 8 <= len(mdat_payload):
            inner_size = int.from_bytes(mdat_payload[offset:offset + 4], "big")
            inner_type = mdat_payload[offset + 4:offset + 8]
            
            if inner_size < 8:
                break
                
            if inner_type == b"vttc":
                inner_payload = mdat_payload[offset + 8: offset + inner_size]
                cue_data = cls._parse_vttc_box(inner_payload)
                if cue_data:
                    cues.append(cue_data)
            elif inner_type == b"vtte":
                pass
                    
            offset += inner_size
        
        if not cues:
            return b""
        
        return cls._reconstruct_vtt_from_cues(cues)

    @classmethod
    def _parse_vttc_box(cls, data: bytes) -> Optional[Dict]:
        """Parse a vttc box and extract timing and text."""
        result = {
            "start": None,
            "end": None,
            "text": [],
            "settings": ""
        }
        
        offset = 0
        while offset + 8 <= len(data):
            box_size = int.from_bytes(data[offset:offset + 4], "big")
            box_type = data[offset + 4:offset + 8]
            
            if box_size < 8:
                break
                
            payload = data[offset + 8: offset + box_size]
            
            if box_type == b"payl":
                text = payload.strip(b"\x00").strip()
                if text:
                    try:
                        text_str = text.decode('utf-8', errors='ignore')
                        text_str = text_str.replace('\x00', '')
                        if text_str.strip():
                            result["text"].append(text_str)
                    except:
                        pass
                        
            elif box_type == b"sttg":
                try:
                    result["settings"] = payload.decode('utf-8', errors='ignore').strip()
                except:
                    pass
                    
            elif box_type == b"idnt":
                try:
                    ident = payload.decode('utf-8', errors='ignore')
                    if '-->' in ident:
                        parts = ident.split('-->')
                        if len(parts) == 2:
                            result["start"] = parts[0].strip()
                            result["end"] = parts[1].strip()
                except:
                    pass
                    
            offset += box_size
        
        return result if result["text"] else None
    
    @classmethod
    def _reconstruct_vtt_from_cues(cls, cues: List[Dict]) -> bytes:
        """Reconstruct a complete VTT file from parsed cues."""
        vtt_lines = ["WEBVTT", ""]
        
        for i, cue in enumerate(cues):
            if not cue["start"] or not cue["end"]:
                start_sec = i * 4
                end_sec = start_sec + 4
                start = f"{start_sec // 3600:02d}:{(start_sec % 3600) // 60:02d}:{start_sec % 60:02d}.000"
                end = f"{end_sec // 3600:02d}:{(end_sec % 3600) // 60:02d}:{end_sec % 60:02d}.000"
            else:
                start = cue["start"]
                end = cue["end"]
            
            timestamp_line = f"{start} --> {end}"
            if cue["settings"]:
                timestamp_line += f" {cue['settings']}"
            
            vtt_lines.append(timestamp_line)
            
            for text in cue["text"]:
                text = re.sub(r'<c[^>]*>', '', text)
                text = re.sub(r'</c>', '', text)
                text = re.sub(r'<ruby>', '', text)
                text = re.sub(r'</ruby>', '', text)
                text = re.sub(r'<rt>', '', text)
                text = re.sub(r'</rt>', '', text)
                text = re.sub(r'<[0-9:]+>', '', text)
                
                lines = text.split('\n')
                for line in lines:
                    if line.strip():
                        vtt_lines.append(line.strip())
            
            vtt_lines.append("")
        
        return '\n'.join(vtt_lines).encode('utf-8')

    @classmethod
    def strip_sdh_brackets(cls, text: str) -> str:
        """Remove SDH/CC annotations from subtitle text."""
        for pattern in cls._SDH_PATTERNS:
            text = re.sub(pattern, '', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @classmethod
    def convert_vtt_to_srt(cls, vtt_content: str, strip_sdh: bool = True) -> str:
        """Convert WebVTT to SRT format with proper formatting preservation."""
        lines = []
        counter = 1
        
        vtt_content = vtt_content.replace('\r\n', '\n')
        blocks = re.split(r'\n\s*\n', vtt_content)
        
        for block in blocks:
            block_lines = block.strip().split('\n')
            if not block_lines:
                continue
            
            first_line = block_lines[0].strip() if block_lines else ''
            if first_line == 'WEBVTT' or first_line.startswith('WEBVTT'):
                continue
            
            if first_line == 'STYLE':
                continue
            
            timestamp_line = None
            text_lines = []
            settings = ''
            
            for line in block_lines:
                line = line.strip()
                if '-->' in line:
                    timestamp_line = line
                    if ' line:' in line or ' position:' in line or ' align:' in line:
                        settings = line[line.find('-->') + 3:].strip()
                elif line and not line.isdigit() and '-->' not in line:
                    clean_line = re.sub(r'<(?!/?(?:i|b|u|s|ruby|rt|c))[^>]+>', '', line)
                    clean_line = re.sub(r'&nbsp;', ' ', clean_line)
                    if clean_line.strip():
                        if strip_sdh:
                            clean_line = cls.strip_sdh_brackets(clean_line)
                        if clean_line.strip():
                            text_lines.append(clean_line)
            
            if timestamp_line and text_lines:
                ts_parts = timestamp_line.split('-->')
                if len(ts_parts) == 2:
                    start = ts_parts[0].strip().replace('.', ',')
                    end = ts_parts[1].split()[0].strip().replace('.', ',') if ts_parts[1] else ''
                    
                    def normalize_timestamp(ts):
                        if not ts:
                            return ts
                        parts = ts.replace(',', ':').split(':')
                        if len(parts) == 2:
                            return f"00:{parts[0].zfill(2)}:{parts[1].zfill(2)}"
                        elif len(parts) == 3:
                            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{parts[2].zfill(2)}"
                        return ts
                    
                    start = normalize_timestamp(start)
                    end = normalize_timestamp(end)
                    
                    if ',' in start:
                        main, ms = start.split(',')
                        ms = ms.ljust(3, '0')[:3]
                        start = f"{main},{ms}"
                    if ',' in end:
                        main, ms = end.split(',')
                        ms = ms.ljust(3, '0')[:3]
                        end = f"{main},{ms}"
                    
                    text = '\n'.join(text_lines)
                    
                    if strip_sdh:
                        text = cls.strip_sdh_brackets(text)
                    
                    if text.strip():
                        lines.append(str(counter))
                        lines.append(f"{start} --> {end}")
                        lines.append(text)
                        lines.append("")
                        counter += 1
        
        return '\n'.join(lines)

    @classmethod
    def merge_segmented_webvtt(cls, vtt_raw: str, segment_durations: Optional[List[float]] = None, timescale: int = 1) -> str:
        """Merge segmented WebVTT data with proper timestamp adjustment."""
        MPEG_TIMESCALE = 90_000
        
        has_timestamp_map = 'X-TIMESTAMP-MAP' in vtt_raw
        if not has_timestamp_map:
            return cls._merge_webvtt_text(vtt_raw)
        
        all_mpegts = re.findall(r'MPEGTS:(\d+)', vtt_raw)
        all_local = re.findall(r'LOCAL:([^\s,\n]+)', vtt_raw)
        all_local_zero = all(loc == '00:00:00.000' for loc in all_local)
        all_mpegts_same = len(set(all_mpegts)) == 1
        
        if all_mpegts and all_local and all_local_zero and all_mpegts_same:
            return cls._merge_webvtt_text(vtt_raw)
        
        try:
            import pycaption
            reader = pycaption.WebVTTReader()
            caption_set = reader.read(vtt_raw)
            
            if not caption_set.get_languages():
                return cls._merge_webvtt_text(vtt_raw)
            
            lang = caption_set.get_languages()[0]
            captions = caption_set.get_captions(lang)
            
            if segment_durations and captions:
                first_segment_mpegts = segment_durations[0] * MPEG_TIMESCALE if segment_durations else 0
                
                for caption in captions:
                    if hasattr(caption, 'mpegts') and caption.mpegts:
                        offset = (caption.mpegts - first_segment_mpegts) / MPEG_TIMESCALE
                        if offset > 0:
                            caption.start = caption.start + (offset * 1_000_000)
                            caption.end = caption.end + (offset * 1_000_000)
            
            writer = pycaption.WebVTTWriter()
            return writer.write(caption_set)
            
        except Exception as e:
            logger.warning(f"Failed to parse segmented VTT with pycaption: {e}, falling back to text merge")
            return cls._merge_webvtt_text(vtt_raw)

    @classmethod
    def _merge_webvtt_text(cls, vtt_raw: str) -> str:
        """Simple text-based merge for WebVTT with absolute timestamps."""
        segments = re.split(r"(?=WEBVTT)", vtt_raw.strip())
        segments = [s.strip() for s in segments if s.strip()]
        
        out_lines = ["WEBVTT", ""]
        style_block = None
        
        for seg in segments:
            seg_lines = seg.splitlines()
            i = 0
            
            if seg_lines and seg_lines[i].startswith("WEBVTT"):
                i += 1
            
            while i < len(seg_lines) and not seg_lines[i].strip():
                i += 1
            
            if i < len(seg_lines) and seg_lines[i].strip() == "STYLE":
                if style_block is None:
                    style_lines = ["STYLE"]
                    i += 1
                    while i < len(seg_lines) and seg_lines[i].strip():
                        style_lines.append(seg_lines[i])
                        i += 1
                    style_block = "\n".join(style_lines)
                else:
                    i += 1
                    while i < len(seg_lines) and seg_lines[i].strip():
                        i += 1
                continue
            
            if i < len(seg_lines) and "X-TIMESTAMP-MAP" in seg_lines[i]:
                i += 1
                while i < len(seg_lines) and not seg_lines[i].strip():
                    i += 1
            
            while i < len(seg_lines):
                line = seg_lines[i]
                if line.strip() and not line.startswith("WEBVTT"):
                    out_lines.append(line)
                i += 1
            
            if out_lines[-1]:
                out_lines.append("")
        
        if style_block:
            out_lines.insert(2, style_block)
            out_lines.insert(3, "")
        
        return "\n".join(out_lines)

    @classmethod
    def inject_vtt_position_tags(cls, text: str) -> str:
        """Inject {\an8} position tags for top-positioned WebVTT cues."""
        lines = text.split('\n')
        result = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            match = cls._TIMING_LINE_PATTERN.match(line)
            if match:
                settings = match.group(3) if len(match.groups()) > 2 else ''
                line_match = cls._LINE_POS_PATTERN.search(settings)
                is_top = False
                if line_match:
                    pos_str = line_match.group(1).rstrip('%')
                    try:
                        is_top = float(pos_str) < 50.0
                    except ValueError:
                        pass
                
                result.append(line)
                i += 1
                
                while i < len(lines) and lines[i].strip() and '-->' not in lines[i]:
                    cue_line = lines[i]
                    if is_top and not cue_line.startswith('{\\an'):
                        cue_line = '{\\an8}' + cue_line
                    result.append(cue_line)
                    i += 1
            else:
                result.append(line)
                i += 1
        
        return '\n'.join(result)

    @classmethod
    def sanitize_webvtt(cls, text: str) -> str:
        """Thorough sanitization of WebVTT content."""
        if not text.strip().startswith("WEBVTT"):
            text = "WEBVTT\n\n" + text
        
        lines = text.split('\n')
        sanitized_lines = []
        header_done = False
        
        for line in lines:
            if not header_done:
                if line.startswith("WEBVTT"):
                    sanitized_lines.append("WEBVTT")
                    header_done = True
                continue
            
            if '-' in line and '-->' in line:
                line = re.sub(r'(-\d+:\d+:\d+\.\d+)', '00:00:00.000', line)
            
            match = cls._TIMING_LINE_PATTERN.match(line)
            if match:
                start, end = match.group(1), match.group(2)
                if start.count(':') == 1:
                    start = f"00:{start}"
                if end.count(':') == 1:
                    end = f"00:{end}"
                line = f"{start} --> {end}"
            
            sanitized_lines.append(line)
        
        return '\n'.join(sanitized_lines)

    def convert_subtitle_to_srt(self, save_path: str, strip_sdh: bool = True) -> Optional[str]:
        """Convert subtitle to SRT format with comprehensive handling."""
        import logging
        log = logging.getLogger("Tracks")
        
        # Skip if already SRT
        if self.codec and self.codec.lower() == "srt":
            return save_path
        
        # Skip ASS/SSA
        if self.codec and self.codec.lower() in ["ass", "ssa"]:
            log.info(f" + ASS/SSA subtitle kept in original format")
            if not save_path.endswith('.ass'):
                new_path = os.path.splitext(save_path)[0] + '.ass'
                os.rename(save_path, new_path)
                return new_path
            return save_path
        
        # Read the file
        with open(save_path, "rb") as fd:
            raw = fd.read()
        
        if len(raw) < 10:
            log.warning(f" - Subtitle file too small ({len(raw)} bytes), skipping conversion")
            return save_path
        
        # Try to extract text from binary formats
        if self.codec and self.codec.lower() in ["wvtt", "stpp"]:
            log.info(f" + Extracting text from {self.codec.upper()} container...")
            extracted = self.extract_mdat_text(raw, self.codec)
            if extracted and len(extracted) > 10:
                try:
                    vtt_content = extracted.decode('utf-8', errors='ignore')
                    if '-->' in vtt_content:
                        srt_content = self.convert_vtt_to_srt(vtt_content, strip_sdh)
                        if srt_content and srt_content.strip():
                            srt_path = os.path.splitext(save_path)[0] + '.srt'
                            with open(srt_path, 'w', encoding='utf-8') as f:
                                f.write(srt_content)
                            # Remove old file
                            if os.path.exists(save_path) and save_path != srt_path:
                                try:
                                    os.unlink(save_path)
                                except:
                                    pass
                            log.info(f" + Subtitle converted to SRT: {os.path.basename(srt_path)} ({len(srt_content)} chars)")
                            return srt_path
                except Exception as e:
                    log.warning(f" - Failed to decode extracted content: {e}")
        
        # Handle VTT
        elif self.codec and self.codec.lower() in ["vtt", "webvtt"]:
            try:
                if raw.startswith(b'\x00\x00\x00') or (len(raw) > 4 and raw[:4] == b'mdat'):
                    log.info(" + Detected binary VTT, extracting from MDAT...")
                    extracted = self.extract_mdat_text(raw, "vtt")
                    vtt_content = extracted.decode('utf-8', errors='ignore')
                else:
                    vtt_content = raw.decode('utf-8', errors='ignore')
                
                srt_content = self.convert_vtt_to_srt(vtt_content, strip_sdh)
                if srt_content and srt_content.strip():
                    srt_path = os.path.splitext(save_path)[0] + '.srt'
                    with open(srt_path, 'w', encoding='utf-8') as f:
                        f.write(srt_content)
                    if os.path.exists(save_path) and save_path != srt_path:
                        try:
                            os.unlink(save_path)
                        except:
                            pass
                    log.info(f" + Subtitle converted to SRT: {os.path.basename(srt_path)} ({len(srt_content)} chars)")
                    return srt_path
            except Exception as e:
                log.warning(f" - VTT conversion failed: {e}")
        
        # Return original if conversion failed
        return save_path

    def download(self, out, name=None, headers=None, proxy=None):
        """
        Download the subtitle track and convert to SRT.
        """
        import logging
        import asyncio
        import os
        import shutil
        from pathlib import Path
        import urllib.parse
        import re
        
        # IMPORTANT: Import aria2c from the correct module
        from fuckdl.utils.io import aria2c
        
        log = logging.getLogger("Tracks")
        
        if os.path.isfile(out):
            raise ValueError("Path must be to a directory and not a file")
    
        os.makedirs(out, exist_ok=True)
    
        re_name = (name or "{type}_{id}_{enc}").format(
            type=self.__class__.__name__,
            id=self.id,
            enc="enc" if self.encrypted else "dec"
        )
        
        # ===== HBOMax DASH subtitle downloader using aria2c =====
        if str(self.source).startswith("HBOMax"):
            log.info(f" + Downloading HBOMax subtitle using aria2c")
            
            # Build the complete URL from extra data
            track_url = None
            period_base_url = None
            
            # Try to get period_base_url from extra
            if hasattr(self, 'extra') and self.extra:
                if isinstance(self.extra, tuple) and len(self.extra) >= 2:
                    adaptation_set = self.extra[1]
                    period = adaptation_set.getparent() if adaptation_set is not None else None
                    if period is not None:
                        period_base_url = period.findtext("BaseURL")
            
            # Get the track URL
            if hasattr(self, 'url') and self.url:
                if isinstance(self.url, list) and len(self.url) > 0:
                    track_url = self.url[0] if isinstance(self.url[0], str) else None
                elif isinstance(self.url, str):
                    track_url = self.url
            
            # If we don't have a valid URL, try to build from extra
            if not track_url and hasattr(self, 'extra') and self.extra:
                if isinstance(self.extra, tuple) and len(self.extra) >= 1:
                    rep = self.extra[0]
                    if rep is not None:
                        # Try to get BaseURL
                        track_url = rep.findtext("BaseURL")
                        if not track_url:
                            segment_template = rep.find("SegmentTemplate")
                            if segment_template is not None:
                                track_url = segment_template.get("media")
            
            # Ensure track_url is a string
            if not track_url or not isinstance(track_url, str):
                log.warning(f" - No valid URL found for subtitle track")
                # Try fallback to N_m3u8DL-RE
                track_url = None
            
            # If we have a URL with $Number$, expand it
            urls_to_download = []
            
            if track_url and '$Number$' in track_url:
                # Get segment count from timeline
                start_number = 1
                segment_count = 1
                
                if hasattr(self, 'extra') and self.extra:
                    if isinstance(self.extra, tuple) and len(self.extra) >= 1:
                        rep = self.extra[0]
                        segment_template = rep.find("SegmentTemplate") if rep is not None else None
                        if segment_template is None and len(self.extra) >= 2:
                            adaptation_set = self.extra[1]
                            segment_template = adaptation_set.find("SegmentTemplate") if adaptation_set is not None else None
                        
                        if segment_template is not None:
                            start_number = int(segment_template.get("startNumber") or 1)
                            
                            segment_timeline = segment_template.find("SegmentTimeline")
                            if segment_timeline is not None:
                                segment_count = 0
                                for s in segment_timeline.findall("S"):
                                    segment_count += 1 + (int(s.get("r") or 0))
                            else:
                                # Check for duration attribute
                                duration = segment_template.get("duration")
                                timescale = segment_template.get("timescale")
                                if duration and timescale:
                                    # Approximate segment count based on period duration
                                    period_duration = None
                                    if period_base_url:
                                        # Try to get period duration from extra
                                        pass
                                    segment_count = 1  # Default to 1 if unknown
                
                # Build URLs
                for i in range(start_number, start_number + segment_count):
                    segment_url = track_url.replace('$Number$', str(i))
                    if period_base_url and not re.match("^https?://", segment_url.lower()):
                        segment_url = urllib.parse.urljoin(period_base_url, segment_url)
                    urls_to_download.append(segment_url)
            
            elif track_url:
                # Single URL
                if period_base_url and not re.match("^https?://", track_url.lower()):
                    track_url = urllib.parse.urljoin(period_base_url, track_url)
                urls_to_download = [track_url]
            
            # Download the subtitles
            if urls_to_download:
                save_path = os.path.join(out, re_name + ".vtt")
                
                try:
                    if len(urls_to_download) == 1:
                        # Single file download
                        asyncio.run(aria2c(urls_to_download[0], save_path, headers or {}, proxy if self.needs_proxy else None))
                    else:
                        # Multiple segment download
                        asyncio.run(aria2c(urls_to_download, save_path, headers or {}, proxy if self.needs_proxy else None, track=self))
                    
                    if os.path.exists(save_path) and os.path.getsize(save_path) > 10:
                        self._location = save_path
                        log.info(f" + Subtitle downloaded: {os.path.basename(save_path)} ({os.path.getsize(save_path)} bytes)")
                    else:
                        log.warning(f" - Download produced empty or missing file")
                        
                except Exception as e:
                    log.warning(f" - aria2c download failed: {e}")
                    self._location = None
            
            # Fallback to N_m3u8DL-RE if aria2c failed
            if not self._location or not os.path.exists(self._location):
                log.info(f" + Trying N_m3u8DL-RE as fallback for subtitle")
                
                # Buscar N_m3u8DL-RE
                project_root = Path("C:/DRMLab")
                bins_path = project_root / "binaries"
                
                executable = None
                if bins_path.exists():
                    for exe_name in ["N_m3u8DL-RE.exe", "RE.exe", "N_m3u8DL-RE", "RE"]:
                        exe_path = bins_path / exe_name
                        if exe_path.exists():
                            executable = str(exe_path)
                            break
                
                if not executable:
                    executable = shutil.which("N_m3u8DL-RE")
                    if not executable:
                        executable = shutil.which("RE")
                
                if executable:
                    mpd_url = getattr(self, "manifest_url", None)
                    if mpd_url:
                        out_dir = Path(out)
                        out_dir.mkdir(parents=True, exist_ok=True)
                        
                        cmd = [
                            executable, mpd_url,
                            "--save-name", re_name,
                            "--save-dir", str(out_dir),
                            "--tmp-dir", str(out_dir),
                            "--auto-subtitle-fix", "True",
                            "--log-level", "INFO",
                        ]
                        
                        if hasattr(self, "mpd_representation_id") and self.mpd_representation_id:
                            cmd += ["--select-subtitle", f"id={self.mpd_representation_id}"]
                        elif self.language:
                            cmd += ["--select-subtitle", f"lang={self.language}"]
                        
                        if self.needs_proxy and proxy:
                            cmd += ["--custom-proxy", proxy]
                        else:
                            cmd += ["--use-system-proxy", "False"]
                        
                        try:
                            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                            
                            if result.returncode == 0:
                                downloaded_files = list(out_dir.rglob(f"{re_name}*"))
                                if downloaded_files:
                                    largest_file = max(downloaded_files, key=lambda f: f.stat().st_size)
                                    self._location = str(largest_file)
                                    log.info(f" + N_m3u8DL-RE downloaded: {os.path.basename(self._location)}")
                        except Exception as e:
                            log.warning(f" - N_m3u8DL-RE failed: {e}")
            
            if not self._location or not os.path.exists(self._location):
                raise RuntimeError(f"Failed to download subtitle for {re_name}")
            
            self.codec = "vtt"
            
            # Convert to SRT if needed
            if self.codec and self.codec.lower() in ["wvtt", "stpp", "vtt", "webvtt", "ttml", "dfxp"]:
                try:
                    converted_path = self.convert_subtitle_to_srt(self._location, strip_sdh=True)
                    if converted_path and converted_path != self._location:
                        self._location = converted_path
                        self.codec = "srt"
                        log.info(f" + Subtitle converted to SRT: {os.path.basename(converted_path)}")
                except Exception as e:
                    log.warning(f" - Subtitle conversion failed: {e}")
            
            # Verify file exists
            if self._location and os.path.exists(self._location):
                log.debug(f" + Subtitle file verified: {self._location} ({os.path.getsize(self._location)} bytes)")
            else:
                log.warning(f" - Subtitle file missing: {self._location}")
            
            return self._location
        
        # ===== Normal download for subtitles (non-HBOMax) =====
        # Determine output filename
        if isinstance(name, (list, tuple)):
            name = "_".join(map(str, name))
        
        save_path = os.path.join(out, re_name + ".vtt")
        
        # Download the subtitle
        from fuckdl.utils.io import aria2c
        import asyncio
        
        asyncio.run(aria2c(
            self.url,
            save_path,
            headers if not self.source in ["ATVP", "iT"] else {},
            proxy if self.needs_proxy else None
        ))
        
        if os.path.getsize(save_path) <= 3:
            raise IOError("Download failed, the downloaded file is empty.")
        
        self._location = save_path
        
        # Convert to SRT
        if self.codec and self.codec.lower() in ["wvtt", "stpp", "vtt", "webvtt", "ttml", "dfxp"]:
            converted_path = self.convert_subtitle_to_srt(save_path, strip_sdh=True)
            if converted_path and converted_path != save_path:
                self._location = converted_path
                self.codec = "srt"
                log.info(f" + Subtitle converted to SRT: {os.path.basename(converted_path)}")
        
        # Verify file exists
        if self._location and os.path.exists(self._location):
            log.debug(f" + Subtitle file verified: {self._location} ({os.path.getsize(self._location)} bytes)")
        else:
            log.warning(f" - Subtitle file missing: {self._location}")
        
        return self._location

    def __str__(self):
        codec = next((CODEC_MAP[x] for x in CODEC_MAP if (self.codec or "").startswith(x)), self.codec)
        
        track_name = self.get_track_name() or ""
        
        if self.forced:
            if track_name:
                track_name = f"{track_name} (Forced)"
            else:
                track_name = "Forced"
        elif self.sdh:
            if track_name:
                track_name = f"{track_name} (SDH)"
            else:
                track_name = "SDH"
        elif self.cc:
            if track_name:
                track_name = f"{track_name} (CC)"
            else:
                track_name = "CC"
        
        if self.is_original_lang:
            if track_name:
                track_name = f"{track_name} [Original]"
            else:
                track_name = "[Original]"
        
        display_parts = [
            "â”œâ”€ SUB",
            f"[{codec}]",
            f"{self.language}"
        ]
        
        if track_name:
            display_parts.append(track_name)
        
        return " | ".join(display_parts)


class MenuTrack:
    line_1 = re.compile(r"^CHAPTER(?P<number>\d+)=(?P<timecode>[\d\\.]+)$")
    line_2 = re.compile(r"^CHAPTER(?P<number>\d+)NAME=(?P<title>[\d\\.]+)$")

    def __init__(self, number, title, timecode):
        self.id = f"chapter-{number}"
        self.number = number
        self.title = title
        if "." not in timecode:
            timecode += ".000"
        self.timecode = timecode

    def __bool__(self):
        return bool(self.number and self.number >= 0 and self.title and self.timecode)

    def __repr__(self):
        return "CHAPTER{num}={time}\nCHAPTER{num}NAME={name}".format(
            num=f"{self.number:02}",
            time=self.timecode,
            name=self.title
        )

    def __str__(self):
        return " | ".join([
            "â”œâ”€ CHP",
            f"[{self.number:02}]",
            self.timecode,
            self.title
        ])

    @classmethod
    def loads(cls, data):
        lines = [x.strip() for x in data.strip().splitlines(keepends=False)]
        if len(lines) > 2:
            return MenuTrack.loads("\n".join(lines))
        one, two = lines

        one_m = cls.line_1.match(one)
        two_m = cls.line_2.match(two)
        if not one_m or not two_m:
            raise SyntaxError(f"An unexpected syntax error near:\n{one}\n{two}")

        one_str, timecode = one_m.groups()
        two_str, title = two_m.groups()
        one_num, two_num = int(one_str.lstrip("0")), int(two_str.lstrip("0"))

        if one_num != two_num:
            raise SyntaxError(f"The chapter numbers ({one_num},{two_num}) does not match.")
        if not timecode:
            raise SyntaxError("The timecode is missing.")
        if not title:
            raise SyntaxError("The title is missing.")

        return cls(number=one_num, title=title, timecode=timecode)

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as fd:
            return cls.loads(fd.read())

    def dumps(self):
        return repr(self)

    def dump(self, path):
        with open(path, "w", encoding="utf-8") as fd:
            return fd.write(self.dumps())

    @staticmethod
    def format_duration(seconds):
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02.0f}:{minutes:02.0f}:{seconds:06.3f}"


class Tracks:
    TRACK_ORDER_MAP = {
        VideoTrack: 0,
        AudioTrack: 1,
        TextTrack: 2,
        MenuTrack: 3
    }

    def __init__(self, *args):
        self.videos = []
        self.audios = []
        self.subtitles = []
        self.chapters = []

        if args:
            self.add(as_list(*args))

    def __iter__(self):
        return iter(as_list(self.videos, self.audios, self.subtitles))

    def __repr__(self):
        return "{name}({items})".format(
            name=self.__class__.__name__,
            items=", ".join([f"{k}={repr(v)}" for k, v in self.__dict__.items()])
        )

    def __str__(self):
        rep = ""
        last_track_type = None
        tracks = [*list(self), *self.chapters]
        for track in sorted(tracks, key=lambda t: self.TRACK_ORDER_MAP[type(t)]):
            if type(track) != last_track_type:
                last_track_type = type(track)
                count = sum(type(x) is type(track) for x in tracks)
                rep += "{count} {type} Track{plural}{colon}\n".format(
                    count=count,
                    type=track.__class__.__name__.replace("Track", ""),
                    plural="s" if count != 1 else "",
                    colon=":" if count > 0 else ""
                )
            rep += f"{track}\n"

        return rep.rstrip()

    def exists(self, by_id=None, by_url=None):
        if by_id:
            return any(x.id == by_id for x in self)
        if by_url:
            return any(x.url == by_url for x in self)
        return False

    def add(self, tracks, warn_only=True):
        if isinstance(tracks, Tracks):
            tracks = [*list(tracks), *tracks.chapters]

        existing_ids = {track.id for track in self}

        duplicates = 0
        for track in as_list(tracks):
            if track.id in existing_ids:
                if not warn_only:
                    raise ValueError(
                        "One or more of the provided Tracks is a duplicate. "
                        "Track IDs must be unique but accurate using static values."
                    )
                duplicates += 1
                continue

            existing_ids.add(track.id)

            if isinstance(track, VideoTrack):
                self.videos.append(track)
            elif isinstance(track, AudioTrack):
                self.audios.append(track)
            elif isinstance(track, TextTrack):
                self.subtitles.append(track)
            elif isinstance(track, MenuTrack):
                self.chapters.append(track)
            else:
                raise ValueError("Track type was not set or is invalid.")

        log = logging.getLogger("Tracks")

        if duplicates:
            log.warning(f" - Found and skipped {duplicates} duplicate tracks")

    def print(self, level=logging.INFO):
        log = logging.getLogger("Tracks")
        for line in str(self).splitlines(keepends=False):
            log.log(level, line)

    def sort_videos(self, by_language=None):
        if not self.videos:
            return
        self.videos = sorted(self.videos, key=lambda x: float(x.bitrate or 0.0), reverse=True)
        for language in reversed(by_language or []):
            if str(language) == "all":
                language = next((x.language for x in self.videos if x.is_original_lang), "")
            if not language:
                continue
            self.videos = sorted(
                self.videos,
                key=lambda x: "" if is_close_match(language, [x.language]) else str(x.language)
            )

    def sort_audios(self, by_language=None):
        if not self.audios:
            return
        self.audios = sorted(self.audios, key=lambda x: float(x.bitrate or 0.0), reverse=True)
        self.audios = sorted(self.audios, key=lambda x: float(x.channels.replace("ch", "").replace("/JOC", "") if x.channels is not None else 0.0), reverse=True)
        self.audios = sorted(self.audios, key=lambda x: str(x.language) if x.descriptive else "")
        for language in reversed(by_language or []):
            if str(language) == "all":
                language = next((x.language for x in self.audios if x.is_original_lang), "")
            if not language:
                continue
            self.audios = sorted(
                self.audios,
                key=lambda x: "" if is_close_match(language, [x.language]) else str(x.language)
            )

    def sort_subtitles(self, by_language=None):
        if not self.subtitles:
            return
        self.subtitles = sorted(
            self.subtitles, key=lambda x: str(x.language) + ("-cc" if x.cc else "") + ("-sdh" if x.sdh else "")
        )
        self.subtitles = sorted(self.subtitles, key=lambda x: not x.forced)
        for language in reversed(by_language or []):
            if str(language) == "all":
                language = next((x.language for x in self.subtitles if x.is_original_lang), "")
            if not language:
                continue
            self.subtitles = sorted(
                self.subtitles,
                key=lambda x: "" if is_close_match(language, [x.language]) else str(x.language)
            )

    def sort_chapters(self):
        if not self.chapters:
            return
        self.chapters = sorted(self.chapters, key=lambda x: x.number)

    @staticmethod
    def select_by_language(languages, tracks, one_per_lang=True):
        if not tracks:
            return
        if "all" not in languages:
            track_type = tracks[0].__class__.__name__.lower().replace("track", "").replace("text", "subtitle")
            orig_tracks = tracks
            tracks = [
                x for x in tracks
                if is_close_match(x.language, languages) or (x.is_original_lang and "orig" in languages)
            ]
            if not tracks:
                if languages == ["orig"]:
                    all_languages = set(x.language for x in orig_tracks)
                    if len(all_languages) == 1:
                        languages = list(all_languages)
                        tracks = [
                            x for x in orig_tracks
                            if is_close_match(x.language, languages) or (x.is_original_lang and "orig" in languages)
                        ]
                    else:
                        raise ValueError(
                            f"There's no original {track_type} track. Please specify a language manually with "
                            f"{'-al' if track_type == 'audio' else '-sl'}."
                        )
                else:
                    raise ValueError(
                        f"There's no {track_type} tracks that match the language{'' if len(languages) == 1 else 's'}: "
                        f"{', '.join(languages)}"
                    )
        if one_per_lang:
            if "all" in languages:
                languages = list(sorted(set(x.language for x in tracks), key=str))
            for language in languages:
                if language == "orig":
                    yield next(x for x in tracks if x.is_original_lang)
                else:
                    match = get_closest_match(language, [x.language for x in tracks])
                    if match:
                        yield next(x for x in tracks if x.language == match)
        else:
            for track in tracks:
                yield track

    def select_videos(self, by_language=None, by_vbitrate=None, by_quality=None, by_worst=None, by_range=None,
                      one_only: bool = True, by_codec=None) -> None:
        import logging
        log = logging.getLogger("Tracks")

        if by_quality:
            try:
                target_height = int(by_quality)
            except (ValueError, TypeError):
                target_height = None

            if target_height:
                videos_quality = [x for x in self.videos if x.height == target_height]

                if not videos_quality and self.videos:
                    closest_tracks = sorted(
                        self.videos,
                        key=lambda x: abs(x.height - target_height)
                    )

                    if closest_tracks:
                        min_diff = abs(closest_tracks[0].height - target_height)
                        videos_quality = [
                            x for x in self.videos
                            if abs(x.height - target_height) == min_diff
                        ]

                        log.info(f" + No exact {target_height}p match found. Using closest available: {videos_quality[0].height}p")

                if not videos_quality:
                    raise ValueError(f"There's no video track close to {by_quality}p resolution. Aborting.")
                self.videos = videos_quality

        if by_vbitrate:
            self.videos = [x for x in self.videos if int(x.bitrate) <= int(by_vbitrate * 1001)]
        if by_worst:
            self.videos = sorted(self.videos, key=lambda x: float(x.bitrate or 0.0), reverse=False)
        if by_range:
            self.videos = [x for x in self.videos if {
                "HDR10": x.hdr10,
                "HLG": x.hlg,
                "DV": x.dv,
                "SDR": not x.hdr10 and not x.dv
            }.get((by_range or "").upper(), True)]
            if not self.videos:
                raise ValueError(f"There's no {by_range} video track. Aborting.")
        if by_language:
            self.videos = list(self.select_by_language(by_language, self.videos))
        if one_only and self.videos:
            self.videos = sorted(self.videos, key=lambda x: float(x.bitrate or 0.0), reverse=True)
            self.videos = [self.videos[0]]

    def select_videos_multi(self, ranges: list[str], by_quality=None, by_vbitrate=None) -> None:
        selected = []
        for r in ranges:
            temp = Tracks()
            temp.videos = self.videos.copy()
            temp.select_videos(by_range=r, by_quality=by_quality, one_only=False)
            if by_vbitrate:
                temp.videos = [x for x in temp.videos if int(x.bitrate) <= int(by_vbitrate * 1001)]
            if temp.videos:
                best = max(temp.videos, key=lambda x: x.bitrate)
                selected.append(best)
        unique = {}
        for v in selected:
            key = (v.width, v.height, v.codec, v.hdr10, v.dv)
            if key not in unique or v.bitrate > unique[key].bitrate:
                unique[key] = v
        self.videos = list(unique.values())

    def select_audios(
        self,
        with_descriptive: bool = True,
        with_atmos: bool = False,
        by_language=None,
        by_bitrate=None,
        by_channels=None,
        by_codec=None,
        should_fallback: bool = False
    ) -> None:
        if not with_descriptive:
            self.audios = [x for x in self.audios if not x.descriptive]

        if by_codec:
            codec_audio = list(filter(lambda x: by_codec.lower() in (x.codec or "").lower(), self.audios))
            if not codec_audio and not should_fallback:
                raise ValueError(f"There's no {by_codec} audio tracks. Aborting.")
            else:
                self.audios = (codec_audio if codec_audio else self.audios)

        if by_channels:
            channels_audio = list(filter(lambda x: x.channels == by_channels, self.audios))
            if not channels_audio and not should_fallback:
                raise ValueError(f"There's no {by_channels} audio tracks. Aborting.")
            else:
                self.audios = (channels_audio if channels_audio else self.audios)

        if with_atmos:
            atmos_audio = list(filter(lambda x: x.atmos, self.audios))
            self.audios = (atmos_audio if atmos_audio else self.audios)

        if by_bitrate:
            self.audios = [x for x in self.audios if int(x.bitrate) <= int(by_bitrate * 1000)]

        if by_language:
            normal_audios = [x for x in self.audios if not x.descriptive]
            desc_audios = [x for x in self.audios if x.descriptive]

            selected = list(self.select_by_language(by_language, normal_audios))
            selected += list(self.select_by_language(by_language, desc_audios))

            self.audios = selected

    def select_subtitles(self, by_language=None, with_cc=True, with_sdh=True, with_forced=True):
        if not with_cc:
            self.subtitles = [x for x in self.subtitles if not x.cc]
        if not with_sdh:
            self.subtitles = [x for x in self.subtitles if not x.sdh]
        if isinstance(with_forced, list):
            self.subtitles = [
                x for x in self.subtitles
                if not x.forced or is_close_match(x.language, with_forced)
            ]
        if not with_forced:
            self.subtitles = [x for x in self.subtitles if not x.forced]
        if by_language:
            self.subtitles = list(self.select_by_language(by_language, self.subtitles, one_per_lang=False))

    def export_chapters(self, to_file=None):
        self.sort_chapters()
        data = "\n".join(map(repr, self.chapters))
        if to_file:
            os.makedirs(os.path.dirname(to_file), exist_ok=True)
            with open(to_file, "w", encoding="utf-8") as fd:
                fd.write(data)
        return data

    @staticmethod
    def from_m3u8(*args, **kwargs):
        from fuckdl import parsers
        return parsers.m3u8.parse(*args, **kwargs)

    @staticmethod
    def from_mpd(*args, **kwargs):
        from fuckdl import parsers
        return parsers.mpd.parse(**kwargs)

    @staticmethod
    def from_ism(*args, **kwargs):
        from fuckdl import parsers
        return parsers.ism.parse(**kwargs)

    def make_hybrid(self) -> str:
        import time
        from pathlib import Path

        start_time = time.time()
        log = logging.getLogger("Hybrid")
        log.info(" + Processing to Hybrid")

        hdr = next((t for t in self.videos if t.hdr10 and not t.dv), None)
        dv = next((t for t in self.videos if t.dv and not t.hdr10), None)

        if not hdr or not dv:
            raise ValueError("Hybrid failed: Could not find valid pair of HDR10 and DV tracks.")

        hdr_path = Path(hdr.locate()).resolve()
        dv_path = Path(dv.locate()).resolve()

        hybrid_output = hdr_path.with_name(f"{hdr_path.stem}_hybrid.hevc")

        try:
            hybrid_file = self.make_hybrid_dv_hdr(
                dv_file=dv_path,
                hdr_file=hdr_path,
                output_file=hybrid_output
            )

            hybrid_path = Path(hybrid_file)
            if not hybrid_path.exists() or hybrid_path.stat().st_size < 1000:
                raise FileNotFoundError(f"Hybrid file creation failed or file is empty: {hybrid_path}")

            hdr._location = str(hybrid_path)

            if dv in self.videos:
                self.videos.remove(dv)

            try:
                if hdr_path.exists() and hdr_path != hybrid_path:
                    hdr_path.unlink()
                if dv_path.exists():
                    dv_path.unlink()
            except Exception as e:
                log.warning(f" - Failed to delete source files after hybrid creation: {e}")

        except Exception as e:
            log.error(f" - Hybrid creation failed: {e}")
            raise

        end_time = time.time()
        duration = format_duration(end_time - start_time)
        log.info(f" + Finish processing Hybrid in {duration}!")

        return str(hdr._location)

    def make_hybrid_dv_hdr(self, dv_file: Path, hdr_file: Path, output_file: Path) -> str:
        dovi_tool = shutil.which("dovi_tool")
        if not dovi_tool:
            dovi_tool = shutil.which("dovi_tool", path="./binaries")

        if not dovi_tool:
            raise EnvironmentError("dovi_tool executable not found in PATH or ./binaries/")

        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise EnvironmentError("ffmpeg executable not found.")

        rpu_file = output_file.with_name("RPU.bin")

        temp_files_to_clean = [rpu_file]

        dv_input = dv_file
        hdr_input = hdr_file

        try:
            if dv_file.suffix.lower() != ".hevc":
                dv_raw = dv_file.with_suffix(".temp_dv.hevc")
                temp_files_to_clean.append(dv_raw)
                subprocess.run([
                    ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                    "-i", str(dv_file),
                    "-c:v", "copy", "-bsf:v", "hevc_mp4toannexb",
                    "-f", "hevc", str(dv_raw)
                ], check=True)
                dv_input = dv_raw

            if hdr_file.suffix.lower() != ".hevc":
                hdr_raw = hdr_file.with_suffix(".temp_hdr.hevc")
                temp_files_to_clean.append(hdr_raw)
                subprocess.run([
                    ffmpeg, "-y", "-hide_banner", "-loglevel", "error",
                    "-i", str(hdr_file),
                    "-c:v", "copy", "-bsf:v", "hevc_mp4toannexb",
                    "-f", "hevc", str(hdr_raw)
                ], check=True)
                hdr_input = hdr_raw

            subprocess.run([
                dovi_tool, "extract-rpu",
                "-i", str(dv_input),
                "-o", str(rpu_file)
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

            subprocess.run([
                dovi_tool, "inject-rpu",
                "-i", str(hdr_input),
                "-r", str(rpu_file),
                "-o", str(output_file)
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Error running dovi_tool/ffmpeg: {e}")
        finally:
            for temp in temp_files_to_clean:
                if temp.exists():
                    try:
                        temp.unlink()
                    except OSError:
                        pass

        return str(output_file)

    def mux(self, prefix):
        if self.videos:
            muxed_location = self.videos[0].locate()
            if not muxed_location:
                raise ValueError("The provided video track has not yet been downloaded.")
            muxed_location = os.path.splitext(muxed_location)[0] + ".muxed.mkv"
        elif self.audios:
            muxed_location = self.audios[0].locate()
            if not muxed_location:
                raise ValueError("A provided audio track has not yet been downloaded.")
            muxed_location = os.path.splitext(muxed_location)[0] + ".muxed.mka"
        elif self.subtitles:
            muxed_location = self.subtitles[0].locate()
            if not muxed_location:
                raise ValueError("A provided subtitle track has not yet been downloaded.")
            muxed_location = os.path.splitext(muxed_location)[0] + ".muxed.mks"
        elif self.chapters:
            muxed_location = config.filenames.chapters.format(filename=prefix)
            if not muxed_location:
                raise ValueError("A provided chapter has not yet been downloaded.")
            muxed_location = os.path.splitext(muxed_location)[0] + ".muxed.mks"
        else:
            raise ValueError("No tracks provided, at least one track must be provided.")

        muxed_location = os.path.join(config.directories.downloads, os.path.basename(muxed_location))

        cl = [
            "mkvmerge",
            "--output",
            muxed_location
        ]

        for i, vt in enumerate(self.videos):
            location = vt.locate()
            if not location:
                raise ValueError("Somehow a Video Track was not downloaded before muxing...")
            cl.extend([
                "--language", "0:und",
                "--disable-language-ietf",
                "--default-track", f"0:{i == 0}",
                "--compression", "0:none",
                "(", location, ")"
            ])

        for i, at in enumerate(self.audios):
            location = at.locate()
            if not location:
                raise ValueError("Somehow an Audio Track was not downloaded before muxing...")
            # Get display name for audio track (handles Atmos, DD+, etc.)
            audio_display = at.get_codec_display()
            if at.atmos and "Atmos" not in audio_display:
                audio_display = f"{audio_display} Atmos"
            
            cl.extend([
                "--track-name", f"0:{at.get_track_name() or audio_display}",
                "--language", "0:{}".format(LANGUAGE_MUX_MAP.get(
                    str(at.language), at.language.to_alpha3()
                )),
                "--disable-language-ietf",
                "--default-track", f"0:{i == 0}",
                "--compression", "0:none",
                "(", location, ")"
            ])

        for st in self.subtitles:
            location = st.locate()
            if not location:
                raise ValueError("Somehow a Text Track was not downloaded before muxing...")
            default = bool(self.audios and is_close_match(st.language, [self.audios[0].language]) and st.forced)
            cl.extend([
                "--track-name", f"0:{st.get_track_name() or ''}",
                "--language", "0:{}".format(LANGUAGE_MUX_MAP.get(
                    str(st.language), st.language.to_alpha3()
                )),
                "--disable-language-ietf",
                "--sub-charset", "0:UTF-8",
                "--forced-track", f"0:{st.forced}",
                "--default-track", f"0:{default}",
                "--compression", "0:none",
                "(", location, ")"
            ])

        if self.chapters:
            location = config.filenames.chapters.format(filename=prefix)
            self.export_chapters(location)
            cl.extend(["--chapters", location])

        p = subprocess.Popen(cl, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        in_progress = False
        for line in TextIOWrapper(p.stdout, encoding="utf-8"):
            if re.search(r"Using the (?:demultiplexer|output module) for the format", line):
                continue
            if line.startswith("Progress:"):
                in_progress = True
                sys.stdout.write("\r" + line.rstrip('\n'))
            else:
                if in_progress:
                    in_progress = False
                    sys.stdout.write("\n")
                sys.stdout.write(line)
        returncode = p.wait()
        return muxed_location, returncode