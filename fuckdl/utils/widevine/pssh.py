# -*- coding: utf-8 -*-
import base64
import uuid

from fuckdl.vendor.pymp4.parser import Box
from fuckdl.utils.xml import load_xml
from .protos import widevine_pb2 as widevine
from .cdm import Cdm


def first_or_else(iterable, default):
    return next(iter(iterable or []), None) or default


def first_or_none(iterable):
    return first_or_else(iterable, None)


def first(iterable):
    return next(iter(iterable))


def build_pssh(*, kid=None, init_data=None):
    if not (bool(kid) ^ bool(init_data)):
        raise ValueError("Exactly one of kid or init_data must be provided")

    if kid:
        # Crear WidevineCencHeader con el KID
        cenc_header = widevine.WidevineCencHeader()
        cenc_header.algorithm = widevine.WidevineCencHeader.Algorithm.AESCTR
        cenc_header.key_id.append(kid)
        init_data = cenc_header.SerializeToString()

    return Box.parse(Box.build({
        "type": b"pssh",
        "version": 0,
        "flags": 0,
        "system_ID": Cdm.uuid,
        "init_data": init_data,
    }))


def generate_from_kid(kid: str):
    if not kid:
        return None

    kid_uuid = uuid.UUID(kid)
    return build_pssh(kid=kid_uuid.bytes)


def generate_from_b64(pssh: str):
    if not pssh:
        return None

    return Box.parse(base64.b64decode(pssh))


def convert_playready_pssh(pssh):
    if isinstance(pssh, bytes):
        xml_str = pssh
    elif isinstance(pssh, str):
        xml_str = base64.b64decode(pssh)
    else:
        raise TypeError("PSSH must be bytes or str")

    xml_str = xml_str.decode("utf-16-le", "ignore")
    xml_str = xml_str[xml_str.index("<"):]

    xml = load_xml(xml_str).find("DATA")  # root: WRMHEADER

    kid = (
        xml.findtext("KID")  # v4.0.0.0
        or first_or_none(xml.xpath("PROTECTINFO/KID/@VALUE"))  # v4.1.0.0
        or first_or_none(xml.xpath("PROTECTINFO/KIDS/KID/@VALUE"))  # v4.3.0.0
    )
    
    if not kid:
        raise ValueError("No KID found in PlayReady PSSH")

    init_data = widevine.WidevineCencHeader()
    init_data.algorithm = widevine.WidevineCencHeader.Algorithm.AESCTR
    kid_bytes = base64.b64decode(kid)
    kid_uuid = uuid.UUID(bytes_le=kid_bytes)
    init_data.key_id.append(kid_uuid.bytes)

    return build_pssh(init_data=init_data.SerializeToString()), kid_uuid.hex