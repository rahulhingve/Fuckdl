import base64
import html
import json
import logging
import os
import shutil
import subprocess
import sys
import traceback
from zlib import crc32
from pathlib import Path
from datetime import datetime, timedelta
from http.cookiejar import MozillaCookieJar
import random
import time
import click
import re
import requests
from appdirs import AppDirs
from langcodes import Language
from pymediainfo import MediaInfo
from Crypto.Random import get_random_bytes
import math

from fuckdl import services
from fuckdl.config import Config, config, credentials, directories, filenames
from fuckdl.objects import AudioTrack, Credential, TextTrack, Title, Titles, VideoTrack, Track
from fuckdl.objects.vaults import InsertResult, Vault, Vaults
from fuckdl.utils import Cdm as LocalWidevineCdm, is_close_match
from fuckdl.utils.click import (AliasedGroup, ContextData, acodec_param, language_param, quality_param,
                                          range_param, vcodec_param, wanted_param, channels_param)
from fuckdl.utils.collections import as_list, merge_dict
from fuckdl.utils.io import load_yaml
from fuckdl.utils.widevine.device import LocalDevice, RemoteDevice
from pyplayready.cdm import Cdm as PlayReadyCdm
from pyplayready.device import Device as PlayReadyDevice
from pyplayready.system.pssh import PSSH as PlayReadyPSSH
from pyplayready.crypto.ecc_key import ECCKey
from pyplayready.system.bcert import CertificateChain, Certificate
from fuckdl.vendor.pymp4.parser import Box
from fuckdl.utils.monalisa import MonaLisa, MonaLisaCDM
try:
    from pywidevine.device import Device as PyWidevineDevice
    from pywidevine.cdm import Cdm as PyWidevineCdm
    PY_WIDEVINE_AVAILABLE = True
except ImportError:
    PY_WIDEVINE_AVAILABLE = False
    PyWidevineDevice = None
    PyWidevineCdm = None


def reprovision_device(prd_path: Path) -> None:
    """
    Reprovision a PlayReady Device (.prd) by creating a new leaf certificate and new encryption/signing keys.
    Will override the device if an output path or directory is not specified.

    Only works on PRD Devices of v3 or higher.
    """
    if not prd_path.is_file():
        raise Exception("prd_path: Not a path to a file, or it doesn't exist.")

    device = PlayReadyDevice.load(prd_path)

    if device.group_key is None:
        raise Exception("Device does not support reprovisioning, re-create it or use a Device with a version of 3 or higher")

    device.group_certificate.remove(0)

    encryption_key = ECCKey.generate()
    signing_key = ECCKey.generate()

    device.encryption_key = encryption_key
    device.signing_key = signing_key

    new_certificate = Certificate.new_leaf_cert(
        cert_id=get_random_bytes(16),
        security_level=device.group_certificate.get_security_level(),
        client_id=get_random_bytes(16),
        signing_key=signing_key,
        encryption_key=encryption_key,
        group_key=device.group_key,
        parent=device.group_certificate
    )
    device.group_certificate.prepend(new_certificate)

    prd_path.parent.mkdir(parents=True, exist_ok=True)
    prd_path.write_bytes(device.dumps())


def create_playready_device(device_dir, force_refresh=False):
    """Create a PlayReady Device (.prd) file from an ECC private group key and group certificate chain."""
    group_key = device_dir / 'zgpriv.dat'
    group_certificate = device_dir / 'bgroupcert.dat'
    infofile = device_dir / 'PR.json'

    if not group_key.is_file():
        raise TypeError("group_key: Not a path to a file, or it doesn't exist.")
    if not group_certificate.is_file():
        raise TypeError("group_certificate: Not a path to a file, or it doesn't exist.")

    if force_refresh:
        if infofile.is_file():
            infofile.unlink()
        device_files = list(device_dir.glob("*.prd"))
        for device_file in device_files:
            device_file.unlink()
        logging.warning(" + Forcing device refresh...")
    elif infofile.is_file():
        with open(infofile, 'r') as file:
            info = json.loads(file.read())
        if "device" in info:
            device_prd = Path(device_dir / info["device"])
            if device_prd.is_file():
                logging.info(" + Loading existing device: %s", info["device"])
                return device_prd
        else:
            infofile.unlink()
            logging.warning(" + Invalid info file, refreshing device...")

    encryption_key = ECCKey.generate()
    signing_key = ECCKey.generate()

    group_key_obj = ECCKey.load(group_key)
    certificate_chain = CertificateChain.load(group_certificate)

    new_certificate = Certificate.new_leaf_cert(
        cert_id=get_random_bytes(16),
        security_level=certificate_chain.get_security_level(),
        client_id=get_random_bytes(16),
        signing_key=signing_key,
        encryption_key=encryption_key,
        group_key=group_key_obj,
        parent=certificate_chain
    )
    certificate_chain.prepend(new_certificate)

    device = PlayReadyDevice(
        group_key=group_key_obj.dumps(),
        encryption_key=encryption_key.dumps(),
        signing_key=signing_key.dumps(),
        group_certificate=certificate_chain.dumps(),
    )

    expiry = (datetime.now() + timedelta(days=3650)).isoformat()
    prd_bin = device.dumps()
    out_path = device_dir / f"{device.get_name()}_{crc32(prd_bin).to_bytes(4, 'big').hex()}.prd"

    if out_path.exists():
        logging.error(f"A file already exists at the path '{out_path}', cannot overwrite.")
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(device.dumps())

    logging.info(" + Created PlayReady Device (.prd) file, %s", out_path.name)
    logging.info(" + Security Level: %s", device.security_level)
    logging.info(" + Device expiry: %s (10 years from now)", expiry)

    with open(infofile, 'w') as file:
        json.dump({
            "expiry": expiry,
            "device": out_path.name,
            "SecurityLevel": device.security_level,
            "created": datetime.now().isoformat()
        }, file)

    return out_path


def get_cdm(service, profile=None, cdm_name=None):
    """
    Get CDM Device (either remote or local) for a specified service.
    Raises a ValueError if there's a problem getting a CDM.
    """
    if not cdm_name:
        cdm_name = config.cdm.get(service) or config.cdm.get("default")
    if not cdm_name:
        raise ValueError("A CDM to use wasn't listed in the fuckdl.yml config")
    if isinstance(cdm_name, dict):
        if not profile:
            raise ValueError("CDM config is mapped for profiles, but no profile was chosen")
        cdm_name = cdm_name.get(profile) or config.cdm.get("default")
        if not cdm_name:
            raise ValueError(f"A CDM to use was not mapped for the profile {profile}")

    try:
        device_dir = Path(directories.devices) / cdm_name

        is_amazon_us = service in ["AMZN", "amazon"]
        is_sl3000 = "sl3000" in cdm_name.lower() or "hisense" in cdm_name.lower()

        if device_dir.is_dir() and (device_dir / 'zgpriv.dat').is_file() and (device_dir / 'bgroupcert.dat').is_file():
            if is_amazon_us and is_sl3000:
                prd_files = list(device_dir.glob("*.prd"))
                if prd_files:
                    latest_prd = max(prd_files, key=lambda x: x.stat().st_mtime)
                    if (int(time.time()) - int(os.path.getmtime(latest_prd))) > 172800:
                        try:
                            reprovision_device(latest_prd)
                            logging.info(f" + Reprovisioned PlayReady Device (.prd) file, {latest_prd.name}")
                        except Exception as e:
                            logging.warning(f"Reprovision Failed - {e}")
                    logging.info(f" + Using existing PlayReady device for Amazon US: {latest_prd.name}")
                    return PlayReadyDevice.load(latest_prd)

            device_path = create_playready_device(device_dir)
            if device_path:
                return PlayReadyDevice.load(device_path)

        # Check if it's a direct path to a PlayReady device file
        if "sl2000" in cdm_name.lower() or "sl3000" in cdm_name.lower():
            prd_path = os.path.join(directories.devices, f"{cdm_name}.prd")
            if os.path.exists(prd_path):
                if (int(time.time()) - int(os.path.getmtime(prd_path))) > 172800:
                    try:
                        reprovision_device(Path(prd_path))
                        logging.info(f" + Reprovisioned PlayReady Device (.prd) file, {cdm_name}")
                    except Exception as e:
                        logging.warning(f"Reprovision Failed - {e}")
                return PlayReadyDevice.load(prd_path)
            else:
                if device_dir.is_dir():
                    device_path = create_playready_device(device_dir)
                    if device_path:
                        return PlayReadyDevice.load(device_path)

        wvd_path = os.path.join(directories.devices, f"{cdm_name}.wvd")

        # Try pywidevine first
        if PY_WIDEVINE_AVAILABLE and os.path.exists(wvd_path):
            try:
                device = PyWidevineDevice.load(wvd_path)
                logging.debug(f" + Loaded with pywidevine: {cdm_name}")
                return device
            except Exception as e:
                logging.warning(f" - pywidevine failed, falling back to local: {e}")

        # Fallback to local implementation
        if os.path.exists(wvd_path):
            return LocalDevice.load(wvd_path)
        else:
            raise FileNotFoundError(f"Device file not found: {wvd_path}")

    except FileNotFoundError:
        dirs = [
            os.path.join(directories.devices, cdm_name),
            os.path.join(AppDirs("pywidevine", False).user_data_dir, "devices", cdm_name),
            os.path.join(AppDirs("pywidevine", False).site_data_dir, "devices", cdm_name),
        ]

        for d in dirs:
            try:
                # Try pywidevine first
                if PY_WIDEVINE_AVAILABLE:
                    try:
                        return PyWidevineDevice.from_dir(d)
                    except:
                        pass
                # Fallback to local
                return LocalDevice.from_dir(d)
            except FileNotFoundError:
                pass

        cdm_api = next(iter(x for x in config.cdm_api if x["name"] == cdm_name), None)
        if cdm_api:
            return RemoteDevice(**cdm_api)

        raise ValueError(f"Device {cdm_name!r} not found")

def get_service_config(service):
    """Get both service config and service secrets as one merged dictionary."""
    service_config = load_yaml(filenames.service_config.format(service=service.lower()))

    user_config = (load_yaml(filenames.user_service_config.format(service=service.lower()))
                   or load_yaml(filenames.user_service_config.format(service=service)))

    if user_config:
        merge_dict(service_config, user_config)

    return service_config


def get_profile(service):
    """Get the default profile for a service from the config."""
    profile = config.profiles.get(service)
    if profile is False:
        return None  # auth-less service if `false` in config
    if not profile:
        profile = config.profiles.get("default")
    if not profile:
        raise ValueError(f"No profile has been defined for '{service}' in the config.")

    return profile


def get_cookie_jar(service, profile):
    """Get the profile's cookies if available."""
    cookie_file = os.path.join(directories.cookies, service.lower(), f"{profile}.txt")
    if not os.path.isfile(cookie_file):
        cookie_file = os.path.join(directories.cookies, service, f"{profile}.txt")
    if os.path.isfile(cookie_file):
        cookie_jar = MozillaCookieJar(cookie_file)
        with open(cookie_file, "r+", encoding="utf-8") as fd:
            unescaped = html.unescape(fd.read())
            fd.seek(0)
            fd.truncate(0)
            fd.write(unescaped)
        cookie_jar.load(ignore_discard=True, ignore_expires=True)
        return cookie_jar
    return None


def get_credentials(service, profile="default"):
    """Get the profile's credentials if available."""
    cred = credentials.get(service, {})

    if isinstance(cred, dict):
        cred = cred.get(profile)
    elif profile != "default":
        return None

    if cred:
        if isinstance(cred, list):
            return Credential(*cred)
        else:
            return Credential.loads(cred)


@click.group(name="dl", short_help="Download from a service.", cls=AliasedGroup, context_settings=dict(
    help_option_names=["-?", "-h", "--help"],
    max_content_width=116,
    default_map=config.arguments
))
@click.option("--debug", is_flag=True, hidden=True)
@click.option("-p", "--profile", type=str, default=None,
              help="Profile to use when multiple profiles are defined for a service.")
@click.option("-q", "--quality", callback=quality_param, default=None,
              help="Download Resolution, defaults to best available.")
@click.option("-v", "--vcodec", callback=vcodec_param, default=None,
              help="Video Codec, defaults to H264.")
@click.option("-a", "--acodec", callback=acodec_param, default=None,
              help="Audio Codec")
@click.option("-vb", "--vbitrate", "vbitrate", type=int, default=None,
              help="Video Bitrate, defaults to Max.")
@click.option("-ab", "--abitrate", "abitrate", type=int, default=None,
              help="Audio Bitrate, defaults to Max.")
@click.option("-aa", "--atmos", is_flag=True, default=False,
              help="Prefer Atmos Audio")
@click.option("-ch", "--channels", callback=channels_param, default=None,
              help="Audio Channels")
@click.option("-r", "--range", "range_", callback=range_param, default=None,
              help="Video Color Range, defaults to SDR.")
@click.option("-w", "--wanted", callback=wanted_param, default=None,
              help="Wanted episodes, e.g. `S01-S05,S07`, `S01E01-S02E03`, `S02-S02E03`, etc., defaults to all.")
@click.option("-al", "--alang", callback=language_param, default="orig",
              help="Language wanted for audio.")
@click.option("-sl", "--slang", callback=language_param, default="all",
              help="Language wanted for subtitles.")
@click.option("--delay", type=int, default=None,
              help="Delay between title processing")
@click.option("--proxy", type=str, default=None,
              help="Proxy URI to use. If a 2-letter country is provided, it will try get a proxy from the config.")
@click.option("-A", "--audio-only", is_flag=True, default=False,
              help="Only download audio tracks.")
@click.option("-S", "--subs-only", is_flag=True, default=False,
              help="Only download subtitle tracks.")
@click.option("-C", "--chapters-only", is_flag=True, default=False,
              help="Only download chapters.")
@click.option("-ns", "--no-subs", is_flag=True, default=False,
              help="Do not download subtitle tracks.")
@click.option("-na", "--no-audio", is_flag=True, default=False,
              help="Do not download audio tracks.")
@click.option("-nv", "--no-video", is_flag=True, default=False,
              help="Do not download video tracks.")
@click.option("-nc", "--no-chapters", is_flag=True, default=False,
              help="Do not download chapters tracks.")
@click.option("-ad", "--audio-description", is_flag=True, default=False,
              help="Download audio description tracks.")
@click.option("--list", "list_", is_flag=True, default=False,
              help="Skip downloading and list available tracks and what tracks would have been downloaded.")
@click.option("--selected", is_flag=True, default=False,
              help="List selected tracks and what tracks are downloaded.")
@click.option("--cdm", type=str, default=None,
              help="Override the CDM that will be used for decryption.")
@click.option("--keys", is_flag=True, default=False,
              help="Skip downloading, retrieve the decryption keys (via CDM or Key Vaults) and print them.")
@click.option("--cache", is_flag=True, default=False,
              help="Disable the use of the CDM and only retrieve decryption keys from Key Vaults. "
                   "If a needed key is unable to be retrieved from any Key Vaults, the title is skipped.")
@click.option("--no-cache", is_flag=True, default=False,
              help="Disable the use of Key Vaults and only retrieve decryption keys from the CDM.")
@click.option("--no-proxy", is_flag=True, default=False,
              help="Force disable all proxy use.")
@click.option("--force-proxy", is_flag=True, default=False,
              help="Force using proxy even if current region matches.")
@click.option("-nm", "--no-mux", is_flag=True, default=False,
              help="Do not mux the downloaded and decrypted tracks.")
@click.option("--mux", is_flag=True, default=False,
              help="Force muxing when using --audio-only/--subs-only/--chapters-only.")
@click.option("--worst", is_flag=True, default=False,
              help="Choose the worst available video tracks rather than the best")
@click.option("-nf", "--no-forced", is_flag=True, default=False,
              help="Do not download forced subtitle tracks.")
@click.pass_context
def dl(ctx, profile, cdm, quality, vcodec, acodec, vbitrate, abitrate, atmos, channels, range_, wanted,
       alang, slang, delay, proxy, audio_only, subs_only, chapters_only, no_subs, no_audio, no_video,
       no_chapters, audio_description, list_, selected, keys, cache, no_cache, no_proxy, force_proxy,
       no_mux, mux, worst, no_forced, *_, **__):
    log = logging.getLogger("dl")

    service = ctx.params.get("service_name") or services.get_service_key(ctx.invoked_subcommand)
    if not service:
        log.exit(" - Unable to find service")

    profile = profile or get_profile(service)
    service_config = get_service_config(service)
    vaults = []
    for vault in config.key_vaults:
        try:
            vaults.append(Config.load_vault(vault))
        except Exception as e:
            log.error(f" - Failed to load vault {vault['name']!r}: {e}")
    vaults = Vaults(vaults, service=service)
    local_vaults = sum(v.type == Vault.Types.LOCAL for v in vaults)
    remote_vaults = sum(v.type == Vault.Types.REMOTE for v in vaults)
    http_vaults = sum(v.type == Vault.Types.HTTP for v in vaults)
    httpapi_vaults = sum(v.type == Vault.Types.HTTPAPI for v in vaults)
    log.info(f" + {local_vaults} Local Vault{'' if local_vaults == 1 else 's'}")
    log.info(f" + {remote_vaults} Remote Vault{'' if remote_vaults == 1 else 's'}")
    log.info(f" + {http_vaults} HTTP Vault{'' if http_vaults == 1 else 's'}")
    log.info(f" + {httpapi_vaults} HTTPAPI Vault{'' if httpapi_vaults == 1 else 's'}")

    try:
        device = get_cdm(service, profile, cdm)
    except ValueError as e:
        raise log.exit(f" - {e}")

    if isinstance(device, PlayReadyDevice):
        log.info(f" + Playready Device: {device.get_name()} (L{device.security_level})")
    elif PY_WIDEVINE_AVAILABLE and isinstance(device, PyWidevineDevice):
        log.info(f" + Widevine Device: {device.system_id} (L{device.security_level})")
    elif isinstance(device, LocalDevice):
        log.info(f" + Widevine Device: {device.system_id if device.system_id else 'No System ID'} (L{device.security_level})")
    else:
        log.info(f" + Widevine Device: Unknown (L{device.security_level})")

    # Create appropriate CDM instance
    if isinstance(device, PlayReadyDevice):
        cdm_instance = PlayReadyCdm.from_device(device)
    elif PY_WIDEVINE_AVAILABLE and isinstance(device, PyWidevineDevice):
        cdm_instance = PyWidevineCdm.from_device(device)
    else:
        cdm_instance = LocalWidevineCdm(device)

    if profile:
        cookies = get_cookie_jar(service, profile)
        credentials_obj = get_credentials(service, profile)
        if not cookies and not credentials_obj and service_config.get("needs_auth", True):
            raise log.exit(f" - Profile {profile!r} has no cookies or credentials")
    else:
        cookies = None
        credentials_obj = None

    ctx.obj = ContextData(
        config=service_config,
        vaults=vaults,
        cdm=cdm_instance,
        profile=profile,
        cookies=cookies,
        credentials=credentials_obj,
    )


@dl.result_callback()
@click.pass_context
def result(ctx, service, quality, range_, wanted, alang, slang, audio_only, subs_only, chapters_only, audio_description,
           list_, keys, cache, no_cache, no_subs, no_audio, no_video, no_chapters, atmos, vbitrate, abitrate,
           acodec, channels, no_mux, worst, mux, delay, selected, no_forced, *_, **__):

    def ccextractor():
        log.info("Extracting EIA-608 captions from stream with CCExtractor")
        track_id = f"ccextractor-{track.id}"
        cc_lang = track.language
        try:
            cc = track.ccextractor(
                track_id=track_id,
                out_path=filenames.subtitles.format(id=track_id, language_code=cc_lang),
                language=cc_lang,
                original=False,
            )
        except EnvironmentError:
            log.warning(" - CCExtractor not found, cannot extract captions")
        else:
            if cc:
                title.tracks.add(cc)
                log.info(" + Extracted")
            else:
                log.info(" + No captions found")

    log = service.log
    service_name = service.__class__.__name__

    log.info("Retrieving Titles")
    try:
        titles = Titles(as_list(service.get_titles()))
    except requests.HTTPError as e:
        log.debug(traceback.format_exc())
        raise log.exit(f" - HTTP Error {e.response.status_code}: {e.response.reason}")
    if not titles:
        raise log.exit(" - No titles returned!")
    titles.order()
    titles.print()

    first = True

    for title in titles.with_wanted(wanted):
        if not first and delay:
            d = delay + random.randint(math.floor(int(-delay/5.0)), math.floor((delay/5.0)))
            log.info(f"Delaying for {d}s before getting next title...")
            time.sleep(d)

        first = False

        if title.type == Title.Types.TV:
            log.info("Getting tracks for {} S{:02}E{:02}{} [{}]".format(
                title.name,
                title.season,
                title.episode,
                f" - {title.episode_name}" if title.episode_name else "",
                title.id
            ))
        else:
            log.info("Getting tracks for {}{} [{}]".format(
                title.name,
                f" ({title.year})" if title.year else "",
                title.id
            ))

        try:
            title.tracks.add(service.get_tracks(title), warn_only=True)
            title.tracks.add(service.get_chapters(title))
        except requests.HTTPError as e:
            log.debug(traceback.format_exc())
            raise log.exit(f" - HTTP Error {e.response.status_code}: {e.response.reason}")
        title.tracks.sort_videos()
        title.tracks.sort_audios(by_language=alang)
        title.tracks.sort_subtitles(by_language=slang)
        title.tracks.sort_chapters()

        for track in title.tracks:
            if track.language == Language.get("none"):
                track.language = title.original_lang
            track.is_original_lang = is_close_match(track.language, [title.original_lang])

        if not list(title.tracks):
            log.error(" - No tracks returned!")
            continue
        if not selected:
            log.info("> All Tracks:")
            title.tracks.print()

        try:
            if range_ == "DV+HDR":
                title.tracks.select_videos_multi(["HDR10", "DV"], by_quality=quality, by_vbitrate=vbitrate)
            else:
                title.tracks.select_videos(by_quality=quality, by_vbitrate=vbitrate, by_range=range_, one_only=True, by_worst=worst)
            title.tracks.select_audios(by_language=alang, by_codec=acodec, by_bitrate=abitrate, with_atmos=atmos, with_descriptive=audio_description, by_channels=channels)
            title.tracks.select_subtitles(by_language=slang, with_forced=False if no_forced else True)
        except ValueError as e:
            log.error(f" - {e}")
            continue

        if no_video:
            title.tracks.videos.clear()
        if no_audio:
            title.tracks.audios.clear()
        if no_subs:
            title.tracks.subtitles.clear()
        if no_chapters:
            title.tracks.chapters.clear()

        if audio_only or subs_only or chapters_only:
            title.tracks.videos.clear()
            if audio_only:
                if not subs_only:
                    title.tracks.subtitles.clear()
                if not chapters_only:
                    title.tracks.chapters.clear()
            elif subs_only:
                if not audio_only:
                    title.tracks.audios.clear()
                if not chapters_only:
                    title.tracks.chapters.clear()
            elif chapters_only:
                if not audio_only:
                    title.tracks.audios.clear()
                if not subs_only:
                    title.tracks.subtitles.clear()
            if not mux:
                no_mux = True

        log.info("> Selected Tracks:")
        title.tracks.print()

        if list_:
            continue

        skip_title = False
        track_keys = {}
        
        # Store all content keys globally for this title (for multi-key services like YouTube)
        title_all_keys = {}

        # Get keys
        for track in title.tracks:
            if (service_name == "AppleTVPlus" or service_name == "iTunes") and "VID" in str(track):
                track.encrypted = True

            if track.encrypted:
                if track.source == "DSNP":
                    track.pr_pssh = None
                    track.pssh = None
                # Add newline before licensing for better readability
                log.info(f"Licensing: {str(track).replace('â”œâ”€ ', '')}")

                # Get PSSH
                is_playready_device = hasattr(ctx.obj.cdm, 'device') and hasattr(ctx.obj.cdm.device, 'type') and ctx.obj.cdm.device.type == LocalDevice.Types.PLAYREADY
                if is_playready_device:
                    if not track.get_pr_pssh(service.session):
                        raise log.exit(" - Failed to get PR_PSSH")
                else:
                    if not track.get_pssh(service.session):
                        raise log.exit(" - Failed to get PSSH")
                if track.monalisa:
                    if track.mls_pssh:
                        log.info(f" + MLS_PSSH: {track.mls_pssh}")
                    else:
                        log.info(f" + MLS_PSSH: (No PSSH available)")
                elif is_playready_device:
                    log.info(f" + PR_PSSH: {track.pr_pssh}")
                else:
                    try:
                        log.info(f" + WV_PSSH: {base64.b64encode(Box.build(track.pssh)).decode()}")
                    except:
                        log.info(f" + WV_PSSH: {track.pssh}")

                # Get KID
                if hasattr(track, 'kid') and track.kid:
                    log.info(f" + KID: {track.kid}")
                else:
                    if not track.get_kid(service.session):
                        raise log.exit(" - Failed to get KID")
                    log.info(f" + KID: {track.kid}")
            if not track.encrypted:
                continue

            # Check MonaLisa FIRST
            if track.monalisa and track.key:
                log.info(f" + KEY: {track.key} (From MonaLisa)")
                track_keys[track.id] = track.key
                if track.kid:
                    title_all_keys[track.kid.lower().replace('-', '')] = track.key
                continue

            # Try to get key from cache/vault
            if not track.key:
                if not no_cache:
                    track.key, vault_used = ctx.obj.vaults.get(track.kid, title.id)
                    if track.key:
                        log.info(f" + KEY: {track.key} (From {vault_used.name} {vault_used.type.name} Key Vault)")
                        for vault in ctx.obj.vaults.vaults:
                            if vault == vault_used:
                                continue
                            try:
                                result = ctx.obj.vaults.insert_key(
                                    vault, service_name.lower(), track.kid, track.key, title.id, commit=True
                                )
                                if result == InsertResult.SUCCESS:
                                    log.info(f" + Cached to {vault} vault")
                                elif result == InsertResult.ALREADY_EXISTS:
                                    log.info(f" + Already exists in {vault} vault")
                            except:
                                pass
                        track_keys[track.id] = track.key
                    elif cache:
                        skip_title = True
                        break

            # If still no key, request license from CDM
            if not track.key:
                if isinstance(ctx.obj.cdm, PlayReadyCdm):
                    try:
                        session_id = ctx.obj.cdm.open()
                        wrm_header = PlayReadyPSSH(track.pr_pssh).wrm_headers[0]
                        challenge = ctx.obj.cdm.get_license_challenge(session_id, wrm_header).encode()
                        license_res = service.license(
                            challenge=challenge,
                            title=title,
                            track=track,
                            session_id=session_id
                        )
                        if isinstance(license_res, dict):
                            license_b64 = license_res.get("license", [None])[0]
                            if not license_b64:
                                raise Exception("No license field found in response")
                            license_res = base64.b64decode(license_b64).decode("utf-8")
                        elif isinstance(license_res, bytes):
                            try:
                                license_res = base64.b64decode(license_res).decode("utf-8")
                            except Exception:
                                license_res = license_res.decode("utf-8")
                        elif isinstance(license_res, str):
                            try:
                                license_res = base64.b64decode(license_res).decode("utf-8")
                            except Exception:
                                pass
                        ctx.obj.cdm.parse_license(session_id, license_res)
                        content_keys = [
                            (x.key_id.hex, x.key.hex()) for x in ctx.obj.cdm.get_keys(session_id)
                        ]
                        ctx.obj.cdm.close(session_id)
                    except Exception as e:
                        raise log.exit(f" - Error {e}")

                else:  # Widevine
                    # Check if it's pywidevine or local implementation
                    if PY_WIDEVINE_AVAILABLE and isinstance(ctx.obj.cdm, PyWidevineCdm):
                        # pywidevine implementation - Get ALL keys (including virtual/dummy)
                        from pywidevine.pssh import PSSH
                        pssh_obj = PSSH(track.pssh) if isinstance(track.pssh, str) else PSSH(track.pssh)
                        session_id = ctx.obj.cdm.open()
                        try:
                            challenge = ctx.obj.cdm.get_license_challenge(session_id, pssh_obj)
                            license_res = service.license(
                                challenge=challenge,
                                title=title,
                                track=track,
                                session_id=session_id,
                                service_name=service_name
                            )
                            ctx.obj.cdm.parse_license(session_id, license_res)
                            keys_list = ctx.obj.cdm.get_keys(session_id)
                            # Extract ALL keys - DO NOT filter by type!
                            # This includes virtual/dummy keys that YouTube uses
                            content_keys = [(k.kid, k.key.hex()) if hasattr(k.key, 'hex') else (k.kid, k.key) 
                                           for k in keys_list]  # Removed: if k.type == "CONTENT"
                        finally:
                            ctx.obj.cdm.close(session_id)
                    else:
                        # Local Widevine implementation - Get ALL keys
                        is_playready_local = hasattr(ctx.obj.cdm, 'device') and hasattr(ctx.obj.cdm.device, 'type') and ctx.obj.cdm.device.type == LocalDevice.Types.PLAYREADY
                        if is_playready_local:
                            session_id = ctx.obj.cdm.open(track.pr_pssh)
                        else:
                            session_id = ctx.obj.cdm.open(track.pssh)
                        try:
                            ctx.obj.cdm.set_service_certificate(
                                session_id,
                                service.certificate(
                                    challenge=ctx.obj.cdm.service_certificate_challenge,
                                    title=title,
                                    track=track,
                                    session_id=session_id
                                ) or ctx.obj.cdm.common_privacy_cert
                            )
                            ctx.obj.cdm.parse_license(
                                session_id,
                                service.license(
                                    challenge=ctx.obj.cdm.get_license_challenge(session_id),
                                    title=title,
                                    track=track,
                                    session_id=session_id
                                )
                            )
                        except requests.HTTPError as e:
                            log.debug(traceback.format_exc())
                            raise log.exit(f" - HTTP Error {e.response.status_code}: {e.response.reason}")
                        # Get ALL keys (content_only=False includes virtual/dummy keys)
                        all_keys = ctx.obj.cdm.get_keys(session_id, content_only=False)
                        content_keys = [
                            (x.kid.hex(), x.key.hex()) for x in all_keys
                        ]
                        ctx.obj.cdm.close(session_id)

                if not content_keys:
                    raise log.exit(" - No content keys were returned by the CDM!")

                # Determine CDM type for logging
                if isinstance(ctx.obj.cdm, PlayReadyCdm):
                    cdm_type = "PLAYREADY"
                elif PY_WIDEVINE_AVAILABLE and isinstance(ctx.obj.cdm, PyWidevineCdm):
                    cdm_type = "WIDEVINE"
                elif hasattr(ctx.obj.cdm, 'device') and hasattr(ctx.obj.cdm.device, 'type'):
                    if ctx.obj.cdm.device.type == LocalDevice.Types.PLAYREADY:
                        cdm_type = "PLAYREADY"
                    else:
                        cdm_type = "WIDEVINE"
                else:
                    cdm_type = "WIDEVINE"
                
                log.info(f" + Obtained content keys from the {cdm_type} CDM")

                # Normalize KIDs for comparison (handle UUID objects and strings)
                def normalize_kid(k):
                    if hasattr(k, 'hex'):
                        return k.hex.lower()
                    # Remove hyphens and convert to lowercase
                    return str(k).replace('-', '').replace('_', '').lower()

                # Collect ALL keys (including virtual/dummy keys)
                valid_content_keys = []
                kid_to_key_map = {}  # Map KID -> key for ALL returned keys
                
                for kid, key in content_keys:
                    normalized = normalize_kid(kid)
                    # Skip only the all-zero KIDs that are truly dummy
                    if normalized == "00000000000000000000000000000000":
                        continue
                    
                    display_kid = kid.hex if hasattr(kid, 'hex') else str(kid).replace('-', '').replace('_', '')
                    valid_content_keys.append((display_kid, key))
                    kid_to_key_map[normalized] = key
                    
                    # Show ALL keys for debugging
                    log.info(f" + {display_kid}:{key}")
                
                # YouTube uses a virtual KID in the manifest that doesn't match CDM keys
                # We need to map the track's KID to the real video KID
                if service_name == "YouTubeMovies" and isinstance(track, VideoTrack):
                    track_kid_normalized = normalize_kid(track.kid)
                    
                    # Check if track KID is not in the returned keys
                    if track_kid_normalized not in kid_to_key_map and valid_content_keys:
                        # The first returned key is typically the video key for YouTube
                        real_video_kid = valid_content_keys[0][0]
                        real_video_key = valid_content_keys[0][1]
                        
                        # Map the virtual KID to the real video KID
                        kid_to_key_map[track_kid_normalized] = real_video_key
                        log.info(f" + YouTube mapping: virtual KID {track.kid} -> real KID {real_video_kid}")
                        
                        # Also update the track's KID to the real one for future reference
                        track.kid = real_video_kid
                        
                        # Add to valid_content_keys for caching
                        valid_content_keys.append((track_kid_normalized, real_video_key))

                # Store all keys for this title
                title_all_keys.update(kid_to_key_map)

                track_kid_normalized = normalize_kid(track.kid)
                matching_key = None
                
                track_kid_normalized = normalize_kid(track.kid)
                if track_kid_normalized in kid_to_key_map:
                    # Exact match found
                    matching_key = kid_to_key_map[track_kid_normalized]
                else:
                    log.warning(f" - No exact KID match for {track.kid}")
                    log.warning(f" - Available KIDs: {list(kid_to_key_map.keys())}")
                    
                    if valid_content_keys:
                        track.all_keys = kid_to_key_map
                        if isinstance(track, VideoTrack):
                            video_kids = [k for k in kid_to_key_map.keys() 
                                         if k not in ["0da1f979f23e5716aafb73caced96931", 
                                                      "03eae3a47b225796b0f538051e269864"]]
                            if video_kids:
                                matching_key = kid_to_key_map[video_kids[0]]
                                log.info(f" + Using video fallback key from KID: {video_kids[0][:8]}")
                            else:
                                matching_key = list(kid_to_key_map.values())[0]
                                log.info(f" + Using fallback key from KID: {list(kid_to_key_map.keys())[0][:8]}")
                        else:
                            matching_key = list(kid_to_key_map.values())[0]
                            log.info(f" + Using fallback key from KID: {list(kid_to_key_map.keys())[0][:8]}")

                # Cache ALL obtained keys
                for vault in ctx.obj.vaults.vaults:
                    try:
                        log.info(f"Caching to {vault.name} ({vault.type.name}) vault")
                        cached = 0
                        already_exists = 0
                        for kid, key in valid_content_keys:
                            result = ctx.obj.vaults.insert_key(vault, service_name.lower(), kid, key, title.id)
                            if result == InsertResult.FAILURE:
                                log.warning(f" - Failed, table {service_name.lower()} doesn't exist in the vault.")
                            elif result == InsertResult.SUCCESS:
                                cached += 1
                            elif result == InsertResult.ALREADY_EXISTS:
                                already_exists += 1
                        ctx.obj.vaults.commit(vault)
                        log.info(f" + Cached {cached}/{len(valid_content_keys)} keys")
                        if already_exists:
                            log.info(f" + {already_exists}/{len(valid_content_keys)} keys already existed in vault")
                    except Exception as e:
                        log.debug(f"Vault caching error: {e}")

                if matching_key:
                    track.key = matching_key
                    if hasattr(track, 'all_keys'):
                        track.all_keys = kid_to_key_map
                    log.info(f" + KEY: {matching_key[:32]} (From CDM)")
                    track_keys[track.id] = matching_key
                else:
                    log.debug(f"Available KIDs: {[kid for kid, _ in valid_content_keys]}")
                    raise log.exit(f" - No content key was returned")

        if skip_title:
            for track in title.tracks:
                track.delete()
            continue

        if keys:
            continue

        # Download and Decrypt Loop
        for track in title.tracks:
            if track.id in track_keys and not track.key:
                track.key = track_keys[track.id]
                log.debug(f"Restored key for {track.id}")
            
            # If track has all_keys from multi-key services, use the appropriate one
            if hasattr(track, 'all_keys') and track.all_keys and track.kid:
                normalized_kid = track.kid.replace('-', '').lower()
                if normalized_kid in track.all_keys:
                    track.key = track.all_keys[normalized_kid]
                    log.debug(f" + Selected specific key for KID {normalized_kid[:8]}")

            if not keys:
                # Add newline before download for better readability
                log.info(f"Downloading: {track}")
                if track.needs_proxy:
                    proxy_url = next(iter(service.session.proxies.values()), None)
                else:
                    proxy_url = None

                track.download(directories.temp, headers=service.session.headers, proxy=proxy_url)
                
                # Add newline after download
                print()
                log.info(" + Downloaded")

            if isinstance(track, VideoTrack) and track.needs_ccextractor_first and not no_subs:
                ccextractor()

            if track.encrypted:
                if keys or track.locate() is None:
                    if track.locate() is None:
                        log.warning(f" - Skipping decryption for {track.id}: File not found")
                    continue

                log.info("Decrypting... ")

                if track.key:
                    if track.monalisa:
                        log.info(f" + KEY: {track.key} (From MonaLisa)")
                    else:
                        log.info(f" + KEY: {track.key[:32]}")
                    track_keys[track.id] = track.key
                else:
                    log.warning(" + No key available for decryption!")

            if track.key:
                if not config.decrypter:
                    raise log.exit(" - No decrypter specified")
                if track.smooth:
                    config.decrypter = "mp4decrypt"

                if config.decrypter == "packager":
                    platform = {"win32": "win", "darwin": "osx"}.get(sys.platform, sys.platform)
                    names = ["shaka-packager", "packager", f"packager-{platform}"]
                    executable = next((x for x in (shutil.which(x) for x in names) if x), None)
                    if not executable:
                        raise log.exit(" - Unable to find packager binary")

                    dec = os.path.splitext(track.locate())[0] + ".dec.mp4"
                    try:
                        os.makedirs(directories.temp, exist_ok=True)
                        proc = subprocess.Popen([
                            executable,
                            "input={},stream={},output={}".format(
                                track.locate(),
                                track.__class__.__name__.lower().replace("track", ""),
                                dec
                            ),
                            "--enable_raw_key_decryption", "--keys",
                            ", ".join([
                                f"label=0:key_id={track.kid.lower()}:key={track.key.lower()}",
                                f"label=1:key_id=00000000000000000000000000000000:key={track.key.lower()}",
                            ]),
                            "--temp_dir", directories.temp
                        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

                        last_progress = ""
                        for line in proc.stdout:
                            line = line.strip()
                            if not line:
                                continue

                            if re.match(r'^\d+/\d+$', line):
                                sys.stdout.write(f"\r   + Decrypting: {line}")
                                sys.stdout.flush()
                                last_progress = line
                            elif "Packaging completed successfully" in line:
                                if last_progress:
                                    sys.stdout.write(f"\r   + Decrypting: {last_progress} - Complete\n")
                                else:
                                    sys.stdout.write("\r   + Decrypting: Complete\n")
                                sys.stdout.flush()
                            elif any(x in line.lower() for x in ['error', 'fail', 'warning']):
                                print(f"\n   ! {line}")
                            else:
                                if line and not any(x in line for x in ['progress', '%', '[', ']']):
                                    print(f"\n   + {line}")

                        proc.wait()

                        if proc.returncode != 0:
                            raise subprocess.CalledProcessError(proc.returncode, proc.args)

                    except subprocess.CalledProcessError:
                        raise log.exit(" - Failed!")

                elif config.decrypter == "mp4decrypt":
                    executable = shutil.which("mp4decrypt")
                    if not executable:
                        raise log.exit(" - Unable to find mp4decrypt binary")

                    dec = os.path.splitext(track.locate())[0] + ".dec.mp4"
                    try:
                        # For multi-key scenarios, we may need multiple --key parameters
                        cmd = [executable]
                        
                        # Add primary key
                        cmd.extend(["--key", f"{track.kid.lower()}:{track.key.lower()}"])
                        
                        # If track has all_keys, add all of them (helps with some services)
                        if hasattr(track, 'all_keys') and track.all_keys:
                            for kid, key in track.all_keys.items():
                                if kid != track.kid.lower().replace('-', ''):
                                    cmd.extend(["--key", f"{kid}:{key.lower()}"])
                        
                        cmd.extend([track.locate(), dec])
                        
                        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

                        for line in proc.stdout:
                            line = line.strip()
                            if not line:
                                continue

                            if re.search(r'\d+%', line) or re.search(r'\d+/\d+', line):
                                sys.stdout.write(f"\r   + Decrypting: {line}")
                                sys.stdout.flush()
                            elif "Progress" in line:
                                continue
                            elif any(x in line.lower() for x in ['error', 'fail']):
                                print(f"\n   ! {line}")
                            else:
                                print(f"\n   + {line}")

                        proc.wait()

                        if proc.returncode != 0:
                            raise subprocess.CalledProcessError(proc.returncode, proc.args)

                    except subprocess.CalledProcessError:
                        raise log.exit(" - Failed!")

                else:
                    log.exit(f" - Unsupported decrypter: {config.decrypter}")

                if track.swap(dec):
                    sys.stdout.write("\n")
                    log.info(" + Decrypted")
                else:
                    log.warning(" - Decryption completed but file swap failed")

                if track.needs_repack or (config.decrypter == "mp4decrypt" and isinstance(track, (VideoTrack, AudioTrack))):
                    log.info("Repackaging stream with FFmpeg (to fix malformed streams)")

                    fixed_file = f"{track.locate()}_fixed.mkv"
                    try:
                        proc = subprocess.Popen([
                            "ffmpeg", "-hide_banner",
                            "-loglevel", "error",
                            "-i", track.locate(),
                            "-map_metadata", "-1",
                            "-fflags", "bitexact",
                            "-codec", "copy",
                            fixed_file
                        ], stderr=subprocess.PIPE, text=True)

                        for line in proc.stderr:
                            line = line.strip()
                            if not line:
                                continue

                            if re.search(r'frame=\s*\d+', line):
                                sys.stdout.write(f"\r   + Repackaging: {line[:60]}")
                                sys.stdout.flush()
                            elif "Insufficient bits" in line:
                                sys.stdout.write(f"\n   ! {line}\n")
                                sys.stdout.write("   + Repackaging: continuing...")
                                sys.stdout.flush()
                            elif any(x in line.lower() for x in ['error']):
                                print(f"\n   ! {line}")

                        proc.wait()

                        if proc.returncode == 0 and os.path.exists(fixed_file):
                            sys.stdout.write("\r   + Repackaging: Complete\n")
                            sys.stdout.flush()
                            os.unlink(track.locate())
                            os.rename(fixed_file, track.locate())
                            log.info(" + Repackaged")
                        else:
                            sys.stdout.write("\n")
                            log.warning(" - Repackage failed, using original file")
                            if os.path.exists(fixed_file):
                                os.unlink(fixed_file)

                    except Exception as e:
                        sys.stdout.write("\n")
                        log.warning(f" - Repackage failed: {e}")
                        if os.path.exists(fixed_file):
                            os.unlink(fixed_file)

            if isinstance(track, VideoTrack) and track.needs_ccextractor and not no_subs:
                ccextractor()

        # Hybrid DV+HDR Processing
        if range_ == "DV+HDR":
            try:
                dvhdr_tracks = [t for t in title.tracks.videos if t.dv and t.hdr10]
                if dvhdr_tracks:
                    log.info(" + Track is already DV+HDR, no processing needed")
                else:
                    hybrid_path = title.tracks.make_hybrid()
                    log.info(f" + Hybrid DV+HDR created: {hybrid_path}")
            except Exception as e:
                log.warning(f" - Skipped Hybrid DV+HDR: {e}")

        if not list(title.tracks) and not title.tracks.chapters:
            continue

        # Muxing / Output
        if no_mux:
            if title.tracks.chapters:
                final_file_path = directories.downloads
                if title.type == Title.Types.TV:
                    final_file_path = os.path.join(final_file_path, title.parse_filename(folder=True))
                os.makedirs(final_file_path, exist_ok=True)
                chapters_loc = filenames.chapters.format(filename=title.filename)
                title.tracks.export_chapters(chapters_loc)
                shutil.move(chapters_loc, os.path.join(final_file_path, os.path.basename(chapters_loc)))

            for track in title.tracks:
                media_info = MediaInfo.parse(track.locate())
                final_file_path = directories.downloads
                if title.type == Title.Types.TV:
                    final_file_path = os.path.join(
                        final_file_path, title.parse_filename(folder=True)
                    )
                os.makedirs(final_file_path, exist_ok=True)
                filename = title.parse_filename(media_info=media_info)
                if isinstance(track, (AudioTrack, TextTrack)):
                    filename += f".{track.language}"
                extension = track.codec if isinstance(track, TextTrack) else os.path.splitext(track.locate())[1][1:]
                if isinstance(track, AudioTrack) and extension == "mp4":
                    extension = "m4a"
                track.move(os.path.join(final_file_path, f"{filename}.{track.id}.{extension}"))

        else:
            log.info("Muxing tracks into an MKV container")
            muxed_location, returncode = title.tracks.mux(title.filename)
            if returncode == 1:
                log.warning(" - mkvmerge had at least one warning, will continue anyway...")
            elif returncode >= 2:
                raise log.exit(" - Failed to mux tracks into MKV file")
            log.info(" + Muxed")

            for track in title.tracks:
                track.delete()

            if title.tracks.chapters:
                try:
                    os.unlink(filenames.chapters.format(filename=title.filename))
                except FileNotFoundError:
                    pass

            media_info = MediaInfo.parse(muxed_location)
            final_file_path = directories.downloads
            if title.type == Title.Types.TV:
                final_file_path = os.path.join(
                    final_file_path, title.parse_filename(media_info=media_info, folder=True)
                )
            os.makedirs(final_file_path, exist_ok=True)

            if audio_only:
                extension = "mka"
            elif subs_only:
                extension = "mks"
            else:
                extension = "mkv"

            shutil.move(
                muxed_location,
                os.path.join(final_file_path, f"{title.parse_filename(media_info=media_info)}.{extension}")
            )

    log.info("Processed all titles!")


def load_services():
    for service in services.__dict__.values():
        if callable(getattr(service, "cli", None)):
            dl.add_command(service.cli)


load_services()