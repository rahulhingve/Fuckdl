import asyncio
import contextlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional, Union, List

import httpx
import requests
import yaml
import tqdm

_ip_info_cache = None


def load_yaml(path: str) -> dict:
    """Load YAML configuration file."""
    if not os.path.isfile(path):
        return {}
    with open(path, encoding="utf-8") as fd:
        return yaml.safe_load(fd)

def get_ip_info(session: Optional[Any] = None, fresh: bool = False) -> dict:
    """Get IP location information using ipwho.is."""
    global _ip_info_cache
    
    if fresh or _ip_info_cache is None:
        try:
            client = session or requests
            response = client.get("https://ipwho.is/", timeout=10)
            _ip_info_cache = response.json()
        except Exception:
            _ip_info_cache = {"country_code": "unknown", "country": "Unknown"}
    
    return _ip_info_cache or {"country_code": "unknown", "country": "Unknown"}


@contextlib.asynccontextmanager
async def start_pproxy(host: str, port: int, username: str, password: str):
    """Start a local pproxy server that routes through a remote proxy."""
    import pproxy
    
    rerouted_proxy = "http://localhost:8081"
    server = pproxy.Server(rerouted_proxy)
    remote = pproxy.Connection(f"http+ssl://{host}:{port}#{username}:{password}")
    handler = await server.start_server(dict(rserver=[remote]))
    try:
        yield rerouted_proxy
    finally:
        handler.close()
        await handler.wait_closed()


def download_range(url: str, count: int, start: int = 0, proxy: Optional[str] = None) -> bytes:
    """Download n bytes without using the Range header."""
    executable = shutil.which("curl")
    if not executable:
        raise EnvironmentError("Track needs curl to download a chunk of data but wasn't found...")

    arguments = [
        executable, "-s", "-L", "--proxy-insecure",
        "--output", "-", "--url", url
    ]
    if proxy:
        arguments.extend(["--proxy", proxy])

    curl = subprocess.Popen(
        arguments,
        stdout=subprocess.PIPE,
        stderr=open(os.devnull, "wb"),
        shell=False
    )
    buffer = b''
    location = -1
    while len(buffer) < count:
        stdout = curl.stdout
        data = b''
        if stdout:
            data = stdout.read(1)
        if len(data) > 0:
            location += len(data)
            if location >= start:
                buffer += data
        else:
            if curl.poll() is not None:
                break
    curl.kill()
    return buffer


async def aria2c(
    uri: Union[str, List[str]],
    out: Union[str, Path],
    headers: Optional[dict] = None,
    proxy: Optional[str] = None,
    track: Optional[Any] = None
) -> None:
    await aria2c_standard(uri, out, headers, proxy)


async def aria2c_standard(
    uri: Union[str, List[str]],
    out: Union[str, Path],
    headers: Optional[dict] = None,
    proxy: Optional[str] = None
) -> None:
    """Downloads file(s) using Aria2(c) - Standard Implementation."""
    executable = shutil.which("aria2c") or shutil.which("aria2")
    if not executable:
        raise EnvironmentError("Aria2c executable not found...")

    arguments = [
        executable, "-c", "--remote-time", "-o", os.path.basename(out),
        "-x", "16", "-j", "16", "-s", "16",
        "--allow-overwrite=true", "--auto-file-renaming=false",
        "--retry-wait", "5", "--max-tries", "15",
        "--max-file-not-found", "15", "--summary-interval", "0",
        "--file-allocation", "none" if sys.platform == "win32" else "falloc",
        "--console-log-level", "warn", "--download-result", "hide",
    ]
    
    for header, value in (headers or {}).items():
        if header.lower() == "accept-encoding":
            continue
        arguments.extend(["--header", f"{header}: {value}"])

    segmented = isinstance(uri, list)
    segments_dir = f"{out}_segments"
    
    if segmented:
        uri_input = "\n".join([
            f"{url}\n\tdir={segments_dir}\n\tout={i:08}.mp4"
            for i, url in enumerate(uri)
        ])
        
        if proxy:
            arguments.append("--all-proxy")
            if proxy.lower().startswith("https://"):
                auth, hostname = proxy[8:].split("@")
                async with start_pproxy(*hostname.split(":"), *auth.split(":")) as pproxy_:
                    arguments.extend([pproxy_, "-d", segments_dir, "-i-"])
                    proc = await asyncio.create_subprocess_exec(*arguments, stdin=subprocess.PIPE)
                    await proc.communicate(uri_input.encode("utf-8"))
            else:
                arguments.extend([proxy, "-d", segments_dir, "-i-"])
                proc = await asyncio.create_subprocess_exec(*arguments, stdin=subprocess.PIPE)
                await proc.communicate(uri_input.encode("utf-8"))
        else:
            arguments.extend(["-d", segments_dir, "-i-"])
            proc = await asyncio.create_subprocess_exec(*arguments, stdin=subprocess.PIPE)
            await proc.communicate(uri_input.encode("utf-8"))
        
        if proc and proc.returncode is not None and proc.returncode != 0:
            raise Exception(f"Aria2c failed with return code {proc.returncode}")
        
        # Merge segments with progress
        print("\n   + Merging segments: ", end="", flush=True)
        with open(out, "wb") as ofd:
            files = sorted(os.listdir(segments_dir))
            total_files = len(files)
            for i, file in enumerate(files):
                file_path = os.path.join(segments_dir, file)
                with open(file_path, "rb") as ifd:
                    data = ifd.read()
                    data = re.sub(
                        b"(tfhd\x00\x02\x00\x1a\x00\x00\x00\x01\x00\x00\x00)\x02",
                        b"\\g<1>\x01",
                        data
                    )
                    ofd.write(data)
                os.unlink(file_path)
                pct = ((i + 1) / total_files) * 100
                sys.stdout.write(f"\r   + Merging segments: {pct:.0f}%")
                sys.stdout.flush()
            os.rmdir(segments_dir)
        sys.stdout.write("\n")
    else:
        if proxy:
            arguments.append("--all-proxy")
            if proxy.lower().startswith("https://"):
                auth, hostname = proxy[8:].split("@")
                async with start_pproxy(*hostname.split(":"), *auth.split(":")) as pproxy_:
                    arguments.extend([pproxy_, "-d", os.path.dirname(out), uri])
                    proc = await asyncio.create_subprocess_exec(*arguments)
                    await proc.communicate()
            else:
                arguments.extend([proxy, "-d", os.path.dirname(out), uri])
                proc = await asyncio.create_subprocess_exec(*arguments)
                await proc.communicate()
        else:
            arguments.extend(["-d", os.path.dirname(out), uri])
            proc = await asyncio.create_subprocess_exec(*arguments)
            await proc.communicate()
        
        if proc and proc.returncode is not None and proc.returncode != 0:
            raise Exception(f"Aria2c failed with return code {proc.returncode}")

async def n_m3u8dl_re_dash(
    mpd_url: str,
    out: Union[str, Path],
    re_name: str = None,
    headers: Optional[dict] = None,
    proxy: Optional[str] = None,
    representation_id: Optional[str] = None,
    track_type: str = "video"
) -> str:
    """
    Download DASH manifest using N_m3u8DL-RE.
    
    Args:
        mpd_url: URL of the MPD manifest
        out: Output directory path
        re_name: Base name for output file
        headers: HTTP headers to use
        proxy: Proxy URL to use
        representation_id: Specific representation ID to select
        track_type: Type of track ('video', 'audio', 'subtitle')
    
    Returns:
        Path to downloaded file
    """
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if re_name is None:
        re_name = "stream"
    
    executable = shutil.which("N_m3u8DL-RE") or shutil.which("RE")
    if not executable:
        raise EnvironmentError("N_m3u8DL-RE executable not found in PATH")
    
    cmd = [
        executable, mpd_url,
        "--save-name", re_name,
        "--save-dir", str(out_dir),
        "--tmp-dir", str(out_dir),
        "--auto-subtitle-fix", "True",
        "--log-level", "INFO",
    ]
    
    # Select specific representation if provided
    if representation_id:
        if track_type == "video":
            cmd += ["--select-video", f"id={representation_id}"]
        elif track_type == "audio":
            cmd += ["--select-audio", f"id={representation_id}"]
        elif track_type == "subtitle":
            cmd += ["--select-subtitle", f"id={representation_id}"]
    
    # Add headers
    if headers:
        header_str = "\r\n".join([f"{k}: {v}" for k, v in headers.items()])
        cmd += ["--header", header_str]
    
    # Add proxy if needed
    if proxy:
        cmd += ["--custom-proxy", proxy]
    else:
        cmd += ["--use-system-proxy", "False"]
    
    # Run downloader
    subprocess.run(cmd, check=True)
    
    # Find downloaded file
    files_in_dir = list(out_dir.rglob(f"{re_name}*"))
    if not files_in_dir:
        raise RuntimeError(f"N_m3u8DL-RE did not generate any file for {re_name}")
    
    # Return the largest file (usually the main stream)
    largest_file = max(files_in_dir, key=lambda f: f.stat().st_size)
    return str(largest_file)


async def n_m3u8dl_re_hls(
    m3u8_url: str,
    out: Union[str, Path],
    re_name: str = None,
    headers: Optional[dict] = None,
    proxy: Optional[str] = None
) -> str:
    """
    Download HLS manifest using N_m3u8DL-RE.
    
    Args:
        m3u8_url: URL of the M3U8 manifest
        out: Output directory path
        re_name: Base name for output file
        headers: HTTP headers to use
        proxy: Proxy URL to use
    
    Returns:
        Path to downloaded file
    """
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if re_name is None:
        re_name = "stream"
    
    executable = shutil.which("N_m3u8DL-RE") or shutil.which("RE")
    if not executable:
        raise EnvironmentError("N_m3u8DL-RE executable not found in PATH")
    
    cmd = [
        executable, m3u8_url,
        "--save-name", re_name,
        "--save-dir", str(out_dir),
        "--tmp-dir", str(out_dir),
        "--auto-subtitle-fix", "True",
        "--log-level", "INFO",
    ]
    
    # Add headers
    if headers:
        header_str = "\r\n".join([f"{k}: {v}" for k, v in headers.items()])
        cmd += ["--header", header_str]
    
    # Add proxy if needed
    if proxy:
        cmd += ["--custom-proxy", proxy]
    else:
        cmd += ["--use-system-proxy", "False"]
    
    # Run downloader
    subprocess.run(cmd, check=True)
    
    # Find downloaded file
    files_in_dir = list(out_dir.rglob(f"{re_name}*"))
    if not files_in_dir:
        raise RuntimeError(f"N_m3u8DL-RE did not generate any file for {re_name}")
    
    # Return the largest file
    largest_file = max(files_in_dir, key=lambda f: f.stat().st_size)
    return str(largest_file)


async def n_m3u8dl_re_ism(
    ism_url: str,
    out: Union[str, Path],
    re_name: str = None,
    headers: Optional[dict] = None,
    proxy: Optional[str] = None
) -> str:
    """
    Download ISM (Smooth Streaming) manifest using N_m3u8DL-RE.
    
    Args:
        ism_url: URL of the ISM manifest
        out: Output directory path
        re_name: Base name for output file
        headers: HTTP headers to use
        proxy: Proxy URL to use
    
    Returns:
        Path to downloaded file
    """
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    if re_name is None:
        re_name = "stream"
    
    executable = shutil.which("N_m3u8DL-RE") or shutil.which("RE")
    if not executable:
        raise EnvironmentError("N_m3u8DL-RE executable not found in PATH")
    
    cmd = [
        executable, ism_url,
        "--save-name", re_name,
        "--save-dir", str(out_dir),
        "--tmp-dir", str(out_dir),
        "--log-level", "INFO",
    ]
    
    # Add headers
    if headers:
        header_str = "\r\n".join([f"{k}: {v}" for k, v in headers.items()])
        cmd += ["--header", header_str]
    
    # Add proxy if needed
    if proxy:
        cmd += ["--custom-proxy", proxy]
    else:
        cmd += ["--use-system-proxy", "False"]
    
    # Run downloader
    subprocess.run(cmd, check=True)
    
    # Find downloaded file
    files_in_dir = list(out_dir.rglob(f"{re_name}*"))
    if not files_in_dir:
        raise RuntimeError(f"N_m3u8DL-RE did not generate any file for {re_name}")
    
    # Return the largest file
    largest_file = max(files_in_dir, key=lambda f: f.stat().st_size)
    return str(largest_file)


async def m3u8re(
    uri: Union[str, List[str]],
    out: Union[str, Path],
    headers: Optional[dict] = None,
    proxy: Optional[str] = None
) -> None:
    """
    Legacy m3u8re function - only for single URL HLS downloads.
    For segmented downloads, use n_m3u8dl_re_* functions instead.
    """
    out = Path(out)
    
    if isinstance(uri, list):
        raise ValueError("m3u8re does not support multiple URI downloads. Use n_m3u8dl_re_hls() for segmented HLS.")

    if headers:
        headers = {k: v for k, v in headers.items() if k.lower() != "accept-encoding"}

    executable = shutil.which("m3u8re") or shutil.which("N_m3u8DL-RE")
    if not executable:
        raise EnvironmentError("N_m3u8DL-RE executable not found...")

    arguments = [
        executable, uri,
        "--tmp-dir", str(out.parent),
        "--save-dir", str(out.parent),
        "--save-name", out.name.replace('.mp4', '').replace('.vtt', '').replace('.m4a', ''),
        "--auto-subtitle-fix", "False",
        "--thread-count", "32",
        "--download-retry-count", "100",
        "--log-level", "INFO"
    ]

    if headers:
        arguments.extend([
            "--header",
            "\r\n".join([f"{k}: {v}" for k, v in headers.items()])
        ])
        
    if proxy:
        arguments.extend(["--custom-proxy", proxy])

    try:
        subprocess.run(arguments, check=True)
    except subprocess.CalledProcessError:
        raise ValueError("N_m3u8DL-RE failed too many times, aborting")

    print()