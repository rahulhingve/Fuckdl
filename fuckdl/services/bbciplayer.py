from __future__ import annotations

import hashlib
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import click
import m3u8
import requests

from fuckdl.config import directories
from fuckdl.objects import AudioTrack, TextTrack, Title, Track, Tracks
from fuckdl.services.BaseService import BaseService
from fuckdl.utils.collections import as_list
from fuckdl.utils.sslciphers import SSLCiphers


class BBCiPlayer(BaseService):
    """
    Service code for the BBC iPlayer streaming service (https://www.bbc.co.uk/iplayer).

    V2 - Updated by @anotherbiguserhere    

    Tips:
        - Use full title URL as input for best results.
        - Use --list-titles before anything, iPlayer's listings are often messed up.
        - An SSL certificate (PEM) is required for accessing the UHD endpoint.
        - Use --range HLG to request H265 UHD tracks

    Fixed issuses in the manifiest extraction and upgrated some things into the code

    Credits to the original author of this service

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026
    """

    ALIASES = ["BBCiP", "bbc", "iplayer"]
    GEOFENCE = ["gb"]
    TITLE_RE = r"^(?:https?://(?:www\.)?bbc\.co\.uk/(?:iplayer/(?P<type>episode|episodes)/|programmes/))?(?P<id>[a-z0-9]+)(?:/.*)?$"
    
    # Constants
    AUDIO_PATTERN = r"-audio_\w+=\d+"
    VIDEO_PATTERN = r"-video=(\d+)"
    AUDIO_BITRATE_PATTERN = r"-audio_\w+=(\d+)"
    REQUEST_TIMEOUT = 30

    @staticmethod
    @click.command(name="BBCiPlayer", short_help="https://www.bbc.co.uk/iplayer")
    @click.argument("title", type=str)
    @click.option("-m", "--movie", is_flag=True, default=False, help="Title is a movie.")
    @click.pass_context
    def cli(ctx, **kwargs):
        return BBCiPlayer(ctx, **kwargs)

    def __init__(self, ctx, title, movie):
        self.mytitle = title
        self.movie = movie
        super().__init__(ctx)
        
        # Get parameters from parent context
        self.vcodec = ctx.parent.params.get("vcodec") if ctx.parent else None
        self.range_ = ctx.parent.params.get("range_") if ctx.parent else None
        
        # Setup certificate path for UHD content
        self.cert_path = Path(directories.package_root) / "certs" / "bbciplayer.pem"
        
        # Configure session
        self.session.headers.update({"user-agent": "BBCiPlayer/5.17.2.32046"})
        
        # Validate HDR requirements
        if self.range_ and self.range_ == "HLG":
            if not self.cert_path.exists():
                self.log.error("HLG tracks cannot be requested without an SSL certificate")
                sys.exit(1)
            self.session.headers.update({"user-agent": self.config["user_agent"]})
            self.vcodec = "H265"
        
        self.configure()

    def get_titles(self):
        try:
            match = re.match(self.TITLE_RE, self.mytitle)
            if not match:
                raise ValueError("Invalid title format")
            title_type = match.group("type")
            pid = match.group("id")
        except Exception:
            raise ValueError("Could not parse ID from title - is the URL correct?")

        data = self.get_data(pid, slice_id=None)
        
        # Handle single episode
        if data is None and title_type == "episode":
            episode = self.fetch_episode(pid)
            return [episode] if episode else []

        # Handle missing metadata
        if data is None:
            raise ValueError(f"Metadata was not found - if {pid} is an episode, use full URL as input")

        # Handle single item (movie or single episode)
        if data.get("count", 0) < 2:
            response = self.session.get(self.config["endpoints"]["episodes"].format(pid=pid))
            response.raise_for_status()
            data = response.json()
            
            if not data.get("episodes"):
                raise ValueError(f"Metadata was not found for {pid}")

            episode = data["episodes"][0]

            if self.movie:
                return [Title(
                    id_=episode.get("id"),
                    type_=Title.Types.MOVIE,
                    name=episode.get("title"),
                    year=episode.get("release_date_time", "").split("-")[0] if episode.get("release_date_time") else "",
                    source=self.ALIASES[0],
                    service_data=episode
                )]
            
            episode_title = self.fetch_episode(pid)
            return [episode_title] if episode_title else []
        
        # Handle series
        slices = data.get("slices", [{"id": None}])
        seasons = []
        for slice_item in slices:
            season_data = self.get_data(pid, slice_item.get("id"))
            if season_data:
                seasons.append(season_data)
        
        episode_ids = []
        for season in seasons:
            if not season:
                continue
            results = season.get("entities", {}).get("results", [])
            for episode in results:
                episode_data = episode.get("episode", {})
                if not episode_data.get("live") and episode_data.get("id"):
                    episode_ids.append(episode_data["id"])
        
        return self.get_episodes(episode_ids)

    def get_tracks(self, title):
        # Get playlist
        r = self.session.get(url=self.config["endpoints"]["playlist"].format(pid=title.id))
        r.raise_for_status()
        playlist = r.json()

        # Get versions
        versions = playlist.get("allAvailableVersions", [])
        
        if playlist.get("info", {}).get("readme"):
            # Try to fetch from site source code
            r = self.session.get(self.config["base_url"].format(type="episode", pid=title.id))
            redux_match = re.search("window.__IPLAYER_REDUX_STATE__ = (.*?);</script>", r.text)
            if redux_match:
                data = json.loads(redux_match.group(1))
                versions = [{"pid": x.get("id")} for x in data.get("versions", []) if x.get("kind") != "audio-described"]

        if self.vcodec == "H265":
            default_version = playlist.get("defaultAvailableVersion", {})
            if default_version.get("pid"):
                versions = [{"pid": default_version.get("pid")}]
            else:
                versions = []

        if not versions:
            self.log.error(" - No available versions for this title was found")
            sys.exit(1)

        # Get media connections
        connections = []
        for version in versions:
            vpid = version.get("pid")
            if vpid:
                media = self.check_all_versions(vpid)
                if media:
                    connections.append(media)

        # Select best quality
        all_qualities = []
        for conn in connections:
            if isinstance(conn, list):
                for item in conn:
                    if item.get("height"):
                        all_qualities.append(int(item["height"]))
            elif isinstance(conn, dict) and conn.get("height"):
                all_qualities.append(int(conn["height"]))

        if not all_qualities:
            self.log.error(" - No qualities found")
            sys.exit(1)

        max_quality = max(all_qualities, key=int)
        
        # Find media with max quality
        media = None
        for conn in connections:
            if isinstance(conn, list):
                if any(item.get("height") == max_quality for item in conn):
                    media = conn
                    break
            elif isinstance(conn, dict) and conn.get("height") == max_quality:
                media = conn
                break
        
        if not media:
            media = connections[0] if connections else None

        if not media:
            self.log.error(" - Selection unavailable. Title doesn't exist or your IP address is blocked")
            sys.exit(1)

        # Get video connection
        video_items = [x for x in media if x.get("kind") == "video"]
        if not video_items:
            raise ValueError("No video found in media")
        
        video = video_items[0]
        video_connections = sorted(video.get("connection", []), key=lambda x: x.get("priority", 0))
        
        connection = {}
        if self.vcodec == "H265":
            connection = video_connections[0] if video_connections else {}
        else:
            for conn in video_connections:
                if conn.get("supplier") == "mf_akamai" and conn.get("transferFormat") == "dash":
                    connection = conn
                    break
            if not connection and video_connections:
                connection = video_connections[0]

        if not connection:
            raise ValueError("No valid video connection found")

        # Process connection URL
        if self.vcodec != "H265":
            if connection.get("transferFormat") == "dash":
                connection["href"] = "/".join(
                    connection["href"].replace("dash", "hls").split("?")[0].split("/")[0:-1] + ["hls", "master.m3u8"]
                )
                connection["transferFormat"] = "hls"
            elif connection.get("transferFormat") == "hls":
                connection["href"] = "/".join(
                    connection["href"].replace(".hlsv2.ism", "").split("?")[0].split("/")[0:-1] + ["hls", "master.m3u8"]
                )

        # Parse manifest
        if connection.get("transferFormat") == "dash":
            tracks = Tracks.from_mpd(
                url=connection["href"],
                session=self.session,
                source=self.ALIASES[0]
            )
        elif connection.get("transferFormat") == "hls":
            response = self.session.get(connection["href"])
            response.raise_for_status()
            playlist_m3u8 = m3u8.loads(response.text, connection["href"])
            tracks = Tracks.from_m3u8(playlist_m3u8, source=self.ALIASES[0])
        else:
            raise ValueError(f"Unsupported transfer format: {connection.get('transferFormat')}")

        # Process audio tracks
        for video_track in tracks.videos:
            # Mark HLG content
            video_track.hlg = video_track.codec and video_track.codec.startswith("hev1") and not (video_track.hdr10 or video_track.dv)

            video_urls = as_list(video_track.url)
            if not video_urls:
                continue

            if any(re.search(self.AUDIO_PATTERN, x) for x in video_urls):
                # Create audio track
                audio_url = re.sub(r"-video=\d+", "", video_urls[0])
                audio = AudioTrack(
                    id_=hashlib.md5(audio_url.encode()).hexdigest()[0:7],
                    url=audio_url,
                    codec=video_track.extra.stream_info.codecs.split(",")[0] if video_track.extra and hasattr(video_track.extra, 'stream_info') else "aac",
                    language='en',
                    bitrate=int(self.find(self.AUDIO_BITRATE_PATTERN, video_urls[0]) or 0),
                    descriptive=False,
                    descriptor=Track.Descriptor.M3U,
                    source=self.ALIASES[0],
                    encrypted=video_track.encrypted,
                    pssh=video_track.pssh,
                    extra=video_track.extra,
                )
                
                # Add if not already present
                if not any(t.id == audio.id for t in tracks):
                    tracks.add(audio)
                
                # Update video track
                video_track.url = [re.sub(self.AUDIO_PATTERN, "", x) for x in video_urls][0]
                if video_track.extra and hasattr(video_track.extra, 'stream_info'):
                    codecs = video_track.extra.stream_info.codecs.split(",")
                    video_track.codec = codecs[1] if len(codecs) > 1 else codecs[0]
                video_track.bitrate = int(self.find(self.VIDEO_PATTERN, as_list(video_track.url)[0]) or 0)

        # Add subtitles
        caption_items = [x for x in media if x.get("kind") == "captions"]
        if caption_items:
            caption = caption_items[0]
            caption_connections = sorted(caption.get("connection", []), key=lambda x: x.get("priority", 0))
            if caption_connections:
                caption_conn = caption_connections[0]
                tracks.add(
                    TextTrack(
                        id_=hashlib.md5(caption_conn["href"].encode()).hexdigest()[0:6],
                        url=caption_conn["href"],
                        codec=caption.get("type", "application/ttml+xml").split("/")[-1].replace("ttaf+xml", "ttml"),
                        language="en",
                        source=self.ALIASES[0],
                        forced=False,
                        sdh=True,
                    )
                )

        return tracks

    def get_chapters(self, title):
        return []

    def certificate(self, **_):
        return None

    def license(self, challenge, track, **_):
        return None

    def get_data(self, pid, slice_id):
        json_data = {
            "id": "9fd1636abe711717c2baf00cebb668de",
            "variables": {
                "id": pid,
                "perPage": 200,
                "page": 1,
                "sliceId": slice_id if slice_id else None,
            },
        }

        try:
            r = self.session.post(self.config["endpoints"]["metadata"], json=json_data)
            r.raise_for_status()
            return r.json()["data"]["programme"]
        except Exception:
            return None

    def check_all_versions(self, vpid):
        media = None

        if self.vcodec == "H265":
            if not self.cert_path.exists():
                self.log.error(" - H265 tracks cannot be requested without an SSL certificate")
                sys.exit(1)

            session = self.session
            session.mount("https://", SSLCiphers())
            session.mount("http://", SSLCiphers())
            mediaset = "iptv-uhd"

            for mediator in ["securegate.iplayer.bbc.co.uk", "ipsecure.stage.bbc.co.uk"]:
                try:
                    url = self.config["endpoints"]["secure"].format(mediator, vpid, mediaset)
                    response = session.get(url, cert=str(self.cert_path))
                    response.raise_for_status()
                    availability = response.json()
                    if availability.get("media"):
                        media = availability["media"]
                        break
                    elif availability.get("result"):
                        self.log.error(f"Error: {availability['result']}")
                        sys.exit(1)
                except Exception:
                    continue

        else:
            mediaset = "iptv-all"

            for mediator in ["open.live.bbc.co.uk", "open.stage.bbc.co.uk"]:
                try:
                    url = self.config["endpoints"]["open"].format(mediator, mediaset, vpid)
                    response = self.session.get(url)
                    response.raise_for_status()
                    availability = response.json()
                    if availability.get("media"):
                        media = availability["media"]
                        break
                    elif availability.get("result"):
                        self.log.error(f"Error: {availability['result']}")
                        sys.exit(1)
                except Exception:
                    continue

        return media if media else []

    def fetch_episode(self, pid):
        try:
            r = self.session.get(self.config["endpoints"]["episodes"].format(pid=pid))
            r.raise_for_status()

            data = r.json()
            episode = data["episodes"][0]
            subtitle = episode.get("subtitle")
            year = episode.get("release_date_time", "").split("-")[0] if episode.get("release_date_time") else ""
            numeric_position = episode.get("numeric_tleo_position")

            season_num = 0
            episode_num = 0
            episode_name = ""

            if subtitle:
                series_match = re.search(r"Series (\d+):|Season (\d+):|(\d{4}/\d{2}):", subtitle)
                if series_match:
                    season_num = int(series_match.group(1) or series_match.group(2) or series_match.group(3).replace("/", ""))
                
                if season_num == 0 and not data.get("slices"):
                    season_num = 1
                
                number_match = re.search(r"(\d+)\.|Episode (\d+)", subtitle)
                if number_match:
                    episode_num = int(number_match.group(1) or number_match.group(2))
                else:
                    episode_num = numeric_position or 0
                
                name_match = re.search(r"\d+\. (.+)", subtitle)
                if name_match:
                    episode_name = name_match.group(1)
                elif not re.search(r"Series \d+: Episode \d+", subtitle):
                    episode_name = subtitle
            
            return Title(
                id_=episode.get("id"),
                type_=Title.Types.TV,
                name=episode.get("title"),
                episode_name=episode_name,
                season=season_num,
                episode=episode_num,
                year=year,
                source=self.ALIASES[0],
                service_data=episode
            )
        except Exception as e:
            self.log.error(f"Failed to fetch episode {pid}: {e}")
            return None

    def get_episodes(self, episodes):
        titles = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.fetch_episode, pid) for pid in episodes]
            for future in futures:
                result = future.result()
                if result:
                    titles.append(result)
        return titles

    def find(self, pattern, string, group=None):
        if not string:
            return None
        if group:
            m = re.search(pattern, string)
            return m.group(group) if m else None
        else:
            matches = re.findall(pattern, string)
            return matches[0] if matches else None

    def configure(self):
        self.session.headers.update({
            "User-Agent": self.config["user_agent"],
        })