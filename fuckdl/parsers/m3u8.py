import base64
import re
from hashlib import md5

from fuckdl.objects import AudioTrack, TextTrack, Track, Tracks, VideoTrack
from fuckdl.utils import Cdm
from fuckdl.vendor.pymp4.parser import Box



def parse(master, source=None):
    """
    Convert a Variant Playlist M3U8 document to a Tracks object with Video, Audio and
    Subtitle Track objects. This is not an M3U8 parser, use https://github.com/globocom/m3u8
    to parse, and then feed the parsed M3U8 object.

    :param master: M3U8 object of the `m3u8` project: https://github.com/globocom/m3u8
    :param source: Source tag for the returned tracks.

    The resulting Track objects' URL will be to another M3U8 file, but this time to an
    actual media stream and not to a variant playlist. The m3u8 downloader code will take
    care of that, as the tracks downloader will be set to `M3U8`.

    Don't forget to manually handle the addition of any needed or extra information or values.
    Like `encrypted`, `pssh`, `hdr10`, `dv`, e.t.c. Essentially anything that is per-service
    should be looked at. Some of these values like `pssh` and `dv` will try to be set automatically
    if possible but if you definitely have the values in the service, then set them.
    Subtitle Codec will default to vtt as it has no codec information.

    Example:
        tracks = Tracks.from_m3u8(m3u8.load(url))
        # check the m3u8 project for more info and ways to parse m3u8 documents
    """
    if not master.is_variant:
        raise ValueError("Tracks.from_m3u8: Expected a Variant Playlist M3U8 document...")

    # get pssh if available
    # uses master.data.session_keys instead of master.keys as master.keys is ONLY EXT-X-KEYS and
    # doesn't include EXT-X-SESSION-KEYS which is whats used for variant playlist M3U8.
    widevine_urn = "urn:uuid:edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
    widevine_keys = [x.uri for x in master.session_keys if x.keyformat.lower() == widevine_urn]
    pssh = widevine_keys[0].split(",")[-1] if widevine_keys else None
    pr_keys = [x.uri for x in master.session_keys if x.keyformat.lower() == "com.microsoft.playready"]
    pr_pssh = pr_keys[0].split(",")[-1] if pr_keys else None
    if pssh:
        pssh = base64.b64decode(pssh)
        # noinspection PyBroadException
        try:
            pssh = Box.parse(pssh)
        except Exception:
            pssh = Box.parse(Box.build(dict(
                type=b"pssh",
                version=0,  # can only assume version & flag are 0
                flags=0,
                system_ID=Cdm.uuid,
                init_data=pssh
            )))

    # Helper function to safely extract codec
    def safe_get_codec(stream_info):
        """Safely extract codec from stream_info, handling None values."""
        try:
            if hasattr(stream_info, 'codecs') and stream_info.codecs:
                return stream_info.codecs.split(",")[0].split(".")[0]
            return "h264"  # Default codec if none found
        except (AttributeError, TypeError, IndexError):
            return "h264"  # Default fallback codec

    # Helper function to safely get resolution
    def safe_get_resolution(stream_info):
        """Safely extract resolution from stream_info, handling None values."""
        try:
            if hasattr(stream_info, 'resolution') and stream_info.resolution:
                return stream_info.resolution
            return (0, 0)
        except (TypeError, AttributeError):
            return (0, 0)

    # Helper function to safely get frame rate
    def safe_get_frame_rate(stream_info):
        """Safely extract frame rate from stream_info, handling None values."""
        try:
            if hasattr(stream_info, 'frame_rate') and stream_info.frame_rate:
                return stream_info.frame_rate
            return None
        except (TypeError, AttributeError):
            return None

    # Helper function to safely get video range
    def safe_get_video_range(stream_info):
        """Safely extract video range from stream_info, handling None values."""
        try:
            if hasattr(stream_info, 'video_range') and stream_info.video_range:
                return stream_info.video_range.strip('"')
            return "SDR"
        except (TypeError, AttributeError):
            return "SDR"

    # Helper function to detect HDR/DV
    def is_dv(codec_str):
        """Check if codec indicates Dolby Vision."""
        try:
            if codec_str:
                return codec_str.split(".")[0] in ("dvhe", "dvh1")
            return False
        except (AttributeError, IndexError):
            return False

    tracks = Tracks()

    # VIDEO tracks
    for x in master.playlists:
        stream_info = x.stream_info
        codec_str = safe_get_codec(stream_info)
        
        tracks.add(VideoTrack(
            id_=md5(str(x).encode()).hexdigest()[0:7],  # 7 chars only for filename length
            source=source,
            url=("" if re.match("^https?://", x.uri) else x.base_uri) + x.uri,
            # metadata
            codec=codec_str,
            language=None,  # playlists don't state the language, fallback must be used
            bitrate=stream_info.average_bandwidth if hasattr(stream_info, 'average_bandwidth') and stream_info.average_bandwidth else (stream_info.bandwidth if hasattr(stream_info, 'bandwidth') else 0),
            width=safe_get_resolution(stream_info)[0],
            height=safe_get_resolution(stream_info)[1],
            fps=safe_get_frame_rate(stream_info),
            hdr10=(not is_dv(codec_str) and safe_get_video_range(stream_info) != "SDR"),
            hlg=False,  # TODO: Can we get this from the manifest?
            dv=is_dv(codec_str),
            # switches/options
            descriptor=Track.Descriptor.M3U,
            # decryption
            encrypted=bool(master.keys or master.session_keys),
            pssh=pssh,
            pr_pssh=pr_pssh,
            # extra
            extra=x
        ))

    # AUDIO tracks
    if hasattr(master, 'media') and master.media:
        for x in master.media:
            if x.type == "AUDIO" and x.uri:
                # Safely extract audio codec
                audio_codec = "aac"
                try:
                    if hasattr(x, 'codecs') and x.codecs:
                        audio_codec = x.codecs.split(",")[0].split(".")[0]
                except (AttributeError, IndexError):
                    pass
                
                tracks.add(AudioTrack(
                    id_=md5(str(x).encode()).hexdigest()[0:6],
                    source=source,
                    url=("" if re.match("^https?://", x.uri) else x.base_uri) + x.uri,
                    # metadata
                    codec=audio_codec,
                    language=x.language,
                    bitrate=0,  # TODO: M3U doesn't seem to state bitrate?
                    channels=x.channels if hasattr(x, 'channels') else None,
                    atmos=(x.channels or "").endswith("/JOC") if hasattr(x, 'channels') else False,
                    descriptive="public.accessibility.describes-video" in (x.characteristics or "") if hasattr(x, 'characteristics') else False,
                    # switches/options
                    descriptor=Track.Descriptor.M3U,
                    # decryption
                    encrypted=False,  # don't know for sure if encrypted
                    pssh=pssh,
                    pr_pssh=pr_pssh,
                    # extra
                    extra=x
                ))

    # SUBTITLE tracks
    if hasattr(master, 'media') and master.media:
        for x in master.media:
            if x.type == "SUBTITLES" and x.uri:
                tracks.add(TextTrack(
                    id_=md5(str(x).encode()).hexdigest()[0:6],
                    source=source,
                    url=("" if re.match("^https?://", x.uri) else x.base_uri) + x.uri,
                    # metadata
                    codec="vtt",  # assuming VTT, codec info isn't shown
                    language=x.language,
                    forced=x.forced == "YES" if hasattr(x, 'forced') else False,
                    sdh="public.accessibility.describes-music-and-sound" in (x.characteristics or "") if hasattr(x, 'characteristics') else False,
                    # switches/options
                    descriptor=Track.Descriptor.M3U,
                    # extra
                    extra=x
                ))

    return tracks