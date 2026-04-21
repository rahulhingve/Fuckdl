from __future__ import annotations

import logging
import re
import unicodedata
from collections import Counter
from enum import Enum
from typing import Any, Iterable, Optional, Union

from langcodes import Language
from pymediainfo import MediaInfo
from rich.tree import Tree
from sortedcontainers import SortedKeyList
from unidecode import unidecode

from fuckdl.objects.tracks import Tracks

# Constants (copied from unshackle)
VIDEO_CODEC_MAP = {
    "AVC": "H.264",
    "HEVC": "H.265"
}

DYNAMIC_RANGE_MAP = {
    "HDR10": "HDR",
    "HDR10+": "HDR",
    "HDR10 / HDR10+": "HDR",
    "Dolby Vision": "DV"
}

AUDIO_CODEC_MAP = {
    "E-AC-3": "DDP",
    "AC-3": "DD"
}


def _get_config():
    """Lazy import of config to avoid circular imports."""
    from fuckdl.config import config
    return config


class Title:
    """Base class for all media titles - compatible with Fuckdl."""
    
    class Types(Enum):
        MOVIE = 1
        TV = 2
        SONG = 3

    def __init__(
        self,
        id_: Any,
        type_: Types,
        service: type = None,
        name: Optional[str] = None,
        year: Optional[Union[int, str]] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        episode_name: Optional[str] = None,
        original_lang: Optional[Union[str, Language]] = None,
        language: Optional[Union[str, Language]] = None,
        source: Optional[str] = None,
        service_data: Optional[dict] = None,
        tracks: Optional[Tracks] = None,
        filename: Optional[str] = None,
        data: Optional[Any] = None,
        description: Optional[str] = None,
    ) -> None:
        """Base title constructor - compatible with Fuckdl parameters."""
        # ID validation
        if not id_:
            raise ValueError("A unique ID must be provided")
        
        # Service validation (optional for backward compatibility)
        if service is None:
            # Try to get from source or use a default
            service = type('Service', (), {'__name__': source or 'Unknown'})
        
        # Handle language parameters
        if original_lang is not None:
            if isinstance(original_lang, str):
                original_lang = Language.get(original_lang)
            if language is None:
                language = original_lang
        
        if language is not None:
            if isinstance(language, str):
                language = Language.get(language)
        
        # Handle year
        if year is not None:
            if isinstance(year, str) and year.isdigit():
                year = int(year)
        
        # Handle season and episode
        if season is not None:
            if isinstance(season, str) and season.isdigit():
                season = int(season)
        
        if episode is not None:
            if isinstance(episode, str) and episode.isdigit():
                episode = int(episode)
        
        # Store attributes
        self.id = id_
        self.type = type_
        self.service = service
        self.name = name
        self.year = year or 0
        self.season = season or 0
        self.episode = episode or 0
        self.episode_name = episode_name
        self.language = language
        self.original_lang = original_lang or language
        self.source = source
        self.service_data = service_data or {}
        self.data = data or service_data
        self.tracks = tracks or Tracks()
        self.filename = filename
        self.description = description
        self.quality = None
        
        # Generate initial filename if not provided
        if not self.filename and self.name:
            self.filename = self._generate_initial_filename()
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Title):
            return NotImplemented
        return self.id == other.id
    
    def __hash__(self) -> int:
        return hash(self.id)
    
    def __str__(self) -> str:
        """String representation - compatible with Fuckdl."""
        if self.type == Title.Types.MOVIE:
            if self.year:
                return f"{self.name} ({self.year})"
            return self.name or f"Title({self.id})"
        else:  # TV
            result = self.name or ""
            if self.year:
                result += f" {self.year}"
            result += f" S{self.season:02}E{self.episode:02}"
            if self.episode_name:
                result += f" {self.episode_name}"
            return result.strip() or f"Title({self.id})"
    
    def _generate_initial_filename(self) -> str:
        """Generate initial filename based on title type."""
        if self.type == Title.Types.MOVIE:
            name = self.name
            if self.year:
                name += f" ({self.year})"
            return self.normalize_filename(name)
        elif self.type == Title.Types.TV:
            name = f"{self.name} S{self.season:02}"
            return self.normalize_filename(name)
        elif self.type == Title.Types.SONG:
            artist = getattr(self, 'artist', 'Unknown')
            album = getattr(self, 'album', 'Unknown')
            name = f"{artist} - {album}"
            if self.year:
                name += f" ({self.year})"
            return self.normalize_filename(name)
        return ""
    
    def parse_filename(self, media_info: MediaInfo = None, folder: bool = False) -> str:
        """Parse filename - compatibility method for Fuckdl."""
        return self.get_filename(media_info, folder)
    
    def get_filename(self, media_info: MediaInfo = None, folder: bool = False, show_service: bool = True) -> str:
        """Get formatted filename - enhanced version."""
        config = _get_config()
        
        # Determine template based on title type
        if self.type == Title.Types.MOVIE:
            template = config.output_template.get("movies", "{title} {year}")
        elif self.type == Title.Types.TV:
            template = config.output_template.get("series", "{title} S{season:02}E{episode:02}")
        elif self.type == Title.Types.SONG:
            template = config.output_template.get("songs", "{artist} - {title}")
        else:
            # Fallback
            if folder:
                name = self.name or "Unknown"
                if self.year:
                    name += f" ({self.year})"
                return self.normalize_filename(name)
            return self.normalize_filename(str(self))
        
        # Build context
        context = self._build_template_context(media_info, show_service) if media_info else self._build_simple_context()
        
        if folder and self.type == Title.Types.TV:
            # Simplify for folder names
            template = re.sub(r'\{episode\}', '', template)
            template = re.sub(r'\{episode_name\?\}', '', template)
            template = re.sub(r'\{episode_name\}', '', template)
            template = re.sub(r'\{season_episode\}', '{season}', template)
            template = re.sub(r'\.{2,}', '.', template)
            template = re.sub(r'\s{2,}', ' ', template)
            template = re.sub(r'^[\.\s]+|[\.\s]+$', '', template)
            
            name = self.name or "Unknown"
            if self.year:
                name += f" ({self.year})"
            name += f" S{self.season:02}"
            return self.normalize_filename(name)
        
        # Format using template
        try:
            from fuckdl.core.utils.template_formatter import TemplateFormatter
            formatter = TemplateFormatter(template)
            filename = formatter.format(context)
            filename = re.sub(r"\s+", ".", filename)
            filename = re.sub(r"\.\.+", ".", filename)
            return self.normalize_filename(filename)
        except ImportError:
            # Simple fallback formatting
            filename = template
            for key, value in context.items():
                filename = filename.replace(f"{{{key}}}", str(value))
            filename = re.sub(r"\s+", ".", filename)
            filename = re.sub(r"\.\.+", ".", filename)
            return self.normalize_filename(filename)
    
    def _build_simple_context(self) -> dict:
        """Build simple context without MediaInfo."""
        context = {
            "title": self.name.replace("$", "S") if self.name else "",
            "year": self.year or "",
            "source": self.source or "",
            "tag": "",
            "quality": "",
            "audio": "",
            "video": "",
        }
        
        if self.type == Title.Types.TV:
            context["season"] = f"S{self.season:02}"
            context["episode"] = f"E{self.episode:02}"
            context["season_episode"] = f"S{self.season:02}E{self.episode:02}"
            context["episode_name"] = self.episode_name or ""
        
        return context
    
    def _build_template_context(self, media_info: MediaInfo, show_service: bool = True) -> dict:
        """Build template context from MediaInfo - enhanced from unshackle."""
        config = _get_config()
        
        primary_video_track = next(iter(media_info.video_tracks), None) if media_info else None
        primary_audio_track = next(iter(media_info.audio_tracks), None) if media_info else None
        
        # Get unique audio languages
        unique_audio_languages = 0
        if media_info and media_info.audio_tracks:
            unique_audio_languages = len({x.language.split("-")[0] for x in media_info.audio_tracks if x.language})
        
        context = {
            "source": self.source or (self.service.__name__ if hasattr(self.service, '__name__') else ""),
            "tag": config.tag or "",
            "repack": "REPACK" if getattr(config, "repack", False) else "",
            "quality": "",
            "resolution": "",
            "audio": "",
            "audio_channels": "",
            "audio_full": "",
            "atmos": "",
            "dual": "",
            "multi": "",
            "video": "",
            "hdr": "",
            "hfr": "",
            "edition": "",
            "lang_tag": "",
            "title": self.name.replace("$", "S") if self.name else "",
            "year": self.year or "",
        }
        
        # Video track processing
        if primary_video_track:
            width = getattr(primary_video_track, "width", primary_video_track.height)
            resolution = min(width, primary_video_track.height)
            
            # Handle weird aspect ratios
            try:
                dar = getattr(primary_video_track, "other_display_aspect_ratio", None) or []
                if dar and dar[0]:
                    aspect_ratio = [int(float(plane)) for plane in str(dar[0]).split(":")]
                    if len(aspect_ratio) == 1:
                        aspect_ratio.append(1)
                    ratio = aspect_ratio[0] / aspect_ratio[1]
                    if ratio not in (16 / 9, 4 / 3, 9 / 16, 3 / 4):
                        resolution = int(max(width, primary_video_track.height) * (9 / 16))
            except Exception:
                pass
            
            # Amazon weird resolution fix
            if width == 1248:
                resolution = 720
            
            scan_suffix = "i" if str(getattr(primary_video_track, "scan_type", "")).lower() == "interlaced" else "p"
            
            context.update({
                "quality": f"{resolution}{scan_suffix}",
                "resolution": str(resolution),
                "video": VIDEO_CODEC_MAP.get(primary_video_track.format, primary_video_track.format),
            })
            
            # HDR processing
            hdr_format = primary_video_track.hdr_format_commercial
            trc = primary_video_track.transfer_characteristics or primary_video_track.transfer_characteristics_original
            if hdr_format:
                if (primary_video_track.hdr_format or "").startswith("Dolby Vision"):
                    if "HDR10" in (primary_video_track.hdr_format_compatibility or ""):
                        context["hdr"] = "DV HDR"
                    else:
                        context["hdr"] = "DV"
                elif (primary_video_track.hdr_format or "").startswith("HDR Vivid"):
                    context["hdr"] = "HDR"
                else:
                    context["hdr"] = DYNAMIC_RANGE_MAP.get(hdr_format, "")
            elif trc and "HLG" in trc:
                context["hdr"] = "HLG"
            
            # HFR detection
            frame_rate = float(primary_video_track.frame_rate) if primary_video_track.frame_rate else 0.0
            if frame_rate > 30:
                context["hfr"] = "HFR"
        
        # Audio track processing
        if primary_audio_track:
            codec = primary_audio_track.format
            channel_layout = primary_audio_track.channel_layout or primary_audio_track.channellayout_original
            
            if channel_layout:
                channels = float(sum({"LFE": 0.1}.get(position.upper(), 1) for position in channel_layout.split(" ")))
            else:
                channel_count = primary_audio_track.channel_s or primary_audio_track.channels or 0
                channels = float(channel_count)
            
            features = primary_audio_track.format_additionalfeatures or ""
            
            context.update({
                "audio": AUDIO_CODEC_MAP.get(codec, codec),
                "audio_channels": f"{channels:.1f}",
                "audio_full": f"{AUDIO_CODEC_MAP.get(codec, codec)}{channels:.1f}",
                "atmos": "Atmos" if ("JOC" in features or getattr(primary_audio_track, "joc", False)) else "",
            })
        
        # Language processing
        if unique_audio_languages == 2:
            context["dual"] = "DUAL"
            context["multi"] = ""
        elif unique_audio_languages > 2:
            context["dual"] = ""
            context["multi"] = "MULTi"
        else:
            context["dual"] = ""
            context["multi"] = ""
        
        # Add TV-specific fields
        if self.type == Title.Types.TV:
            context["season"] = f"S{self.season:02}"
            context["episode"] = f"E{self.episode:02}"
            context["season_episode"] = f"S{self.season:02}E{self.episode:02}"
            context["episode_name"] = self.episode_name or ""
        
        # Add Song-specific fields
        if self.type == Title.Types.SONG:
            context["track_number"] = f"{getattr(self, 'track', 0):02}"
            context["artist"] = getattr(self, 'artist', '').replace("$", "S")
            context["album"] = getattr(self, 'album', '').replace("$", "S")
            context["disc"] = f"{getattr(self, 'disc', 0):02}" if getattr(self, 'disc', 0) > 1 else ""
        
        return context
    
    def is_wanted(self, wanted: set) -> bool:
        """Check if this title is wanted - Fuckdl compatibility."""
        if self.type != Title.Types.TV or not wanted:
            return True
        return f"{self.season}x{self.episode}" in wanted
    
    @staticmethod
    def normalize_filename(filename: str) -> str:
        """Normalize filename to be filesystem-safe - Fuckdl style."""
        # Special character replacements
        filename = filename.replace("Ã¦", "ae")
        filename = filename.replace("Ã¸", "oe")
        filename = filename.replace("Ã¥", "aa")
        filename = filename.replace("'", "")
        filename = unidecode(filename)
        filename = "".join(c for c in filename if unicodedata.category(c) != "Mn")
        
        # Structural replacements
        filename = filename.replace("/", " - ")
        filename = filename.replace("&", " and ")
        filename = filename.replace("$", "S")
        filename = re.sub(r"[:; ]", ".", filename)
        filename = re.sub(r"[\\*!?Â¿,'\"()<>|#]", "", filename)
        filename = re.sub(r"[. ]{2,}", ".", filename)
        filename = filename.rstrip().rstrip(".")
        
        return filename


class Titles(list):
    """
    Collection of titles - compatible with Fuckdl.
    Can contain movies or episodes.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title_name = None
        
        if self:
            # Try to get title name from first item
            first = self[0]
            if hasattr(first, 'name') and first.name:
                self.title_name = first.name
            elif hasattr(first, 'title') and first.title:
                self.title_name = first.title
    
    def print(self):
        """Print summary - Fuckdl compatibility."""
        log = logging.getLogger("Titles")
        log.info(f"Title: {self.title_name}")
        
        # Check if these are TV episodes
        if any(hasattr(x, 'type') and x.type == Title.Types.TV for x in self):
            
            # Group by season
            season_counts = {}
            for x in self:
                season = getattr(x, 'season', 0)
                season_counts[season] = season_counts.get(season, 0) + 1
            
            season_info = ", ".join(f"{s} ({c})" for s, c in sorted(season_counts.items()))
            log.info(f"By Season: {season_info}")
    
    def order(self):
        """Order titles - Fuckdl compatibility."""
        self.sort(key=lambda t: int(getattr(t, 'year', 0) or 0))
        self.sort(key=lambda t: int(getattr(t, 'episode', 0) or 0))
        self.sort(key=lambda t: int(getattr(t, 'season', 0) or 0))
        return self
    
    def with_wanted(self, wanted):
        """Yield only wanted tracks - Fuckdl compatibility."""
        for title in self:
            if hasattr(title, 'is_wanted') and title.is_wanted(wanted):
                yield title
            elif not wanted:
                yield title
    
    def tree(self, verbose: bool = False) -> Tree:
        """Create a rich Tree visualization."""
        if not self:
            return Tree("No titles", guide_style="bright_black")
        
        # Check if these are TV episodes
        if any(hasattr(x, 'type') and x.type == Title.Types.TV for x in self):
            # Group by season
            seasons = {}
            for title in self:
                season = getattr(title, 'season', 0)
                if season not in seasons:
                    seasons[season] = []
                seasons[season].append(title)
            
            season_breakdown = ", ".join(f"S{s}({len(seasons[s])})" for s in sorted(seasons.keys()))
            tree = Tree(f"{len(seasons)} seasons, {season_breakdown}", guide_style="bright_black")
            
            if verbose:
                for season in sorted(seasons.keys()):
                    episodes = seasons[season]
                    max_ep_len = len(str(len(episodes)))
                    season_tree = tree.add(
                        f"[bold]Season {season}[/]: [bright_black]{len(episodes)} episodes",
                        guide_style="bright_black",
                    )
                    for episode in sorted(episodes, key=lambda x: getattr(x, 'episode', 0)):
                        ep_num = getattr(episode, 'episode', 0)
                        ep_name = getattr(episode, 'episode_name', None) or getattr(episode, 'name', None)
                        if ep_name:
                            season_tree.add(
                                f"[bold]{str(ep_num).zfill(max_ep_len)}.[/] [bright_black]{ep_name}"
                            )
                        else:
                            season_tree.add(f"[bright_black]Episode {str(ep_num).zfill(max_ep_len)}")
        else:
            # Movies
            tree = Tree(f"{len(self)} Movie{['s', ''][len(self) == 1]}", guide_style="bright_black")
            if verbose:
                for movie in self:
                    name = getattr(movie, 'name', str(movie))
                    year = getattr(movie, 'year', None)
                    tree.add(f"[bold]{name}[/] [bright_black]({year or '?'})", guide_style="bright_black")
        
        return tree


# Convenience aliases for backward compatibility
Series = Titles
Movies = Titles
Album = Titles


__all__ = ("Title", "Titles", "Series", "Movies", "Album")