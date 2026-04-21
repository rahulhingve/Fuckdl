"""
Fuckdl configuration module.
"""

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import yaml
from appdirs import AppDirs
from requests.utils import CaseInsensitiveDict

from fuckdl.objects.vaults import Vault
from fuckdl.utils.collections import merge_dict


class Config:
    """Configuration management class."""
    
    @staticmethod
    def load_vault(vault_config: dict) -> Vault:
        """Load a vault configuration into a Vault object."""
        normalized_config = {
            "type_" if k == "type" else k: v for k, v in vault_config.items()
        }
        return Vault(**normalized_config)


class Directories:
    """Manages all directory paths used by the application."""
    
    def __init__(self):
        """Initialize directory paths with sensible defaults."""
        self.app_dirs = AppDirs("fuckdl", roaming=False)
        self.package_root = self._get_package_root()
        
        # Configuration directories
        self.configuration = self.package_root / "config"
        self.user_configs = self.package_root
        self.service_configs = self.user_configs / "services"
        
        # Data directories
        self.data = self.package_root
        self.downloads = self.package_root.parent.parent / "downloads"
        self.temp = self.package_root.parent.parent / "temp"
        self.cache = self.package_root / "cache"
        self.cookies = self.data / "cookies"
        self.logs = self.package_root / "logs"
        self.devices = self.data / "devices"
        
        # Ensure critical directories exist
        self._ensure_directories()
    
    def _get_package_root(self) -> Path:
        """Safely determine the package root directory."""
        try:
            return Path(__file__).resolve().parent.parent
        except Exception as e:
            print(f"Warning: Could not resolve package root: {e}", file=sys.stderr)
            return Path.cwd() / "fuckdl"
    
    def _ensure_directories(self) -> None:
        """Create essential directories if they don't exist."""
        essential_dirs = [
            self.temp,
            self.cache,
            self.cookies,
            self.logs,
            self.devices,
        ]
        
        for directory in essential_dirs:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                print(f"Warning: Could not create directory {directory}: {e}", file=sys.stderr)


class Filenames:
    """Manages all filename patterns used by the application."""
    
    def __init__(self, directories: Directories):
        """Initialize filename patterns."""
        self.directories = directories
        
        # Configuration files
        self.root_config = str(directories.configuration / "fuckdl.yml")
        self.user_root_config = str(directories.user_configs / "fuckdl.yml")
        self.service_config = str(directories.configuration / "services" / "{service}.yml")
        self.user_service_config = str(directories.service_configs / "{service}.yml")
        
        # Temporary files
        self.subtitles = str(directories.temp / "TextTrack_{id}_{language_code}.srt")
        self.chapters = str(directories.temp / "{filename}_chapters.txt")
        
        # Log files
        self.log = str(directories.logs / "Fuckdl_{time}.log")
    
    def get_service_config_path(self, service: str, user_config: bool = False) -> Path:
        """Get the configuration file path for a service."""
        if user_config:
            return self.directories.service_configs / f"{service}.yml"
        return self.directories.configuration / "services" / f"{service}.yml"


def get_downloader_for_service(service_name: str) -> str:
    """
    Get the downloader type for a given service based on configuration.
    
    Args:
        service_name: The service name or alias (e.g., 'DisneyPlus', 'DSNP', 'NF')
    
    Returns:
        Downloader type string ('aria2c', 'tqdm', 'm3u8re', 'saldl')
    """
    downloader_config = getattr(config, 'downloader_by_service', {})
    
    if not downloader_config:
        return 'aria2c'  # Default fallback
    
    # Check for exact match
    if service_name in downloader_config:
        return downloader_config[service_name]
    
    # Check case-insensitive
    service_lower = service_name.lower()
    for key, value in downloader_config.items():
        if key.lower() == service_lower:
            return value
    
    # Return default
    return downloader_config.get('default', 'aria2c')


def load_configuration() -> SimpleNamespace:
    """Load and merge configuration files."""
    config_data = {}
    
    # Load root configuration
    root_config_path = Path(filenames.root_config)
    if root_config_path.exists():
        try:
            with open(root_config_path, 'r', encoding='utf-8') as fd:
                config_data = yaml.safe_load(fd) or {}
        except Exception as e:
            print(f"Warning: Could not load root config: {e}", file=sys.stderr)
    
    # Load and merge user configuration
    user_root_config_path = Path(filenames.user_root_config)
    if user_root_config_path.exists():
        try:
            with open(user_root_config_path, 'r', encoding='utf-8') as fd:
                user_config_data = yaml.safe_load(fd) or {}
                merge_dict(config_data, user_config_data)
        except Exception as e:
            print(f"Warning: Could not load user config: {e}", file=sys.stderr)
    
    return SimpleNamespace(**config_data)


def setup_paths() -> None:
    """Setup directory paths based on configuration."""
    try:
        downloads_path = getattr(config, 'directories', {}).get('downloads')
        if downloads_path:
            downloads_path = Path(downloads_path)
            if downloads_path.is_dir():
                directories.downloads = downloads_path
            else:
                print(f"Warning: Configured downloads path does not exist: {downloads_path}", 
                      file=sys.stderr)
        
        temp_path = getattr(config, 'directories', {}).get('temp')
        if temp_path:
            temp_path = Path(temp_path)
            if temp_path.is_dir():
                directories.temp = temp_path
                filenames.subtitles = str(temp_path / "TextTrack_{id}_{language_code}.srt")
                filenames.chapters = str(temp_path / "{filename}_chapters.txt")
            else:
                print(f"Warning: Configured temp path does not exist: {temp_path}", 
                      file=sys.stderr)
    except Exception as e:
        print(f"Warning: Could not setup paths: {e}", file=sys.stderr)


def setup_arguments() -> None:
    """Setup command-line arguments configuration."""
    if not hasattr(config, 'arguments'):
        config.arguments = {}
    
    # Ensure 'range_' exists
    if 'range_' not in config.arguments:
        config.arguments['range_'] = config.arguments.get('range')
    
    # Map service aliases
    try:
        from fuckdl.services import SERVICE_MAP
        
        for service, aliases in SERVICE_MAP.items():
            for alias in aliases:
                if service in config.arguments and alias not in config.arguments:
                    config.arguments[alias] = config.arguments.get(service)
    except ImportError:
        pass
    
    # Make arguments case-insensitive
    config.arguments = CaseInsensitiveDict(config.arguments)


# Initialize core components
directories = Directories()
filenames = Filenames(directories)
config = load_configuration()

# Setup credentials
credentials = getattr(config, 'credentials', {})

# Setup downloader_by_service with defaults if not present
if not hasattr(config, 'downloader_by_service'):
    config.downloader_by_service = {'default': 'aria2c'}

# Configure paths and arguments
setup_paths()
setup_arguments()

# Export public interface
__all__ = [
    'Config',
    'Directories',
    'Filenames',
    'directories',
    'filenames',
    'config',
    'credentials',
    'get_downloader_for_service',
    'load_configuration',
    'setup_paths',
    'setup_arguments'
]