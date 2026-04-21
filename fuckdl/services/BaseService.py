import json
import logging
import os
import re
from abc import ABC

import requests
from requests.adapters import HTTPAdapter, Retry
import random

from fuckdl.config import config, directories
from fuckdl.utils import try_get
from fuckdl.utils.collections import as_list
from fuckdl.utils.io import get_ip_info


class BaseService(ABC):
    """The service base class."""

    # Abstract class variables
    ALIASES = []
    GEOFENCE = []

    def __init__(self, ctx):
        self.config = ctx.obj.config
        self.cookies = ctx.obj.cookies
        self.credentials = ctx.obj.credentials

        self.log = logging.getLogger(self.ALIASES[0])
        self.session = self.get_session()
        self.force_proxy = ctx.parent.params["force_proxy"]

        if ctx.parent.params["no_proxy"]:
            return

        proxy = ctx.parent.params["proxy"] or next(iter(self.GEOFENCE), None)
        if proxy:
            if len("".join(i for i in proxy if not i.isdigit())) == 2:
                proxy = self.get_proxy(proxy)
            if proxy:
                if "://" not in proxy:
                    proxy = f"https://{proxy}"
                self.session.proxies.update({"all": proxy})
            else:
                self.log.info(" + Proxy was skipped as current region matches")

    def get_session(self):
        """Creates a Python-requests Session with common headers and retry handler."""
        session = requests.Session()
        session.mount("https://", HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
        ))
        session.hooks = {
            "response": lambda r, *_, **__: r.raise_for_status(),
        }
        session.headers.update(config.headers)
        session.cookies.update(self.cookies or {})
        return session

    # Abstract functions

    def get_titles(self):
        """Get Titles for the provided title ID."""
        raise NotImplementedError

    def get_tracks(self, title):
        """Get Track objects of the Title."""
        raise NotImplementedError

    def get_chapters(self, title):
        """Get MenuTracks chapter objects of the Title."""
        return []

    def certificate(self, challenge, title, track, session_id):
        """Get the Service Privacy Certificate."""
        return self.license(challenge, title, track, session_id)

    def license(self, challenge, title, track, session_id):
        """Get the License response for the specified challenge."""
        raise NotImplementedError

    # Convenience functions

    def parse_title(self, ctx, title):
        title = title or ctx.parent.params.get("title")
        if not title:
            self.log.exit(" - No title ID specified")
        if not getattr(self, "TITLE_RE"):
            self.title = title
            return {}
        for regex in as_list(self.TITLE_RE):
            m = re.search(regex, title)
            if m:
                self.title = m.group("id")
                return m.groupdict()
        self.log.warning(f" - Unable to parse title ID {title!r}, using as-is")
        self.title = title

    def get_cache(self, key):
        """Get path object for an item from service Cache."""
        return os.path.join(directories.cache, self.ALIASES[0], key)

    # Proxy functions

    def get_proxy(self, region):
        """Get a proxy for the specified region."""
        if not region:
            raise self.log.exit("Region cannot be empty")
        
        region = region.lower()
        self.log.info(f"Obtaining a proxy to \"{region}\"")
        
        # Extract base country code
        base_region = "".join(char for char in region if not char.isdigit())
        
        # Skip proxy if already in correct region
        if not self.force_proxy:
            ip_info = get_ip_info()
            if ip_info and ip_info.get("country_code", "").lower() == base_region:
                return None
        
        # Get default service from config
        default_service = self._get_config_value('default_proxy_service')
        
        # Check if region already includes service
        if ":" in region:
            service, query_region = region.split(":", 1)
            proxy = self._get_proxy_by_service(service, query_region)
            if proxy:
                return proxy
        
        # Use default service if configured
        if default_service and default_service != 'null':
            proxy = self._get_proxy_by_service(default_service, base_region)
            if proxy:
                return proxy
        
        # Fallback to basic proxies from config
        proxies_dict = self._get_config_value('proxies', {})
        if isinstance(proxies_dict, dict) and base_region in proxies_dict:
            proxy = proxies_dict[base_region]
            self.log.info(f" + {proxy} (via basic proxy config)")
            return proxy
        
        raise self.log.exit(f" - Unable to obtain a proxy for region '{region}'.")
    
    def _get_proxy_by_service(self, service, region):
        """Get proxy from specific service."""
        service = service.lower()
        
        if service == "nordvpn":
            return self._get_nordvpn_proxy(region)
        elif service == "surfshark":
            return self._get_surfshark_proxy(region)
        elif service == "windscribe":
            return self._get_windscribe_proxy(region)
        else:
            self.log.warning(f"Unknown service: {service}")
            return None
    
    def _get_config_value(self, key, default=None):
        """Get config value handling both dict and SimpleNamespace."""
        try:
            if isinstance(config, dict):
                return config.get(key, default)
            else:
                return getattr(config, key, default)
        except Exception:
            return default
    
    def _get_provider_config(self, provider_name):
        """Get provider configuration from proxy_providers section."""
        try:
            proxy_providers = self._get_config_value('proxy_providers', {})
            if isinstance(proxy_providers, dict):
                return proxy_providers.get(provider_name, {})
            return {}
        except Exception:
            return {}
    
    # ========================================================================
    # NORDVPN PROXY METHODS
    # ========================================================================
    
    def _get_nordvpn_proxy(self, region):
        """Get NordVPN proxy for a region."""
        nord_config = self._get_provider_config('nordvpn')
        
        username = nord_config.get('username', '')
        password = nord_config.get('password', '')
        
        if not username or not password:
            self.log.warning("NordVPN credentials not configured")
            return None
        
        proxy = f"https://{username}:{password}@"
        
        if any(char.isdigit() for char in region):
            proxy += f"{region}.nordvpn.com"
        else:
            hostname = self._get_nordvpn_server(region)
            if not hostname:
                return None
            proxy += hostname
        
        proxy_url = proxy + ":89"
        self.log.info(f" + {proxy_url} (via NordVPN)")
        return proxy_url
    
    def _get_nordvpn_server(self, country):
        """Get recommended NordVPN server for a country."""
        try:
            countries = self.session.get(
                url="https://api.nordvpn.com/v1/servers/countries"
            ).json()
            
            country_id = [x["id"] for x in countries if x["code"].lower() == country.lower()]
            if not country_id:
                return None
            country_id = country_id[0]
            
            recommendations = self.session.get(
                url="https://api.nordvpn.com/v1/servers/recommendations",
                params={"filters[country_id]": country_id, "limit": 30}
            ).json()
            
            hostnames = [host["hostname"] for host in recommendations]
            return random.choice(hostnames) if hostnames else None
        except Exception as e:
            self.log.warning(f"Failed to get NordVPN server: {e}")
            return None
    
    # ========================================================================
    # SURFSHARK PROXY METHODS
    # ========================================================================
    
    def _get_surfshark_proxy(self, region):
        """Get Surfshark proxy for a region."""
        surf_config = self._get_provider_config('surfshark')
        
        username = surf_config.get('username', '')
        password = surf_config.get('password', '')
        
        self.log.debug(f"Surfshark config: username={username[:10]}..., password={'*' * len(password) if password else 'empty'}")
        
        if not username or not password:
            self.log.warning("Surfshark credentials not configured")
            return None
        
        hostname = self._get_surfshark_server(region)
        if not hostname:
            return None
        
        proxy_url = f"https://{username}:{password}@{hostname}:443"
        self.log.info(f" + {proxy_url[:50]}... (via Surfshark)")
        return proxy_url
    
    def _get_surfshark_server(self, country):
        """Get recommended Surfshark server for a country."""
        try:
            response = self.session.get(
                url='https://api.surfshark.com/v5/server/clusters/all'
            )
            countries = response.json()
            
            items = [
                x for x in countries
                if x.get("countryCode", "").lower() == country.lower()
                and x.get("type", "").lower() not in ("obfuscated", "static")
            ]
            
            if not items:
                return None
            
            return min(items, key=lambda x: x.get("load", 100))["connectionName"]
        except Exception as e:
            self.log.warning(f"Failed to get Surfshark server: {e}")
            return None
    
    # ========================================================================
    # WINDSCRIBE PROXY METHODS
    # ========================================================================
    
    def _get_windscribe_proxy(self, region):
        """Get Windscribe proxy for a region."""
        wind_config = self._get_provider_config('windscribe')
        
        username = wind_config.get('username', '')
        password = wind_config.get('password', '')
        
        if not username or not password:
            self.log.warning("Windscribe credentials not configured")
            return None
        
        server_match = re.match(r"^([a-z]{2})(\d+)$", region)
        if server_match:
            country_code, server_num = server_match.groups()
            hostname = self._get_windscribe_specific_server(country_code, server_num)
        else:
            hostname = self._get_windscribe_random_server(region)
        
        if not hostname:
            return None
        
        proxy_url = f"https://{username}:{password}@{hostname}:443"
        self.log.info(f" + {proxy_url[:50]}... (via Windscribe)")
        return proxy_url
    
    def _get_windscribe_servers(self):
        """Get Windscribe server list."""
        try:
            response = self.session.get(
                url="https://assets.windscribe.com/serverlist/firefox/1/1"
            )
            data = response.json()
            return data.get("data", [])
        except Exception as e:
            self.log.warning(f"Failed to get Windscribe servers: {e}")
            return []
    
    def _get_windscribe_specific_server(self, country_code, server_num):
        """Get specific Windscribe server by number."""
        servers = self._get_windscribe_servers()
        
        num_stripped = server_num.lstrip("0") or "0"
        candidates = [
            f"{country_code}-{server_num}.",
            f"{country_code}-{num_stripped}.",
            f"{country_code}-{server_num.zfill(3)}.",
        ]
        
        for location in servers:
            if location.get("country_code", "").lower() != country_code:
                continue
            for group in location.get("groups", []):
                for host in group.get("hosts", []):
                    hostname = host.get("hostname", "")
                    if any(hostname.startswith(prefix) for prefix in candidates):
                        return hostname
        
        return None
    
    def _get_windscribe_random_server(self, country_code):
        """Get random Windscribe server for a country."""
        servers = self._get_windscribe_servers()
        
        hostnames = []
        for location in servers:
            if location.get("country_code", "").lower() == country_code.lower():
                for group in location.get("groups", []):
                    for host in group.get("hosts", []):
                        if hostname := host.get("hostname"):
                            hostnames.append(hostname)
        
        return random.choice(hostnames) if hostnames else None