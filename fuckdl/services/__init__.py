import urllib3
import os
import re
import logging
from copy import copy
from typing import Any, Optional, Generator
from fuckdl.services.BaseService import BaseService

SERVICE_MAP = {}

for service in os.listdir(os.path.dirname(__file__)):
    if service.startswith("_") or not service.endswith(".py"):
        continue

    service = os.path.splitext(service)[0]

    if service in ("__init__", "BaseService"):
        continue

    with open(os.path.join(os.path.dirname(__file__), f"{service}.py"), encoding="utf-8") as fd:
        code = ""
        for line in fd.readlines():
            # Only remove problematic imports that might cause issues with exec,
            # but keep essential imports like threading.Lock
            match = re.match(r"(\s*)(?:from\s+threading\s+import\s+Lock|import\s+threading)", line)
            if match:
                # Keep threading imports
                code += line
                continue
                
            # Skip other imports to avoid issues, but don't replace with pass
            # This keeps the code structure intact
            match = re.match(r"(\s*)(?:import(?! click)|from)(?!.*threading)", line)
            if match:
                # Skip this line entirely (don't add to code)
                continue

            code += line
            if re.match(r"\s*super\(\)\.__init__\(", line):
                break
        exec(code)

for x in copy(globals()).values():
    if isinstance(x, type) and issubclass(x, BaseService) and x != BaseService:
        SERVICE_MAP[x.__name__] = x.ALIASES


def get_service_key(value):
    """
    Get the Service Key name (e.g. DisneyPlus, not dsnp, disney+, etc.) from the SERVICE_MAP.
    Input value can be of any case-sensitivity and can be either the key itself or an alias.
    """
    value = value.lower()
    for key, aliases in SERVICE_MAP.items():
        if value in map(str.lower, aliases) or value == key.lower():
            return key