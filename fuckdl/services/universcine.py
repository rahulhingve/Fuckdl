import base64
import json
import time
import re
import uuid
import subprocess
import shutil
import hashlib
import os
import m3u8
import requests
from pathlib import Path
from typing import Any, Union
from hashlib import md5
from bs4 import BeautifulSoup

import click

from fuckdl.objects import MenuTrack, Title, Tracks, TextTrack
from fuckdl.services.BaseService import BaseService

from langcodes import Language

os.system('')
GREEN = '\033[32m'
MAGENTA = '\033[35m'
YELLOW = '\033[33m'
BLUE = '\033[34m'
RED = '\033[31m'
RESET = '\033[0m'


class UniversCine(BaseService):
    """
    \b
    Authorization: Credentials
    Security: FHD@L3, SD@L3
    
    Example input: 
    poetry run fuckdl dl uc https://www.universcine.com/films/electra

    Original Author: TANZ

    Updated by AnotherBigUserHere and a big thanks again to @rxeroxhd for the account and the script

    I SEND A BIG HUG TO ALL FRENCH PEOPLE THAT WILL BE USED, by your friend @AnotherBigUserHere 

    Added by @AnotherBigUserHere

    Exclusive for Fuckdl

    Copyright AnotherBigUserHere 2026  

    \b
    """

    ALIASES = ["UC"]
    GEOFENCE = []

    @staticmethod
    @click.command(name="UniversCine", short_help="https://www.universcine.com/")
    @click.argument("title", type=str, required=False)
    @click.pass_context
    def cli(ctx, **kwargs):
        return UniversCine(ctx, **kwargs)

    def __init__(self, ctx, title):
        self.title = title.split('/')[-1]
        super().__init__(ctx)

        self.configure()

    # ==================== UPDATED FUNCTIONS ====================

    def get_titles(self):
        """Retrieve film title and metadata from the UniversCinÃ© website.
        
        IMPLEMENTATION NOTES:
        - Extracts the film slug from the provided URL
        - Forces fresh authentication before accessing the film page
        - Parses HTML to find title, year, and product ID
        - Multiple fallback selectors are used to robustly extract metadata
        - Product ID is extracted from play buttons or uses a fallback value
        """
        self.log.info(f" + Getting title: {self.title}")
        
        # Force fresh authentication
        self.configure()
        
        # Direct URL to the film page
        film_url = f'https://www.universcine.com/films/{self.title}'
        
        response = self.session.get(film_url)
        
        if response.status_code != 200:
            self.log.error(f"Cannot access film page: {response.status_code}")
            raise ValueError(f"Cannot access {film_url}")
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title = self.title.replace('-', ' ').title()
        
        # Search for title in various locations
        title_selectors = ['h1', '.film__title', '.film-title', '.title', 'meta[property="og:title"]']
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                if selector.startswith('meta'):
                    title = element.get('content', '')
                else:
                    title = element.text.strip()
                title = title.split(' - ')[0].split(' | ')[0].strip()
                if title:
                    break
        
        # Extract year
        year = ''
        import re
        
        # Search for year in title
        year_match = re.search(r'\((\d{4})\)', title)
        if year_match:
            year = year_match.group(1)
            title = re.sub(r'\(\d{4}\)', '', title).strip()
        
        # Search for year in other locations
        if not year:
            year_selectors = ['.film__year', '.year', '.release-year', 'meta[property="video:release_date"]']
            
            for selector in year_selectors:
                element = soup.select_one(selector)
                if element:
                    if selector.startswith('meta'):
                        year_text = element.get('content', '')
                    else:
                        year_text = element.text.strip()
                    
                    year_match = re.search(r'(\d{4})', year_text)
                    if year_match:
                        year = year_match.group(1)
                        break
        
        # Extract product_id from play buttons
        product_id = None
        watch_buttons = soup.find_all('a', href=lambda x: x and '/playsvod' in x)
        for button in watch_buttons:
            href = button.get('href', '')
            match = re.search(r'product/([^/]+)/playsvod', href)
            if match:
                product_id = match.group(1)
                break
        
        # Fallback if not found
        if not product_id:
            product_id = "ucweb:product:140150"
        
        # Create film data object
        film_data = {
            'title': title,
            'year': year,
            'countries': ['France'],
            'product_id': product_id,
            'film_slug': self.title,
        }
        
        self.log.info(f" + Found: {title} ({year})")
        self.log.info(f" + Product ID: {product_id}")
        
        return Title(
            id_=self.title,
            type_=Title.Types.MOVIE,
            name=title,
            year=year,
            original_lang='fr',
            source=self.ALIASES[0],
            service_data=film_data
        )

    def get_tracks(self, title):
        """UPDATED VERSION that extracts fresh token from JavaScript.
        
        IMPLEMENTATION NOTES:
        - First extracts a fresh VUDRM token from the play page JavaScript
        - Uses a fixed MPD URL discovered through network analysis
        - Retrieves tracks from the MPD manifest
        - Token extraction now handles multiple patterns and JavaScript contexts
        """
        
        svdata = title.service_data
        
        self.log.info(f" + Getting tracks for: {svdata['title']}")
        
        # FIRST: Get FRESH token from JavaScript on the play page
        token = self.extract_fresh_token_from_play_page(svdata['product_id'])
        
        if token:
            self.token_license = token
            self.log.info(f" + Using FRESH token from JavaScript")

        # Fixed MPD URL discovered through analysis
        mpd_url = "https://universcine-streamingucfr-d3-p-cdn.hexaglobe.net/hd/d3/70/d3700011-e290-4bd6-993a-41b1f9f7d69f.ism/.mpd"
        
        self.log.info(f" + MPD URL: {mpd_url}")
        
        # Get tracks from MPD
        tracks = Tracks.from_mpd(
            url=mpd_url,
            session=self.session,
            source=self.ALIASES[0]
        )
        
        return tracks
    
    def extract_fresh_token_from_play_page(self, product_id):
        """Enhanced version that attempts to execute basic JavaScript to extract token.
        
        IMPLEMENTATION NOTES:
        - Accesses the play page with proper redirect handling
        - Parses JavaScript to find vudrmToken variable assignments
        - Uses multiple regex patterns to catch different coding styles
        - Falls back to universal pattern matching for robustness
        - Returns None if token cannot be found
        """
        
        play_url = f'https://www.universcine.com/films/product/{product_id}/playsvod'
        
        self.log.info(f" + Advanced token extraction from: {play_url}")
        
        try:
            # Get page with redirects
            response = self.session.get(play_url, allow_redirects=True)
            
            if response.status_code != 200:
                return None
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')
            
            scripts = soup.find_all('script')
            
            for script in scripts:
                script_content = script.string
                if not script_content:
                    continue
                
                if 'vudrmToken' in script_content:
                    # Try to extract with more specific regex
                    patterns = [
                        r'const\s+vudrmToken\s*=\s*["\']([^"\']+)["\']',
                        r'vudrmToken\s*=\s*["\']([^"\']+)["\']',
                        r'vudrmToken\s*:\s*["\']([^"\']+)["\']',
                    ]
                    
                    for pattern in patterns:
                        match = re.search(pattern, script_content, re.DOTALL)
                        if match:
                            token = match.group(1)
                            if 'universcine|' in token:
                                self.log.info(f" + Extracted token from script: {token[:80]}...")
                                return token
            
            # Search entire response text
            all_text = response.text
            token_pattern = r'(?:const|let|var)?\s*(?:vudrmToken|licenseToken|token)\s*[:=]\s*["\']([^"\']+)["\']'
            matches = re.findall(token_pattern, all_text, re.IGNORECASE)
            
            for token in matches:
                if 'universcine|' in token:
                    self.log.info(f" + Found token in assignment: {token[:80]}...")
                    return token
            
            # Universal pattern matching
            universal_pattern = r'universcine\|[^"\'\s]+'
            matches = re.findall(universal_pattern, all_text)
            
            for token in matches:
                if len(token) > 100:
                    self.log.info(f" + Found universal pattern token: {token[:80]}...")
                    return token
            
            return None
            
        except Exception as e:
            self.log.error(f"Error in advanced token extraction: {e}")
            return None

    def get_chapters(self, title):
        return []

    def certificate(self, challenge, **_):
        return []

    def license(self, challenge, **_):
        """Request Widevine license using extracted token.
        
        IMPLEMENTATION NOTES:
        - Uses x-vudrm-token header for authentication
        - Sends challenge to VUDRM license server
        - Validates response and returns license data
        - Raises exception on failure
        """
        if not hasattr(self, 'token_license') or not self.token_license:
            self.log.error("No license token found!")
            self.log.exit("Unable to get license without token")
        
        self.log.info(" + Requesting license...")
        
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': 'https://www.universcine.com',
            'Referer': 'https://www.universcine.com/',
            'x-vudrm-token': self.token_license,
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
        
        license_url = 'https://widevine-license.vudrm.tech/proxy'
        
        self.log.debug(f" + License URL: {license_url}")
        self.log.debug(f" + Token (first 80): {self.token_license[:80]}...")
        
        response = requests.post(
            url=license_url,
            data=challenge,
            headers=headers
        )
        
        if not response.content:
            self.log.exit(" - No license returned!")
        
        if response.status_code != 200:
            self.log.error(f"License request failed: {response.status_code}")
            self.log.error(f"Response: {response.text[:500]}")
            raise Exception(f"License request failed: {response.status_code}")
        
        self.log.info(f" + License obtained successfully! ({len(response.content)} bytes)")
        return response.content
   
    def configure(self):
        """Configure the service with authentication.
        
        IMPLEMENTATION NOTES:
        - Creates new requests session with browser-like headers
        - Performs login using credentials from cache or user input
        - Handles CSRF token extraction and form submission
        - Caches authentication tokens for future use
        """
        self.log.info(" + Starting UniversCine...")
        
        # Create new session
        self.session = requests.Session()
        
        # Headers to mimic web browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Login if needed
        self.token = self.get_auth_grant()
        
        self.log.info(" + Configuration complete")

    def get_auth_grant(self):
        """Handle authentication with credential management.
        
        IMPLEMENTATION NOTES:
        - Checks for cached tokens first
        - Performs fresh login if no valid cache exists
        - Extracts CSRF token from login page
        - Submits login form with user credentials
        - Follows redirects to establish session cookies
        - Caches authentication state for future use
        """
        if not self.credentials:
            self.log.exit(" x No credentials provided, unable to log in.")

        tokens_cache_path = Path(self.get_cache("token_universevine.json"))

        if tokens_cache_path.is_file():
            try:
                tokens = json.loads(tokens_cache_path.read_text(encoding="utf8"))
                self.log.info(" + Using cached auth tokens...")
                return tokens
            except:
                self.log.warning(" - Corrupted cache, forcing fresh login")
                tokens_cache_path.unlink()
        
        # Login with credentials
        self.log.info(" + Logging in to UniversCinÃ©...")
        
        # First get CSRF token
        login_page = self.session.get('https://www.universcine.com/login')
        csrf_token = None
        
        if login_page.status_code == 200:
            match = re.search(r'name="_token"\s+value="([^"]+)"', login_page.text)
            if match:
                csrf_token = match.group(1)
                self.log.debug(f" + CSRF token: {csrf_token[:30]}...")
        
        if not csrf_token:
            csrf_token = ""
        
        # Login data
        login_data = {
            '_username': self.credentials.username,
            '_password': self.credentials.password,
            '_token': csrf_token,
            '_submit': 'Connexion',
            '_target_path': 'https://www.universcine.com/home-abonnement'
        }
        
        # Login headers
        login_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.universcine.com',
            'Referer': 'https://www.universcine.com/login',
        }
        
        # Perform login
        response = self.session.post(
            'https://www.universcine.com/login_check',
            data=login_data,
            headers=login_headers,
            allow_redirects=False
        )
        
        if response.status_code != 302:
            self.log.error(f"Login failed: {response.status_code}")
            self.log.error(f"Response: {response.text[:500]}")
            raise Exception("Login failed")
        
        self.log.info(" + Login successful!")
        
        # Follow redirect to fully establish cookies
        redirect_url = response.headers.get('Location', 'https://www.universcine.com/home-abonnement')
        self.session.get(redirect_url)
        
        # Save token (actually just mark as authenticated)
        token = "authenticated_via_cookies"
        
        # Save to cache
        os.makedirs(os.path.dirname(tokens_cache_path), exist_ok=True)
        with open(tokens_cache_path, "w", encoding="utf-8") as fd:
            json.dump(token, fd)
        
        return token