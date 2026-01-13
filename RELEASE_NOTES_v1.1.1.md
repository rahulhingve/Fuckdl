# FuckDL v1.1.1 Release Notes

## Release v1.1.1

This release includes CDM device configuration updates for Amazon Prime Video.

## What's New in v1.1.1

### Amazon Prime Video Updates
- 🔧 **Updated**: Amazon Prime Video now uses `hisense_smarttv_hu32e5600fhwv_SL3000` CDM device (SL3000)
- ✨ Improved compatibility with Amazon Prime Video's DRM requirements
- 🎯 Better support for high-quality content downloads

### Previous Updates (v1.1.0)
- 🔧 **Fixed**: AttributeError when downloader config is missing
- ✅ Added default downloader configuration (n_m3u8dl-re)
- 🛡️ Improved error handling for missing config attributes
- 🔧 **Fixed**: Amazon Prime Video now uses new endpoints for better reliability
- 🎬 **New**: Full support for 4K ISM (Smooth Streaming) downloads

## Features

- ✅ Support for 30+ streaming services
- ✅ Playready and Widevine DRM decryption
- ✅ Multiple downloader support (N_m3u8DL-RE, Aria2c, saldl)
- ✅ CDM device support (SL2000/SL3000, WVD files)
- ✅ Comprehensive command-line interface
- ✅ Multiple quality and codec options
- ✅ Subtitle and audio track selection
- ✅ Episode range selection
- ✅ Proxy support
- ✅ Key vault integration
- ✅ **Amazon Prime Video optimized with SL3000 CDM device**
- ✅ **Support for 4K ISM downloads**

## Supported Services

- All4
- Amazon Prime Video
- Apple TV Plus
- BBC iPlayer
- BritBox
- Crave
- Disney Plus
- Discovery Plus
- HBO Max
- Hulu
- iTunes
- ITV
- Movies Anywhere
- MY5
- Netflix
- Now TV (IT/UK)
- Paramount Plus
- Peacock
- Pluto TV
- Rakuten TV
- Roku
- Skyshowtime
- Stan
- TUBI
- Videoland
- WowTV

## Installation

```bash
poetry install
```

## Quick Start

```bash
# Get help
poetry run fuckdl dl --help

# Download from Amazon Prime Video (now with SL3000 CDM)
poetry run fuckdl dl -al en -sl en -q 2160 Amazon https://www.primevideo.com/...

# Download 4K HDR from Amazon
poetry run fuckdl dl -al en -sl en -q 2160 -r HDR -v H265 Amazon https://www.primevideo.com/...
```

## Documentation

See `HOW_TO_USE.md` for complete usage guide with all command examples.

## CDM Devices Included

- Genius Fashion GAE TV Smart TV (SL3000)
- Hisense SmartTV HU32E5600FHWV (SL3000) - **Now default for Amazon**
- Xiaomi Mi A1 (WVD)

## Created By

**Barbie DRM**  
https://t.me/barbiedrm

## Repository

https://github.com/chromedecrypt/Fuckdl
