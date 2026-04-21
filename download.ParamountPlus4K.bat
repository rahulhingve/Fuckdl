@echo off
poetry run fuckdl dl -al en  -q 2160 -r HDR ParamountPlus -m https://www.paramountplus.com/movies/video/cA6RHYSln7H018wfcs_dtX0iOKatacNr/ --vcodec H265 --manifest-type HLS
pause