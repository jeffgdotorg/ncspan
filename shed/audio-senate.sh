#!/bin/sh

yt-dlp --legacy-server-connect -o "NCGA_Audio_Senate_$(date +%s).%(ext)s" https://audio1.ncleg.gov/senate
