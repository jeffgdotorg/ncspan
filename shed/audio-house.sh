#!/bin/sh

yt-dlp --legacy-server-connect -o "NCGA_Audio_House_$(date +%s).%(ext)s" https://audio1.ncleg.gov/house
