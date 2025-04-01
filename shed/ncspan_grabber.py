import threading
import time
import logging
import argparse
from datetime import datetime, UTC
from yt_dlp import YoutubeDL
import os
import shutil

# --- Logging setup ---
def setup_logging(debug: bool, log_file: str = None):
    level = logging.DEBUG if debug else logging.INFO
    logger = logging.getLogger("yt_dlp_logger")
    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# --- yt-dlp Logger bridge ---
class YtDlpLogger:
    def __init__(self, logger):
        self.logger = logger

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

# --- Stream checker ---
def is_stream_live_or_exit(url, logger):
    ydl_opts = {
        'quiet': True,
        'force_generic_extractor': True,
    }
    with YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as e:
            logger.warning(f"Could not extract info. Assuming generic stream. Reason: {e}")
            return "generic"

        extractor = info.get('extractor')
        is_live = info.get('is_live')
        was_live = info.get('was_live')
        scheduled_ts = info.get('release_timestamp')

        if extractor == 'generic':
            logger.info("Generic stream detected (e.g. audio/mpeg). Proceeding without live checks.")
            return "generic"

        if is_live and not was_live:
            if scheduled_ts:
                scheduled_time = datetime.fromtimestamp(scheduled_ts, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
                logger.info(f"Stream is scheduled to go live at {scheduled_time}. Exiting.")
            else:
                logger.info("Stream is scheduled but no start time is available. Exiting.")
            exit(0)

        elif is_live and was_live:
            logger.info("Stream has already ended. Exiting.")
            exit(0)

        elif not is_live:
            logger.info("Video is not a live stream. Proceeding anyway.")

        return "structured"

# --- Download function ---
def download_with_timeout(url, output_path, duration, logger, free_formats=False, max_resolution=None, legacy_server_connect=False):
    start_time = datetime.now(UTC)
    stop_flag = threading.Event()

    # Construct format selector for resolution constraint
    if max_resolution:
        format_selector = f"bestvideo[height<={max_resolution}]+bestaudio/best[height<={max_resolution}]/best"
    else:
        format_selector = "best"

    ydl_opts = {
        'outtmpl': output_path,
        'format': format_selector,
        'noplaylist': True,
        'live_from_start': True,
        'prefer_free_formats': free_formats,
        'legacyserverconnect': legacy_server_connect,
        'logger': YtDlpLogger(logger),
        'progress_hooks': [
            lambda d: stop_flag.is_set() and (_ for _ in ()).throw(Exception("Timeout reached"))
        ],
    }

    def run_downloader():
        with YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([url])
            except Exception as e:
                logger.warning(f"Download interrupted: {e}")

    thread = threading.Thread(target=run_downloader)
    thread.start()

    thread.join(timeout=duration)

    if thread.is_alive():
        logger.info(f"Stopping download after {duration} seconds.")
        stop_flag.set()
        thread.join()

    end_time = datetime.now(UTC)
    logger.info(f"Download complete. Started at {start_time}, ended at {end_time}")

    return start_time, end_time

# --- Write Internet Archive files ---
def write_internet_archive_metadata_files(output_dir, identifier, filename, title=None, description=None, creator=None, license_url=None):
    files_xml_path = os.path.join(output_dir, f"{identifier}_files.xml")
    meta_xml_path = os.path.join(output_dir, f"{identifier}_meta.xml")

    with open(files_xml_path, "w") as f:
        f.write(f"""<files>
  <file name=\"{filename}\"/>
</files>
""")

    with open(meta_xml_path, "w") as f:
        f.write(f"""<metadata>
  <title>{title or identifier}</title>
  <identifier>{identifier}</identifier>
  <description>{description or ''}</description>
  <creator>{creator or ''}</creator>
  <licenseurl>{license_url or ''}</licenseurl>
  <mediatype>movies</mediatype>
</metadata>
""")

# --- Filename builder ---
def generate_timestamped_output_template(start: datetime, end: datetime, prefix: str, ext_template: str = "%(ext)s"):
    start_str = start.strftime("%Y%m%dT%H%M%S")
    end_str = end.strftime("%Y%m%dT%H%M%S")
    return f"{prefix}live_{start_str}_to_{end_str}.{ext_template}"

# --- Main entrypoint ---
def main():
    parser = argparse.ArgumentParser(description="Download a YouTube live stream or direct media stream with timeout.")
    parser.add_argument("url", help="YouTube live stream URL or generic audio/video stream")
    parser.add_argument("-o", "--output", help="Explicit output filename template (e.g. video.%%(ext)s)")
    parser.add_argument("-d", "--duration", type=int, default=60, help="Max download duration in seconds")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", help="Optional log file path")
    parser.add_argument("--prefix", default="", help="Custom prefix for the filename")
    parser.add_argument("--output-dir", default=".", help="Directory to save the downloaded file")
    parser.add_argument("--free-formats", action="store_true", help="Prefer patent-unencumbered formats (VP9/Opus)")
    parser.add_argument("--max-resolution", type=int, help="Maximum video resolution (e.g. 720, 480)")
    parser.add_argument("--legacy-server-connect", action="store_true", help="Use legacy server connection method")
    parser.add_argument("--ia-title", help="Internet Archive title")
    parser.add_argument("--ia-description", help="Internet Archive description")
    parser.add_argument("--ia-creator", help="Internet Archive creator")
    parser.add_argument("--ia-license", help="Internet Archive license URL")

    args = parser.parse_args()
    logger = setup_logging(debug=args.debug, log_file=args.log_file)

    os.makedirs(args.output_dir, exist_ok=True)
    logger.info("Starting download...")

    if 'youtube.com' in args.url or 'youtu.be' in args.url:
        stream_type = is_stream_live_or_exit(args.url, logger)
    else:
        logger.info("Non-YouTube URL detected. Skipping live stream check.")
        stream_type = "generic"

    if not args.output:
        temp_output = os.path.join(args.output_dir, "temp.%(ext)s")
        start, end = download_with_timeout(args.url, temp_output, args.duration, logger,
                                           free_formats=args.free_formats,
                                           max_resolution=args.max_resolution,
                                           legacy_server_connect=args.legacy_server_connect)
        final_template = generate_timestamped_output_template(start, end, prefix=args.prefix)

        for file in os.listdir(args.output_dir):
            if file.startswith("temp.") and not file.endswith(".part"):
                ext = os.path.splitext(file)[1]
                final_name = final_template.replace("%(ext)s", ext.lstrip("."))
                old_path = os.path.join(args.output_dir, file)
                new_path = os.path.join(args.output_dir, final_name)
                shutil.move(old_path, new_path)
                logger.info(f"Renamed {file} → {final_name}")
                identifier = os.path.splitext(final_name)[0]
                write_internet_archive_metadata_files(
                    args.output_dir, identifier, final_name,
                    title=args.ia_title,
                    description=args.ia_description,
                    creator=args.ia_creator,
                    license_url=args.ia_license
                )
    else:
        output_path = os.path.join(args.output_dir, args.output)
        start, end = download_with_timeout(args.url, output_path, args.duration, logger,
                                           free_formats=args.free_formats,
                                           max_resolution=args.max_resolution,
                                           legacy_server_connect=args.legacy_server_connect)
        identifier = os.path.splitext(os.path.basename(args.output))[0]
        write_internet_archive_metadata_files(
            args.output_dir, identifier, os.path.basename(args.output),
            title=args.ia_title,
            description=args.ia_description,
            creator=args.ia_creator,
            license_url=args.ia_license
        )

if __name__ == "__main__":
    main()
