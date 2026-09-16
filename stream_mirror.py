import os
import re
import time
import asyncio
import argparse
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "YOUR_R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "YOUR_R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "YOUR_R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "YOUR_R2_BUCKET_NAME")
CUSTOM_STREAM_DOMAIN = os.getenv("CUSTOM_STREAM_DOMAIN", "https://phyhunt.org")

boto_config = Config(max_pool_connections=50, retries={'max_attempts': 3})
s3 = boto3.client(
    service_name='s3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name='auto',
    config=boto_config
)

def upload_to_r2(local_path, r2_key, content_type, is_m3u8=False, is_final=False):
    extra_args = {'ContentType': content_type}
    if is_m3u8:
        if is_final:
            extra_args['CacheControl'] = 'public, max-age=2592000, s-maxage=2592000'
        else:
            extra_args['CacheControl'] = 'public, max-age=2, s-maxage=2, must-revalidate'
    else:
        extra_args['CacheControl'] = 'public, max-age=86400, s-maxage=86400'

    try:
        s3.upload_file(local_path, R2_BUCKET_NAME, r2_key, ExtraArgs=extra_args)
    except Exception as e:
        print(f"❌ Upload Failed for {r2_key}: {e}")

def r2_live_uploader(local_dir, r2_folder, stop_event):
    uploaded_ts_files = set()
    last_m3u8_mtime = {}

    print("🚀 CPU Uploader Watchdog Started (20 Threads)...")

    with ThreadPoolExecutor(max_workers=20) as executor:
        while not stop_event.is_set():
            if not os.path.exists(local_dir):
                time.sleep(0.5)
                continue

            try:
                files = os.listdir(local_dir)
            except Exception:
                time.sleep(0.5)
                continue

            ts_files = [f for f in files if f.endswith('.ts')]
            for ts in ts_files:
                if ts not in uploaded_ts_files and not ts.endswith('.tmp'):
                    uploaded_ts_files.add(ts)
                    local_path = os.path.join(local_dir, ts)
                    r2_key = f"{r2_folder}/{ts}"
                    executor.submit(upload_to_r2, local_path, r2_key, 'video/MP2T', False)

            m3u8_files = [f for f in files if f.endswith('.m3u8') and not f.endswith('.tmp')]
            for m3u8 in m3u8_files:
                local_path = os.path.join(local_dir, m3u8)
                try:
                    current_mtime = os.path.getmtime(local_path)
                    if current_mtime != last_m3u8_mtime.get(m3u8):
                        last_m3u8_mtime[m3u8] = current_mtime
                        r2_key = f"{r2_folder}/{m3u8}"
                        executor.submit(upload_to_r2, local_path, r2_key, 'application/vnd.apple.mpegurl', True, False)
                except FileNotFoundError:
                    pass

            uploaded_ts_files.intersection_update(ts_files)
            time.sleep(0.5)

async def get_streamyard_id(watch_url):
    print(f"🚀 Launching Playwright Detector for Watch Link: {watch_url}")
    stream_id = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36")
        page = await context.new_page()

        def handle_request(request):
            nonlocal stream_id
            url = request.url
            if "streamyard-video-delivery.global.ssl.fastly.net/live/" in url:
                match = re.search(r'/live/([^/]+)', url)
                if match:
                    # Clean Stream ID extraction
                    stream_id = match.group(1).split('/')[0].split('?')[0]

        page.on("request", handle_request)
        try:
            await page.goto(watch_url, wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass

        retries = 900
        while not stream_id and retries > 0:
            await asyncio.sleep(2)
            retries -= 1
        await browser.close()
        return stream_id

def process_live_stream(live_url, stream_slug):
    local_hls_dir = f"live_{stream_slug}"
    r2_folder = f"live/{stream_slug}"
    os.makedirs(local_hls_dir, exist_ok=True)

    print(f"\n📡 Starting FFmpeg CPU Transcoding for: {stream_slug}")
    print(f"🔗 Audience Link: {CUSTOM_STREAM_DOMAIN}/{r2_folder}/master.m3u8\n")

    stop_event = threading.Event()
    uploader_thread = threading.Thread(target=r2_live_uploader, args=(local_hls_dir, r2_folder, stop_event))
    uploader_thread.start()

    max_retries = 15
    retry_count = 0
    
    # Hide annoying ffmpeg logs but keep errors
    cmd = [
        "ffmpeg", "-loglevel", "error", "-re",
        "-i", live_url,
        "-filter_complex",
        "[0:v]split=2[v1][v2];"
        "[v1]scale=w=1280:h=720[v720];"
        "[v2]scale=w=640:h=360[v360]",

        # 720p Output
        "-map", "[v720]", "-map", "0:a?",
        "-c:v:0", "libx264", "-preset", "ultrafast",
        "-b:v:0", "2500k", "-g", "120",

        # 360p Output
        "-map", "[v360]", "-map", "0:a?",
        "-c:v:1", "libx264", "-preset", "ultrafast",
        "-b:v:1", "800k", "-g", "120",

        "-c:a", "aac", "-b:a", "128k",

        "-f", "hls",
        "-hls_time", "4",
        "-hls_list_size", "0",
        "-hls_playlist_type", "event",
        "-hls_flags", "temp_file+append_list",
        "-master_pl_name", "master.m3u8",
        "-var_stream_map", "v:0,a:0,name:720p v:1,a:1,name:360p",
        os.path.join(local_hls_dir, "stream_%v.m3u8")
    ]

    while retry_count < max_retries and not stop_event.is_set():
        try:
            if retry_count > 0:
                print(f"🔄 Attempting to reconnect FFmpeg (Retry {retry_count}/{max_retries})...")
            
            subprocess.run(cmd, check=True)
            print("🏁 FFmpeg finished normally. Stream ended by host.")
            break

        except subprocess.CalledProcessError as e:
            print(f"⚠️ FFmpeg disconnected or crashed: {e}")
            retry_count += 1
            if retry_count < max_retries:
                print("⏳ Waiting 5 seconds before retrying...")
                time.sleep(5)
        except KeyboardInterrupt:
            print("\n🛑 Manual Stop (Ctrl+C) Triggered.")
            break
        except Exception as e:
            print(f"❌ Unexpected FFmpeg Error: {e}")
            break

    print("\n🧹 Live Ended! Finalizing M3U8 files...")
    time.sleep(3)

    stop_event.set()
    uploader_thread.join()

    print("🔄 Converting Live to VOD...")
    m3u8_files = [f for f in os.listdir(local_hls_dir) if f.endswith('.m3u8')]
    for m3u8 in m3u8_files:
        local_path = os.path.join(local_hls_dir, m3u8)
        r2_key = f"{r2_folder}/{m3u8}"
        upload_to_r2(local_path, r2_key, 'application/vnd.apple.mpegurl', is_m3u8=True, is_final=True)

    print("✅ VOD Conversion Complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--slug", required=True)
    args = parser.parse_args()

    stream_id = asyncio.run(get_streamyard_id(args.url))

    if stream_id:
        # Fixed Full Domain URL
        fastly_m3u8_url = f"https://streamyard-video-delivery.global.ssl.fastly.net/live/{stream_id}/stream_720p.m3u8"
        print(f"🎯 Target Acquired: {stream_id}")
        print(f"📡 Transcoding URL: {fastly_m3u8_url}")
        process_live_stream(fastly_m3u8_url, args.slug)
    else:
        print("❌ Stream ID পাওয়া যায়নি।")
