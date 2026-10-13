import os
import re
import time
import asyncio
import argparse
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor

import boto3
import requests
from botocore.config import Config
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "YOUR_R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "YOUR_R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "YOUR_R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "YOUR_R2_BUCKET_NAME")
CUSTOM_STREAM_DOMAIN = os.getenv("CUSTOM_STREAM_DOMAIN", "https://phyhunt.org").rstrip("/")

# Physics Hunters LMS: runs started by the site (repository_dispatch with video_id) report
# their progress here, and the R2 link goes into the lecture when the class ends.
# Manual runs (dashboard / workflow_dispatch) have no video_id and report nothing.
PH_CALLBACK_URL = os.getenv("PH_CALLBACK_URL", "")
PH_CALLBACK_TOKEN = os.getenv("PH_CALLBACK_TOKEN", "")
PH_VIDEO_ID = os.getenv("PH_VIDEO_ID", "")
RUN_URL = os.getenv("RUN_URL", "")


def report(slug, state, **extra):
    """state: waiting | live | no_live | failed | ended. Never raises."""
    if not (PH_CALLBACK_URL and PH_CALLBACK_TOKEN and PH_VIDEO_ID):
        return
    body = {"video_id": PH_VIDEO_ID, "slug": slug, "state": state, "run_url": RUN_URL, **extra}
    for attempt in range(5):
        try:
            r = requests.post(PH_CALLBACK_URL, json=body, timeout=15,
                              headers={"Authorization": f"Bearer {PH_CALLBACK_TOKEN}"})
            print(f"📨 PH callback {state}: HTTP {r.status_code}")
            if r.status_code < 500:
                return
        except Exception as e:
            print(f"⚠️ PH callback {state} failed: {e}")
        time.sleep(5 * (attempt + 1))

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
        extra_args['CacheControl'] = 'public, max-age=2592000, s-maxage=2592000'

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

async def get_streamyard_id(watch_url, wait_min):
    print(f"🚀 Launching Playwright Detector for Watch Link: {watch_url} (waiting up to {wait_min} min)")
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

        deadline = time.time() + wait_min * 60
        next_reload = time.time() + 300
        while not stream_id and time.time() < deadline:
            await asyncio.sleep(2)
            # A watch page opened long before the broadcast may not switch to it by itself.
            if not stream_id and time.time() >= next_reload:
                next_reload = time.time() + 300
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60000)
                except Exception:
                    pass
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
        if m3u8 != "master.m3u8":
            close_playlist(local_path)
        r2_key = f"{r2_folder}/{m3u8}"
        upload_to_r2(local_path, r2_key, 'application/vnd.apple.mpegurl', is_m3u8=True, is_final=True)

    print("✅ VOD Conversion Complete!")
    return len([f for f in os.listdir(local_hls_dir) if f.endswith('.ts')])


def close_playlist(path):
    """A playlist left open by a crashed FFmpeg plays as live (from the end): mark it finished."""
    with open(path) as f:
        text = f.read()
    text = text.replace("#EXT-X-PLAYLIST-TYPE:EVENT", "#EXT-X-PLAYLIST-TYPE:VOD")
    if "#EXT-X-ENDLIST" not in text:
        text = text.rstrip("\n") + "\n#EXT-X-ENDLIST\n"
    with open(path, "w") as f:
        f.write(text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--slug", required=True)
    parser.add_argument("--wait-min", type=float, default=30, help="minutes to wait for the broadcast to go live")
    args = parser.parse_args()
    public_url = f"{CUSTOM_STREAM_DOMAIN}/live/{args.slug}/master.m3u8"

    try:
        report(args.slug, "waiting")
        stream_id = asyncio.run(get_streamyard_id(args.url, args.wait_min))

        if stream_id:
            # Fixed Full Domain URL
            fastly_m3u8_url = f"https://streamyard-video-delivery.global.ssl.fastly.net/live/{stream_id}/stream_720p.m3u8"
            print(f"🎯 Target Acquired: {stream_id}")
            print(f"📡 Transcoding URL: {fastly_m3u8_url}")
            report(args.slug, "live", url=public_url)
            segments = process_live_stream(fastly_m3u8_url, args.slug)
            if segments:
                report(args.slug, "ended", url=public_url, segments=segments)
            else:
                report(args.slug, "failed", error="FFmpeg could not record any segment")
        else:
            print("❌ Stream ID পাওয়া যায়নি।")
            report(args.slug, "no_live")
    except BaseException as e:
        report(args.slug, "failed", error=f"{type(e).__name__}: {e}"[:300])
        raise
