import os
import re
import time
import asyncio
import argparse
import threading
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from playwright.async_api import async_playwright

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
CUSTOM_STREAM_DOMAIN = os.getenv("CUSTOM_STREAM_DOMAIN")

boto_config = Config(max_pool_connections=50, retries={'max_attempts': 3})
s3 = boto3.client(
    service_name='s3',
    endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    region_name='auto',
    config=boto_config
)

def get_fast_session():
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=Retry(total=3, backoff_factor=0.2))
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

# ─────────────────────────────────────────────────────────
# FIX 1: v2 Code-এর মতো নিখুঁত Cache-Control Header
# ─────────────────────────────────────────────────────────
def upload_bytes(data, r2_key, content_type, is_m3u8=False, is_final=False):
    extra_args = {'ContentType': content_type}
    if is_m3u8:
        if is_final:
            extra_args['CacheControl'] = 'public, max-age=2592000, s-maxage=2592000'
        else:
            # v2 এর মতো exact 2s cache-control (স্টেবল প্লেব্যাকের জন্য)
            extra_args['CacheControl'] = 'public, max-age=2, s-maxage=2, must-revalidate'
    else:
        extra_args['CacheControl'] = 'public, max-age=86400, s-maxage=86400'

    try:
        s3.put_object(Bucket=R2_BUCKET_NAME, Key=r2_key, Body=data, **extra_args)
    except Exception as e:
        print(f"\n❌ Upload Failed: {r2_key} - {e}")

def create_master_playlist(r2_folder):
    master_content = (
        "#EXTM3U\n"
        "#EXT-X-VERSION:3\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720\n"
        "720p/index.m3u8\n"
        "#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\n"
        "360p/index.m3u8\n"
    )
    upload_bytes(master_content.encode('utf-8'), f"{r2_folder}/master.m3u8", 'application/vnd.apple.mpegurl', is_m3u8=True, is_final=True)

def fetch_and_pipe_ts(ts_url, r2_key, session):
    try:
        res = session.get(ts_url, timeout=3)
        if res.status_code == 200:
            upload_bytes(res.content, r2_key, 'video/MP2T')
    except Exception:
        pass

# ─────────────────────────────────────────────────────────
# FIX 2: M3U8-এ Target Duration ও Sequence নাম্বার ঠিক করা
# ─────────────────────────────────────────────────────────
def mirror_stream_worker(stream_url, quality, r2_base_folder, stop_event):
    session = get_fast_session()
    uploaded_segments = set()
    all_seen_manifest = {}
    target_folder = f"{r2_base_folder}/{quality}"

    with ThreadPoolExecutor(max_workers=20) as executor:
        while not stop_event.is_set():
            try:
                res = session.get(stream_url, timeout=2)
                if res.status_code != 200:
                    time.sleep(0.5)
                    continue

                lines = res.text.splitlines()
                current_duration = 4.0
                updated_live = False

                for line in lines:
                    line_clean = line.strip()
                    if line_clean.startswith("#EXTINF:"):
                        try:
                            current_duration = float(line_clean.split(":")[1].split(",")[0])
                        except Exception:
                            current_duration = 4.0
                    elif line_clean and not line_clean.startswith("#") and ".ts" in line_clean:
                        ts_name = line_clean.split("?")[0].split("/")[-1]
                        
                        if ts_name not in all_seen_manifest:
                            all_seen_manifest[ts_name] = current_duration

                        if ts_name not in uploaded_segments:
                            uploaded_segments.add(ts_name)
                            updated_live = True
                            ts_full_url = urljoin(stream_url, line_clean)
                            r2_key = f"{target_folder}/{ts_name}"
                            executor.submit(fetch_and_pipe_ts, ts_full_url, r2_key, session)

                if updated_live:
                    live_lines = [
                        "#EXTM3U\n",
                        "#EXT-X-VERSION:3\n",
                        "#EXT-X-TARGETDURATION:6\n",
                        "#EXT-X-MEDIA-SEQUENCE:0\n",
                        "#EXT-X-PLAYLIST-TYPE:EVENT\n"
                    ]
                    
                    sorted_ts = sorted(
                        all_seen_manifest.keys(),
                        key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else x
                    )
                    
                    for ts in sorted_ts:
                        dur = all_seen_manifest[ts]
                        live_lines.append(f"#EXTINF:{dur:.6f},\n{ts}\n")

                    upload_bytes("".join(live_lines).encode('utf-8'), f"{target_folder}/index.m3u8", 'application/vnd.apple.mpegurl', is_m3u8=True)

                if "#EXT-X-ENDLIST" in res.text:
                    break

            except Exception:
                pass

            time.sleep(0.5)

    vod_lines = [
        "#EXTM3U\n",
        "#EXT-X-VERSION:3\n",
        "#EXT-X-TARGETDURATION:6\n",
        "#EXT-X-MEDIA-SEQUENCE:0\n",
        "#EXT-X-PLAYLIST-TYPE:VOD\n"
    ]
    sorted_ts = sorted(
        all_seen_manifest.keys(),
        key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else x
    )
    for ts in sorted_ts:
        dur = all_seen_manifest[ts]
        vod_lines.append(f"#EXTINF:{dur:.6f},\n{ts}\n")
    vod_lines.append("#EXT-X-ENDLIST\n")

    upload_bytes("".join(vod_lines).encode('utf-8'), f"{target_folder}/index.m3u8", 'application/vnd.apple.mpegurl', is_m3u8=True, is_final=True)

async def get_streamyard_id(watch_url):
    print(f"🚀 Launching Detector for Watch Link: {watch_url}")
    stream_id = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36")
        page = await context.new_page()

        def handle_request(request):
            nonlocal stream_id
            if "streamyard-video-delivery.global.ssl.fastly.net/live/" in request.url:
                match = re.search(r'/live/([^/]+)/', request.url)
                if match:
                    stream_id = match.group(1)

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

def run_mirror(stream_id, stream_slug, r2_base_folder):
    base_url = f"https://streamyard-video-delivery.global.ssl.fastly.net/live/{stream_id}"
    streams_dict = {
        "720p": f"{base_url}/stream_720p.m3u8",
        "360p": f"{base_url}/stream_360p.m3u8"
    }

    print(f"\n🚀 Stream Replicator Started | Stream ID: {stream_id}")
    print(f"🔗 Audience Link: {CUSTOM_STREAM_DOMAIN}/{r2_base_folder}/master.m3u8\n")

    stop_event = threading.Event()
    workers = []
    for quality, url in streams_dict.items():
        t = threading.Thread(target=mirror_stream_worker, args=(url, quality, r2_base_folder, stop_event))
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--slug", required=True)
    args = parser.parse_args()

    r2_base_folder = f"live/{args.slug}"
    print(f"📌 Pre-creating Master Playlist for: {args.slug}")
    create_master_playlist(r2_base_folder)

    stream_id = asyncio.run(get_streamyard_id(args.url))
    if stream_id:
        print(f"🎯 Target Acquired: {stream_id}")
        run_mirror(stream_id, args.slug, r2_base_folder)
    else:
        print("❌ Stream ID পাওয়া যায়নি।")
