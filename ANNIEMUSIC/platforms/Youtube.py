import asyncio
import contextlib
import json
import os
import re
import time
import aiohttp
import shutil
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from py_yt import VideosSearch

from ANNIEMUSIC.utils.cookie_handler import COOKIE_PATH
from ANNIEMUSIC.utils.database import is_on_off
from ANNIEMUSIC.utils.downloader import download_audio_concurrent, yt_dlp_download
from ANNIEMUSIC.utils.errors import capture_internal_err
from ANNIEMUSIC.utils.formatters import time_to_seconds
from ANNIEMUSIC.utils.tuning import (
    YTDLP_TIMEOUT,
    YOUTUBE_META_MAX,
    YOUTUBE_META_TTL,
)
from ANNIEMUSIC import LOGGER

_cache: Dict[str, Tuple[float, List[Dict]]] = {}
_cache_lock = asyncio.Lock()
_formats_cache: Dict[str, Tuple[float, List[Dict], str]] = {}
_formats_lock = asyncio.Lock()

# API URL System - DYNAMIC
YOUR_API_URL = None
FALLBACK_API_URL = "https://shrutibots.site"

# Rate limiting protection
_request_timestamps = []
_RATE_LIMIT_WINDOW = 60  # 60 seconds window
_MAX_REQUESTS_PER_WINDOW = 10  # Max 10 requests per minute

async def load_api_url():
    global YOUR_API_URL
    logger = LOGGER("ANNIEMUSIC.platforms.Youtube.py")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://pastebin.com/raw/rLsBhAQa", timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    content = await response.text()
                    YOUR_API_URL = content.strip()
                    logger.info(f"✅ API URL loaded successfully: {YOUR_API_URL}")
                else:
                    YOUR_API_URL = FALLBACK_API_URL
                    logger.info("ℹ️ Using fallback API URL")
    except Exception:
        YOUR_API_URL = FALLBACK_API_URL
        logger.info("ℹ️ Using fallback API URL")
    
    return YOUR_API_URL

# Initialize API URL on startup
try:
    loop = asyncio.get_event_loop()
    if loop.is_running():
        asyncio.create_task(load_api_url())
    else:
        loop.run_until_complete(load_api_url())
except RuntimeError:
    pass

def _cookiefile_path() -> Optional[str]:
    path = str(COOKIE_PATH)
    try:
        if path and os.path.exists(path) and os.path.getsize(path) > 0:
            return path
    except Exception:
        pass
    return None

def _cookies_args() -> List[str]:
    p = _cookiefile_path()
    return ["--cookies", p] if p else []

async def _exec_proc(*args: str) -> Tuple[bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return b"", b"timeout"

# Rate limiting check
def _check_rate_limit():
    global _request_timestamps
    now = time.time()
    
    # Remove timestamps older than our window
    _request_timestamps = [ts for ts in _request_timestamps if now - ts < _RATE_LIMIT_WINDOW]
    
    # Check if we've exceeded the limit
    if len(_request_timestamps) >= _MAX_REQUESTS_PER_WINDOW:
        sleep_time = _RATE_LIMIT_WINDOW - (now - _request_timestamps[0])
        time.sleep(sleep_time)
        _request_timestamps = []  # Reset after sleep
    
    # Add current timestamp
    _request_timestamps.append(now)

# Helper function for stream download
async def _download_from_stream(session: aiohttp.ClientSession, stream_url: str, file_path: str, video_id: str) -> Optional[str]:
    """Download from stream URL"""
    try:
        async with session.get(
            stream_url,
            timeout=aiohttp.ClientTimeout(total=120)  # 2 minutes
        ) as file_response:
            if file_response.status != 200:
                return None
            
            with open(file_path, "wb") as f:
                async for chunk in file_response.content.iter_chunked(16384):
                    f.write(chunk)
            
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                return file_path
            else:
                return None
    except Exception:
        return None

# TELEGRAM DOWNLOAD
async def get_telegram_file(telegram_link: str, video_id: str, file_type: str) -> str:
    """
    TELEGRAM DOWNLOAD
    """
    try:
        extension = ".webm" if file_type == "audio" else ".mkv"
        file_path = os.path.join("downloads", f"{video_id}{extension}")
        
        # LOCAL FILE CHECK
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            return file_path
        
        parsed = urlparse(telegram_link)
        parts = parsed.path.strip("/").split("/")
        
        if len(parts) < 2:
            return None
            
        channel_name = parts[0]
        message_id = int(parts[1])
        
        from ANNIEMUSIC import app
        
        max_retries = 1
        for attempt in range(max_retries):
            try:
                timeout_msg = 6.0
                timeout_download = 12.0
                
                # GET MESSAGE
                msg = await asyncio.wait_for(
                    app.get_messages(channel_name, message_id), 
                    timeout=timeout_msg
                )
                
                if not msg or not msg.document and not msg.video and not msg.audio:
                    return None
                
                os.makedirs("downloads", exist_ok=True)
                
                # DOWNLOAD
                await asyncio.wait_for(
                    msg.download(file_name=file_path),
                    timeout=timeout_download
                )
                
                # FILE VERIFICATION
                if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
                    print("✅ Telegram download")
                    return file_path
                else:
                    return None
                    
            except asyncio.TimeoutError:
                return None
            except Exception:
                return None
        
        return None
        
    except Exception:
        return None

# API DOWNLOAD FUNCTIONS - PRIORITY 1
async def download_song(link: str) -> str:
    """API se audio download (PRIORITY 1)"""
    global YOUR_API_URL
    
    if not YOUR_API_URL:
        await load_api_url()
        if not YOUR_API_URL:
            YOUR_API_URL = FALLBACK_API_URL
    
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp3")

    if os.path.exists(file_path):
        return file_path

    try:
        async with aiohttp.ClientSession() as session:
            params = {"url": video_id, "type": "audio"}
            
            async with session.get(
                f"{YOUR_API_URL}/download",
                params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                download_token = data.get("download_token")
                
                if not download_token:
                    return None
                
                stream_url = f"{YOUR_API_URL}/stream/{video_id}?type=audio"
                
                async with session.get(
                    stream_url,
                    headers={"X-Download-Token": download_token},
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as file_response:
                    if file_response.status != 200:
                        return None
                        
                    with open(file_path, "wb") as f:
                        async for chunk in file_response.content.iter_chunked(16384):
                            f.write(chunk)
                    
                    return file_path

    except Exception:
        return None

async def download_video_api(link: str) -> str:
    """API se video download (PRIORITY 1)"""
    global YOUR_API_URL
    
    if not YOUR_API_URL:
        await load_api_url()
        if not YOUR_API_URL:
            YOUR_API_URL = FALLBACK_API_URL
    
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")

    if os.path.exists(file_path):
        return file_path

    try:
        async with aiohttp.ClientSession() as session:
            params = {"url": video_id, "type": "video"}
            
            async with session.get(
                f"{YOUR_API_URL}/download",
                params=params,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                download_token = data.get("download_token")
                
                if not download_token:
                    return None
                
                stream_url = f"{YOUR_API_URL}/stream/{video_id}?type=video"
                
                async with session.get(
                    stream_url,
                    headers={"X-Download-Token": download_token},
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as file_response:
                    if file_response.status != 200:
                        return None
                        
                    with open(file_path, "wb") as f:
                        async for chunk in file_response.content.iter_chunked(16384):
                            f.write(chunk)
                    
                    return file_path

    except Exception as e:
        print(f"❌ API video download error: {str(e)}")
        return None

# YT-DLP DOWNLOAD FUNCTIONS - FALLBACK
async def download_video_ytdlp(link: str) -> str:
    """
    Download video using yt-dlp directly with cookies (FALLBACK METHOD)
    """
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.mp4")

    if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
        return file_path

    # Rate limiting check
    _check_rate_limit()
    
    try:
        # Prepare yt-dlp options with cookies
        ytdlp_opts = [
            "yt-dlp",
            *(_cookies_args()),
            "--no-warnings",
            "--geo-bypass",
            "--force-ipv4",
            "-f",
            "best[height<=?720][width<=?1280]/best",
            "-o",
            file_path,
            link
        ]
        
        # Execute yt-dlp command
        stdout, stderr = await _exec_proc(*ytdlp_opts)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            return file_path
        else:
            # Try alternative formats if first attempt fails
            alternative_formats = [
                "best[ext=mp4]",
                "best",
                "worst[ext=mp4]",
                "worst"
            ]
            
            for fmt in alternative_formats:
                try:
                    ytdlp_opts = [
                        "yt-dlp",
                        *(_cookies_args()),
                        "--no-warnings",
                        "--geo-bypass",
                        "--force-ipv4",
                        "-f",
                        fmt,
                        "-o",
                        file_path,
                        link
                    ]
                    
                    stdout, stderr = await _exec_proc(*ytdlp_opts)
                    
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
                        return file_path
                    
                    await asyncio.sleep(1)
                except Exception:
                    continue
            
            return None

    except Exception as e:
        return None

async def download_audio_ytdlp(link: str) -> str:
    """
    Download audio using yt-dlp with proper cookies (FALLBACK METHOD)
    """
    video_id = link.split('v=')[-1].split('&')[0] if 'v=' in link else link

    if not video_id or len(video_id) < 3:
        return None

    DOWNLOAD_DIR = "downloads"
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.webm")

    if os.path.exists(file_path):
        return file_path

    # Rate limiting check
    _check_rate_limit()
    
    try:
        # Prepare yt-dlp options with cookies for audio
        ytdlp_opts = [
            "yt-dlp",
            *(_cookies_args()),
            "--no-warnings",
            "--geo-bypass",
            "--force-ipv4",
            "-f",
            "bestaudio[ext=webm]/bestaudio",
            "--extract-audio",
            "--audio-format", "webm",
            "-o",
            file_path,
            link
        ]
        
        # Execute yt-dlp command
        stdout, stderr = await _exec_proc(*ytdlp_opts)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 10240:
            return file_path
        else:
            # Try alternative audio formats
            alternative_formats = [
                "bestaudio[ext=m4a]/bestaudio",
                "bestaudio/best",
                "worstaudio"
            ]
            
            for fmt in alternative_formats:
                try:
                    alt_file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.webm")
                    ytdlp_opts = [
                        "yt-dlp",
                        *(_cookies_args()),
                        "--no-warnings",
                        "--geo-bypass",
                        "--force-ipv4",
                        "-f",
                        fmt,
                        "--extract-audio",
                        "--audio-format", "webm",
                        "-o",
                        alt_file_path,
                        link
                    ]
                    
                    stdout, stderr = await _exec_proc(*ytdlp_opts)
                    
                    if os.path.exists(alt_file_path) and os.path.getsize(alt_file_path) > 10240:
                        return alt_file_path
                    
                    await asyncio.sleep(1)
                except Exception:
                    continue
            
            return None

    except Exception as e:
        return None

# Main download functions with API priority
async def download_audio(link: str) -> str:
    """
    Main audio download - API first, then yt-dlp fallback
    """
    # 1. TRY API FIRST (PRIORITY)
    print("🔄 Trying API for audio download...")
    api_result = await download_song(link)
    if api_result:
        print("✅ Audio downloaded via API")
        return api_result
    
    # 2. TRY YT-DLP FALLBACK
    print("🔄 API failed, trying yt-dlp fallback...")
    ytdlp_result = await download_audio_ytdlp(link)
    if ytdlp_result:
        print("✅ Audio downloaded via yt-dlp fallback")
        
        # Convert webm to mp3 if needed
        if ytdlp_result.endswith('.webm'):
            mp3_path = ytdlp_result.replace('.webm', '.mp3')
            try:
                shutil.move(ytdlp_result, mp3_path)
                return mp3_path
            except:
                return ytdlp_result
        return ytdlp_result
    
    print("❌ Both API and yt-dlp failed for audio")
    return None

async def download_video(link: str) -> str:
    """
    Main video download - API first, then yt-dlp fallback
    """
    # 1. TRY API FIRST (PRIORITY)
    print("🔄 Trying API for video download...")
    api_result = await download_video_api(link)
    if api_result:
        print("✅ Video downloaded via API")
        return api_result
    
    # 2. TRY YT-DLP FALLBACK
    print("🔄 API failed, trying yt-dlp fallback...")
    ytdlp_result = await download_video_ytdlp(link)
    if ytdlp_result:
        print("✅ Video downloaded via yt-dlp fallback")
        return ytdlp_result
    
    print("❌ Both API and yt-dlp failed for video")
    return None

# Original API DOWNLOAD (kept for backward compatibility)
async def download_via_api(link: str, download_type: str = "audio") -> Optional[str]:
    """API DOWNLOAD"""
    
    if download_type == "video":
        # Use video download function with fallback
        return await download_video(link)
    
    # Use new audio download function
    return await download_audio(link)

@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    key = f"q:{query}"
    now = time.time()
    async with _cache_lock:
        if key in _cache:
            ts, val = _cache[key]
            if now - ts < YOUTUBE_META_TTL:
                return val
            _cache.pop(key, None)
        if len(_cache) > YOUTUBE_META_MAX:
            _cache.clear()
    try:
        data = await VideosSearch(query, limit=1).next()
        result = data.get("result", [])
    except Exception:
        result = []
    if result:
        async with _cache_lock:
            _cache[key] = (now, result)
    return result

# Shell command function from Test.py
import shlex
import asyncio

async def shell_cmd(cmd):

    # Block dangerous characters (command injection protection)
    if any(x in cmd for x in [";", "&", "|", "$", "`"]):
        return "Unsafe command blocked"

    proc = await asyncio.create_subprocess_exec(
        *shlex.split(cmd),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    out, errorz = await proc.communicate()

    if errorz:
        if "unavailable videos are hidden" in errorz.decode("utf-8").lower():
            return out.decode("utf-8")
        else:
            return errorz.decode("utf-8")

    return out.decode("utf-8")

class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self.status = "https://www.youtube.com/oembed?url="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    def _prepare_link(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()
        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        return link.split("&")[0]

    # URL extraction method
    @capture_internal_err
    async def url(self, message: Message) -> Optional[str]:
        """
        Extract YouTube URL from message
        """
        msgs = [message] + (
            [message.reply_to_message] if message.reply_to_message else []
        )
        for msg in msgs:
            text = msg.text or msg.caption or ""
            entities = msg.entities or msg.caption_entities or []
            for ent in entities:
                if ent.type == MessageEntityType.URL:
                                print("✅ Local cache")
            return common_file_path, True

        # VIDEO KE LIYE
        if songvideo or video:
            # Try our new download_video function (API first, then yt-dlp)
            try:
                downloaded_file = await download_video(link)
                if downloaded_file:
                    print("✅ Video downloaded successfully")
                    
                    # Convert to common file path if needed
                    if downloaded_file != common_file_path and downloaded_file.endswith('.mp4'):
                        try:
                            shutil.move(downloaded_file, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return downloaded_file, True
                    return downloaded_file, True
            except Exception as e:
                print(f"❌ Video download error: {str(e)}")
            
            # Fallback to direct stream
            status, stream_url = await self.video(link)
            if status == 1:
                print("✅ Video stream")
                return stream_url, None
            else:
                return None, None

        # AUDIO KE LIYE - ALL METHODS IN ORDER
        else:
            # 1. TRY OUR NEW download_audio FUNCTION (API FIRST)
            try:
                audio_result = await download_audio(link)
                if audio_result:
                    print("✅ Audio downloaded successfully")
                    
                    # Convert .mp3 to .webm if needed
                    if audio_result.endswith('.mp3') and common_file_path.endswith('.webm'):
                        mp3_path = audio_result
                        webm_path = common_file_path
                        try:
                            shutil.move(mp3_path, webm_path)
                            return webm_path, True
                        except Exception:
                            return audio_result, True
                    return audio_result, True
            except Exception as e:
                print(f"❌ Audio download error: {str(e)}")
            
            # 2. TRY OLD API METHOD
            api_result = await download_via_api(link, "audio")
            if api_result:
                return api_result, True
            
            # 3. TRY YT-DLP (DIRECT - NO is_on_off CONDITION)
            try:
                p = await yt_dlp_download(link, type="audio")
                if p and os.path.exists(p) and os.path.getsize(p) > 10240:
                    print("✅ yt-dlp (original)")
                    
                    # MOVE TO COMMON LOCATION IF NEEDED
                    if p != common_file_path:
                        try:
                            shutil.move(p, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return p, True
                    return p, True
            except Exception as e:
                print(f"❌ Original yt-dlp error: {str(e)}")
            
            # 4. TRY CONCURRENT AS LAST RESORT
            try:
                p = await download_audio_concurrent(link)
                if p and os.path.exists(p) and os.path.getsize(p) > 10240:
                    print("✅ concurrent")
                    
                    # MOVE TO COMMON LOCATION IF NEEDED
                    if p != common_file_path:
                        try:
                            shutil.move(p, common_file_path)
                            return common_file_path, True
                        except Exception:
                            return p, True
                    return p, True
            except Exception as e:
                print(f"❌ Concurrent download error: {str(e)}")
            
            # 5. ALL FAILED
            print("❌ All audio download methods failed")
            return None, None
