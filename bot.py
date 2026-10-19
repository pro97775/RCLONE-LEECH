# ==============================================================================
#  Rclone ➔ Telegram Transfer Bot (100% Bot-Controlled Edition)
#  Engine: WZGram (Rust WarpCrypto) + Pipelined Rclone/MTProto + Motor MongoDB
# ==============================================================================
#  Step-by-Step Architecture:
#    Step  1: Bootstrap, uvloop Event Loop & WZGram Engine Loader
#    Step  2: Logging, FloodWait Log Coalescer & Ring Buffer
#    Step  3: Environment Loader, rclone.conf Bootstrap & File-Type Classifier
#    Step  4: Runtime State, Interactive Prompt State & Zero-Overhead Flood Guard
#    Step  5: MongoDB Persistence Engine (with Local JSON Fallback)
#    Step  6: Health Check HTTP Server & Graceful Shutdown/Restart Cleanup
#    Step  7: Clean UI Formatters & Visual Progress Components
#    Step  8: Bot Control Center Keyboards & Screen Builders (100% Bot Control)
#    Step  9: High-Speed Async Subprocess Engine (rclone / ffmpeg / ffprobe)
#    Step 10: Live Transfer Progress Board (_BoardState)
#    Step 11: Pipelined Transfer Engine (Parallel Download + Upload Semaphores)
#    Step 12: Interactive Remote, Folder & Single-File Browser
#    Step 13: Command Handlers, Interactive Input Handler & Callback Router
#    Step 14: Startup Lifecycle, Auto Command Registration & Auto-Resume Hook
# ==============================================================================

import os
import sys
import shlex
import asyncio
import inspect
import logging
import random
import shutil
import time
import threading
import re
import collections
import hashlib
import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

# ==============================================================================
#  STEP 1: Bootstrap, uvloop Event Loop & WZGram Engine Loader
# ==============================================================================
try:
    import uvloop
    uvloop.install()
except ImportError:
    uvloop = None

try:
    import psutil
except ImportError:
    psutil = None

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except ImportError:
    AsyncIOMotorClient = None

import wzgram as _tg_pkg
from wzgram import Client, filters, idle
from wzgram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
)
from wzgram.errors import FloodWait, MessageNotModified

try:
    from wzgram.types import ReplyParameters
except ImportError:
    ReplyParameters = None

MTPROTO_ENGINE = f"WZGram v{getattr(_tg_pkg, '__version__', 'latest')} (WarpCrypto)"

_SIG_CACHE: dict[Any, set[str]] = {}


def _get_method_params(method: Any) -> set[str]:
    func = getattr(method, "__func__", method)
    cached = _SIG_CACHE.get(func)
    if cached is not None:
        return cached
    try:
        names = set(inspect.signature(method).parameters.keys())
    except Exception:
        names = set()
    _SIG_CACHE[func] = names
    return names


def _build_reply_kwargs(method: Any, reply_to_message_id: int | None) -> dict[str, Any]:
    if not reply_to_message_id:
        return {}
    params = _get_method_params(method)
    if "reply_parameters" in params and ReplyParameters is not None:
        return {"reply_parameters": ReplyParameters(message_id=reply_to_message_id)}
    if "reply_to_message_id" in params:
        return {"reply_to_message_id": reply_to_message_id}
    if ReplyParameters is not None:
        return {"reply_parameters": ReplyParameters(message_id=reply_to_message_id)}
    return {"reply_to_message_id": reply_to_message_id}


def _build_thumb_kwargs(method: Any, thumb: str | None) -> dict[str, Any]:
    if not thumb:
        return {}
    params = _get_method_params(method)
    if "thumb" in params:
        return {"thumb": thumb}
    if "thumbnail" in params:
        return {"thumbnail": thumb}
    return {"thumb": thumb}


_REMOTE_PREFIX_RE = re.compile(r"^[A-Za-z0-9_.-]+:")


def _normalize_remote_path(remote_path: str) -> str:
    """Normalize `Remote:/folder/sub/` into `Remote:folder/sub` (or `Remote:`)."""
    s = remote_path.strip()
    if ":" not in s:
        return s.strip("/")
    r_prefix, r_rest = s.split(":", 1)
    r_rest = r_rest.strip("/")
    return f"{r_prefix.strip()}:{r_rest}" if r_rest else f"{r_prefix.strip()}:"


def _remote_basename(path_str: str, is_remote: bool = False) -> str:
    """
    Extract the filename from a path without stripping colons that belong
    to the filename itself (e.g. `Movie: Part 1.mp4`).
    """
    s = path_str.strip().rstrip("/")
    if is_remote and _REMOTE_PREFIX_RE.match(s):
        s = s.split(":", 1)[-1].lstrip("/")
    if "/" in s:
        name = s.rsplit("/", 1)[-1]
    else:
        name = s
    return name.replace("\x00", "").strip() or "file"


def _rclone_extra_flags() -> list[str]:
    with state_lock:
        raw = RCLONE_FLAGS
    if not raw:
        return []
    try:
        return shlex.split(raw)
    except ValueError:
        return raw.split()


def _extract_forwarded_chat_id(msg: Message) -> int | None:
    """Extract source channel/supergroup ID from a forwarded message across all Bot API / MTProto layers."""
    f_chat = getattr(msg, "forward_from_chat", None)
    if f_chat and getattr(f_chat, "id", None) is not None:
        return int(f_chat.id)
    f_origin = getattr(msg, "forward_origin", None)
    if f_origin:
        o_chat = getattr(f_origin, "chat", None) or getattr(f_origin, "sender_chat", None)
        if o_chat and getattr(o_chat, "id", None) is not None:
            return int(o_chat.id)
    return None


# ==============================================================================
#  STEP 2: Logging, FloodWait Log Coalescer & Ring Buffer
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rclone_bot")
log_buffer: collections.deque[str] = collections.deque(maxlen=150)

for _noisy_logger in (
    "wzgram",
    "wzgram.session",
    "wzgram.connection",
    "wzgram.client",
    "pymongo",
    "motor",
):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

_FLOOD_LOG_RE = re.compile(r"A wait of (\d+) seconds is required", re.IGNORECASE)


class _WzgramFloodDedupFilter(logging.Filter):
    """
    Coalesces WZGram's internal `Retrying upload part ... FLOOD_WAIT_X` warnings
    and notifies `_flood_guard` once per record so parallel chunk workers stagger.
    """

    def __init__(self) -> None:
        super().__init__()
        self._last_flood_log = 0.0
        self._suppressed = 0

    def filter(self, record: logging.LogRecord) -> bool:
        cached_decision = getattr(record, "_flood_decision", None)
        if cached_decision is not None:
            return cached_decision

        msg = record.getMessage()
        if "Retrying upload part" in msg and "FLOOD_WAIT" in msg:
            m = _FLOOD_LOG_RE.search(msg)
            wait_s = int(m.group(1)) if m else 1
            try:
                _flood_guard.notify_from_log(wait_s)
            except Exception:
                pass
            now = time.monotonic()
            if now - self._last_flood_log < 10.0:
                self._suppressed += 1
                record._flood_decision = False
                return False
            if self._suppressed > 0:
                record.msg = f"{msg} (+{self._suppressed} parallel part retries coalesced)"
                record.args = ()
                self._suppressed = 0
            self._last_flood_log = now

        record._flood_decision = True
        return True


_dedup_filter = _WzgramFloodDedupFilter()
for _h in logging.getLogger().handlers:
    _h.addFilter(_dedup_filter)
logging.getLogger("wzgram").addFilter(_dedup_filter)


class _RingBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_buffer.append(self.format(record))


_bh = _RingBufferHandler()
_bh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
_bh.addFilter(_dedup_filter)
logging.getLogger().addHandler(_bh)


# ==============================================================================
#  STEP 3: Environment Loader, rclone.conf Bootstrap & File-Type Classifier
# ==============================================================================
def _load_env_file(path: str = "config.env") -> None:
    p = Path(path)
    if not p.exists():
        return
    log.info(f"Loading environment from {path}")
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = val


_load_env_file("config.env")

def _resolve_rclone_conf_path() -> Path:
    candidates = []
    env_p = os.environ.get("RCLONE_CONFIG", "").strip()
    if env_p:
        candidates.append(Path(env_p))
    candidates.append(Path.home() / ".config" / "rclone" / "rclone.conf")
    candidates.append(Path("/tmp/rclone.conf"))
    for cand in candidates:
        try:
            cand.parent.mkdir(parents=True, exist_ok=True)
            if os.access(cand.parent, os.W_OK):
                os.environ["RCLONE_CONFIG"] = str(cand)
                return cand
        except OSError:
            continue
    fallback = Path("/tmp/rclone.conf")
    os.environ["RCLONE_CONFIG"] = str(fallback)
    return fallback


def _safe_conf_exists() -> bool:
    try:
        return os.path.isfile(RCLONE_CONF_PATH)
    except OSError:
        return False


RCLONE_CONF_PATH = _resolve_rclone_conf_path()


def _write_initial_rclone_conf() -> str | None:
    _target = RCLONE_CONF_PATH

    _url = os.environ.get("RCLONE_CONF_URL", "").strip()
    if _url:
        try:
            import urllib.request
            if "gist.github.com" in _url and "/raw" not in _url:
                _url = _url.rstrip("/") + "/raw"
            with urllib.request.urlopen(_url, timeout=15) as r:
                _data = r.read().decode("utf-8")
            _target.parent.mkdir(parents=True, exist_ok=True)
            _target.write_text(_data, encoding="utf-8")
            os.environ["RCLONE_CONFIG"] = str(_target)
            log.info(f"rclone.conf fetched from URL ({len(_data)} chars)")
            return _data
        except Exception as e:
            log.error(f"Failed to fetch rclone.conf from URL: {e}")

    _rc_text = os.environ.get("RCLONE_CONF", "").strip()
    if _rc_text:
        _rc_text = _rc_text.replace("\\n", "\n")
        try:
            _target.parent.mkdir(parents=True, exist_ok=True)
            _target.write_text(_rc_text, encoding="utf-8")
            os.environ["RCLONE_CONFIG"] = str(_target)
            log.info(f"rclone.conf written from RCLONE_CONF ({len(_rc_text)} chars)")
            return _rc_text
        except Exception as e:
            log.error(f"Failed to write rclone.conf: {e}")

    if _safe_conf_exists():
        os.environ["RCLONE_CONFIG"] = str(_target)
        log.info(f"rclone.conf found at {_target}")
        try:
            return _target.read_text(encoding="utf-8")
        except Exception:
            return None

    log.info("No local rclone.conf yet — will check MongoDB on startup or await /setrclone in bot.")
    return None


_INITIAL_RCLONE_TEXT = _write_initial_rclone_conf()

_MISSING: list[str] = []


def _req(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        _MISSING.append(key)
    return v


_API_ID = _req("API_ID")
API_HASH = _req("API_HASH")
BOT_TOKEN = _req("BOT_TOKEN")
_OWNER = _req("OWNER_ID")

if _MISSING:
    for k in _MISSING:
        log.critical(f"Missing required variable: {k}")
    raise SystemExit(1)

API_ID = int(_API_ID)
OWNER_ID = int(_OWNER)
# DUMP_CHAT_ID defaults to OWNER_ID if not set in env, and is 100% changeable from the bot UI
_DUMP_RAW = os.environ.get("DUMP_CHAT_ID", "").strip()
DUMP_CHAT_ID = int(_DUMP_RAW) if _DUMP_RAW else OWNER_ID

AUTHORIZED_USERS: set[int] = {OWNER_ID}
for _uid in os.environ.get("AUTHORIZED_USERS", "").replace(";", ",").split(","):
    _uid = _uid.strip()
    if _uid:
        try:
            AUTHORIZED_USERS.add(int(_uid))
        except ValueError:
            log.warning(f"Ignoring invalid AUTHORIZED_USERS entry: {_uid}")

MONGO_URI = (
    os.environ.get("MONGO_URI")
    or os.environ.get("DATABASE_URL")
    or os.environ.get("MONGODB_URI")
    or ""
).strip()
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "rclone_tg_bot").strip()

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/rclone_dl").strip()
RCLONE_FLAGS = os.environ.get("RCLONE_FLAGS", "").strip()
RCLONE_BUFFER_SIZE = os.environ.get("RCLONE_BUFFER_SIZE", "64M").strip()
RCLONE_STREAMS = max(1, min(32, int(os.environ.get("RCLONE_STREAMS", "8") or 8)))
SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", str(2000 * 1024 * 1024)))
HEALTH_PORT = int(os.environ.get("PORT") or os.environ.get("HEALTH_PORT") or 8080)
CONCURRENT_JOBS = max(1, min(5, int(os.environ.get("CONCURRENT_JOBS", 2) or 2)))
UPLOAD_WORKERS = max(1, min(16, int(os.environ.get("UPLOAD_WORKERS", 4) or 4)))
BW_LIMIT = os.environ.get("BW_LIMIT", "").strip()
STATS_FILE = os.environ.get("STATS_FILE", "/tmp/rclone_bot_stats.json").strip()
BOARD_REFRESH_INTERVAL = max(2.0, min(30.0, float(os.environ.get("BOARD_REFRESH_INTERVAL", "5.0"))))
AUTO_RESUME_JOBS = os.environ.get("AUTO_RESUME_JOBS", "true").strip().lower() not in ("0", "false", "no", "off")
DEFAULT_UPLOAD_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()
if DEFAULT_UPLOAD_MODE not in ("video", "document"):
    DEFAULT_UPLOAD_MODE = "video"

cores = os.environ.get("CPU_CORES", "0").strip()
threads = int(os.environ.get("CPU_THREADS", os.cpu_count() or 1))
_HAS_TASKSET = bool(shutil.which("taskset"))

Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

# ── File Type Extension Categories (Videos, Images, Documents) ───────────────
VIDEO_EXTS = {
    "mp4", "mkv", "avi", "mov", "webm", "flv", "ts", "m4v", "3gp", "3g2",
    "wmv", "mpg", "mpeg", "m2ts", "mts", "vob", "divx", "xvid", "rm", "rmvb",
    "ogv", "f4v", "asf", "amv", "m2v", "mp2", "mpe", "mpv", "qt", "yuv", "roq", "nsv",
}
IMAGE_EXTS = {
    "jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff", "tif", "heic", "heif",
    "avif", "svg", "ico", "raw", "cr2", "nef", "arw", "dng", "jfif", "pjpeg", "pjp",
}
PHOTO_SENDABLE_EXTS = {"jpg", "jpeg", "png", "webp"}
DOC_EXTS = {
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods", "odp",
    "txt", "rtf", "csv", "tsv", "md", "json", "xml", "yaml", "yml", "ini", "cfg", "conf", "log",
    "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "zst", "tgz", "tbz2", "cab", "iso", "dmg",
    "epub", "mobi", "azw", "azw3", "fb2", "djvu", "cbz", "cbr",
    "apk", "ipa", "exe", "msi", "deb", "rpm", "appimage", "bin", "torrent",
    "srt", "ass", "ssa", "vtt", "sub", "idx", "nfo",
    "py", "js", "html", "css", "sql", "db", "sqlite", "psd", "ai", "eps",
}
AUDIO_EXTS = {
    "mp3", "flac", "aac", "ogg", "opus", "wav", "m4a", "wma", "aiff", "ape",
    "dsd", "mka", "mpc", "ac3", "dts", "ra", "ram", "tta", "wv",
    "mid", "midi", "amr", "3ga", "awb", "gsm", "spx", "caf", "au",
}

ALL_FILE_TYPE_KEYS = ("video", "image", "document")
_TYPE_ALIASES = {
    "video": "video", "videos": "video", "vid": "video", "vids": "video", "v": "video",
    "image": "image", "images": "image", "img": "image", "imgs": "image",
    "photo": "image", "photos": "image", "pic": "image", "pics": "image", "i": "image",
    "document": "document", "documents": "document", "doc": "document", "docs": "document",
    "file": "document", "files": "document", "d": "document",
}


def _parse_file_types(raw: Any) -> set[str]:
    if isinstance(raw, (list, tuple, set)):
        tokens = [str(x).strip().lower() for x in raw]
    elif isinstance(raw, str):
        tokens = [t.strip().lower() for t in re.split(r"[,;\s+]+", raw) if t.strip()]
    else:
        tokens = []

    if not tokens or "all" in tokens or "*" in tokens:
        return {"video", "image", "document"}
    if "media" in tokens:
        return {"video", "image"}

    parsed: set[str] = set()
    for t in tokens:
        mapped = _TYPE_ALIASES.get(t)
        if mapped:
            parsed.add(mapped)
    return parsed if parsed else {"video", "image", "document"}


def _classify_file_ext(filename: str) -> str | None:
    base = _remote_basename(filename, is_remote=False)
    if "." not in base:
        return None
    ext = base.rsplit(".", 1)[-1].lower().strip()
    if not ext:
        return None
    if ext in VIDEO_EXTS:
        return "video"
    if ext in IMAGE_EXTS:
        return "image"
    return "document"


def _file_matches_types(filename: str, allowed_types: set[str]) -> bool:
    cat = _classify_file_ext(filename)
    return bool(cat and cat in allowed_types)


def _fmt_file_types_badge(ft_set: set[str] | None = None) -> str:
    if ft_set is None:
        with state_lock:
            ft_set = set(enabled_file_types)
    if ft_set >= {"video", "image", "document"}:
        return "All (Videos, Images, Docs)"
    if ft_set == {"video"}:
        return "Videos Only"
    if ft_set == {"image"}:
        return "Images Only"
    if ft_set == {"document"}:
        return "Documents Only"
    names = []
    if "video" in ft_set:
        names.append("Videos")
    if "image" in ft_set:
        names.append("Images")
    if "document" in ft_set:
        names.append("Docs")
    return " + ".join(names) if names else "All"


def _fmt_file_types_short(ft_set: set[str] | None = None) -> str:
    if ft_set is None:
        with state_lock:
            ft_set = set(enabled_file_types)
    if ft_set >= {"video", "image", "document"}:
        return "All"
    if ft_set == {"video"}:
        return "Videos"
    if ft_set == {"image"}:
        return "Images"
    if ft_set == {"document"}:
        return "Docs"
    names = []
    if "video" in ft_set:
        names.append("Vid")
    if "image" in ft_set:
        names.append("Img")
    if "document" in ft_set:
        names.append("Doc")
    return "+".join(names)


# ==============================================================================
#  STEP 4: Runtime State, Interactive Prompt State & Zero-Overhead Flood Guard
# ==============================================================================
BOT_START_TIME = time.time()
active_sessions: set[int] = set()
cancel_flags: dict[int, bool] = {}
force_kill_flags: dict[int, bool] = {}
upload_mode: dict[int, str] = {}
# Unified interactive prompt state per user:
# awaiting_input[user_id] = {"kind": "...", "cancel_nav": "nav:..."}
awaiting_input: dict[int, dict[str, Any]] = {}
active_procs: dict[int, set[asyncio.subprocess.Process]] = collections.defaultdict(set)
active_boards: dict[int, "_BoardState"] = {}
_health_server: HTTPServer | None = None

delete_after_upload: bool = False
max_size_enabled: bool = False
max_size_bytes: int = 2000 * 1024 * 1024
enabled_file_types: set[str] = _parse_file_types(os.environ.get("FILE_TYPES", "all"))
last_failed: list[str] = []
last_job_remote: str | None = None
last_job_mode: str = DEFAULT_UPLOAD_MODE
state_lock = threading.Lock()

stats: dict[str, Any] = {
    "total_done": 0,
    "total_failed": 0,
    "total_skipped": 0,
    "total_bytes": 0,
    "last_file": None,
    "last_error": None,
    "current_files": [],
    "floodwait_count": 0,
    "floodwait_last_secs": 0,
    "floodwait_last_time": None,
}
stats_lock = threading.Lock()

_path_registry: collections.OrderedDict[str, str] = collections.OrderedDict()
_single_file_keys: set[str] = set()
_MAX_PATH_REGISTRY = 512


def _reg_path(remote_path: str, is_single_file: bool | None = None) -> str:
    key = hashlib.blake2s(remote_path.encode("utf-8"), digest_size=5).hexdigest()
    with state_lock:
        _path_registry[key] = remote_path
        _path_registry.move_to_end(key)
        if is_single_file is True:
            _single_file_keys.add(key)
        elif is_single_file is False:
            _single_file_keys.discard(key)
        while len(_path_registry) > _MAX_PATH_REGISTRY:
            old_k, _ = _path_registry.popitem(last=False)
            _single_file_keys.discard(old_k)
    return key


def _get_path(key: str) -> str | None:
    with state_lock:
        return _path_registry.get(key)


def _is_single_file_key(key: str) -> bool:
    with state_lock:
        return key in _single_file_keys


def _has_active_session() -> bool:
    with state_lock:
        return bool(active_sessions)


def is_authorized(user_id: int) -> bool:
    with state_lock:
        return user_id in AUTHORIZED_USERS


auth_filter = filters.create(lambda _, __, m: bool(m.from_user and is_authorized(m.from_user.id)))

# Matches any non-command, non-document message when the user is in an interactive prompt
interactive_input_filter = filters.create(
    lambda _, __, m: bool(
        m.from_user
        and is_authorized(m.from_user.id)
        and m.from_user.id in awaiting_input
        and not getattr(m, "document", None)
        and not ((getattr(m, "text", None) or "").startswith("/"))
    )
)

app = Client(
    "rclone_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=max(8, CONCURRENT_JOBS * 4),
    sleep_threshold=60,
    max_concurrent_transmissions=max(4, CONCURRENT_JOBS * 2),
)


class _ZeroOverheadFloodGuard:
    """
    Zero-overhead MTProto Upload Guard:
      • During normal uploads, imposes ZERO artificial delay and ZERO lock serialization.
      • When Telegram raises or logs `FLOOD_WAIT_X` on `SaveBigFilePart`, sets a shared
        `_cooldown_until` window and staggers workers with random jitter (`50ms–300ms`).
    """

    def __init__(self) -> None:
        self._in_flight = 0
        self._cond: asyncio.Condition | None = None
        self._cond_loop: asyncio.AbstractEventLoop | None = None
        self._cooldown_until: float = 0.0
        self._last_stat_ts: float = 0.0
        self._installed: bool = False

    def _ensure_cond(self) -> asyncio.Condition:
        loop = asyncio.get_running_loop()
        if self._cond is None or self._cond_loop is not loop:
            self._cond = asyncio.Condition()
            self._cond_loop = loop
            self._in_flight = 0
        return self._cond

    def _max_slots(self) -> int:
        with state_lock:
            return max(2, UPLOAD_WORKERS * max(1, CONCURRENT_JOBS))

    async def acquire(self) -> None:
        cond = self._ensure_cond()
        async with cond:
            while self._in_flight >= self._max_slots():
                await cond.wait()
            self._in_flight += 1

        now = time.monotonic()
        if now < self._cooldown_until:
            await asyncio.sleep((self._cooldown_until - now) + random.uniform(0.05, 0.30))

    async def release(self) -> None:
        cond = self._ensure_cond()
        async with cond:
            self._in_flight = max(0, self._in_flight - 1)
            cond.notify_all()

    def notify_from_log(self, wait_secs: int) -> None:
        now = time.monotonic()
        target = now + max(1.0, float(wait_secs)) + 0.15
        if target > self._cooldown_until:
            self._cooldown_until = target
        if now - self._last_stat_ts >= 1.5:
            self._last_stat_ts = now
            with stats_lock:
                stats["floodwait_count"] += 1
                stats["floodwait_last_secs"] = int(wait_secs)
                stats["floodwait_last_time"] = time.time()

    async def on_flood_wait(self, wait_secs: int) -> None:
        self.notify_from_log(wait_secs)
        sleep_for = max(0.2, self._cooldown_until - time.monotonic()) + random.uniform(0.05, 0.35)
        await asyncio.sleep(sleep_for)


_flood_guard = _ZeroOverheadFloodGuard()


def _install_wzgram_flood_guard() -> None:
    if _flood_guard._installed:
        return
    _flood_guard._installed = True

    try:
        from wzgram.session import Session as _WzSession
        _orig_invoke = getattr(_WzSession, "invoke", None)
        if callable(_orig_invoke):
            async def _guarded_invoke(self_session: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
                qname = type(query).__name__
                if qname not in ("SaveBigFilePart", "SaveFilePart"):
                    return await _orig_invoke(self_session, query, *args, **kwargs)

                await _flood_guard.acquire()
                try:
                    for _attempt in range(12):
                        try:
                            return await _orig_invoke(self_session, query, *args, **kwargs)
                        except FloodWait as fw:
                            await _flood_guard.on_flood_wait(getattr(fw, "value", 1) or 1)
                    return await _orig_invoke(self_session, query, *args, **kwargs)
                finally:
                    await _flood_guard.release()

            _WzSession.invoke = _guarded_invoke
    except Exception as e:
        log.debug(f"Session.invoke guard note: {e}")

    try:
        _orig_save_file = getattr(Client, "save_file", None)
        if callable(_orig_save_file):
            sig = inspect.signature(_orig_save_file)
            has_workers_param = "workers" in sig.parameters

            async def _guarded_save_file(self_client: Any, *args: Any, **kwargs: Any) -> Any:
                if has_workers_param and "workers" not in kwargs:
                    with state_lock:
                        kwargs["workers"] = UPLOAD_WORKERS
                return await _orig_save_file(self_client, *args, **kwargs)

            Client.save_file = _guarded_save_file
    except Exception as e:
        log.debug(f"Client.save_file guard note: {e}")


_install_wzgram_flood_guard()


def _sync_client_concurrency() -> None:
    with state_lock:
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
    try:
        app.max_concurrent_transmissions = max(2, conc * max(1, up_w // 2))
    except Exception as e:
        log.debug(f"Could not update app.max_concurrent_transmissions: {e}")
    try:
        loop = asyncio.get_running_loop()
        if _flood_guard._cond is not None and _flood_guard._cond_loop is loop:
            async def _notify_all():
                assert _flood_guard._cond is not None
                async with _flood_guard._cond:
                    _flood_guard._cond.notify_all()

            loop.create_task(_notify_all())
    except RuntimeError:
        pass


# ==============================================================================
#  STEP 5: MongoDB Persistence Engine (with Local JSON Fallback)
# ==============================================================================
class DatabaseManager:
    def __init__(self, uri: str, db_name: str, fallback_file: str):
        self.uri = uri
        self.db_name = db_name
        self.fallback_file = Path(fallback_file)
        self.client: Any = None
        self.db: Any = None
        self.connected: bool = False
        self.last_sync_ts: float = 0.0
        self.last_error: str | None = None
        self._has_db_rclone_conf: bool = False

    async def connect(self) -> bool:
        if not self.uri:
            log.info("MONGO_URI not set — using local JSON persistence fallback.")
            await self.load_all()
            return False
        if AsyncIOMotorClient is None:
            log.warning("motor package not installed — falling back to local JSON.")
            await self.load_all()
            return False
        try:
            self.client = AsyncIOMotorClient(
                self.uri,
                serverSelectionTimeoutMS=8000,
                connectTimeoutMS=8000,
            )
            await self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            self.connected = True
            self.last_sync_ts = time.time()
            log.info(f"✅ Connected to MongoDB database: '{self.db_name}'")
            await self.load_all()
            if _INITIAL_RCLONE_TEXT and not self._has_db_rclone_conf:
                await self.save_rclone_conf(_INITIAL_RCLONE_TEXT, source="startup_env")
            return True
        except Exception as e:
            self.connected = False
            self.last_error = str(e)[:120]
            log.error(f"⚠️ MongoDB connection failed ({e}) — using local JSON fallback.")
            await self.load_all()
            return False

    def _snapshot_settings(self) -> dict[str, Any]:
        with state_lock:
            recent_paths = dict(list(_path_registry.items())[-100:])
            single_keys = [k for k in recent_paths if k in _single_file_keys]
            return {
                "delete_after_upload": delete_after_upload,
                "max_size_enabled": max_size_enabled,
                "max_size_bytes": max_size_bytes,
                "enabled_file_types": sorted(enabled_file_types),
                "concurrent_jobs": CONCURRENT_JOBS,
                "upload_workers": UPLOAD_WORKERS,
                "rclone_streams": RCLONE_STREAMS,
                "rclone_buffer_size": RCLONE_BUFFER_SIZE,
                "bw_limit": BW_LIMIT,
                "rclone_flags": RCLONE_FLAGS,
                "board_refresh_interval": BOARD_REFRESH_INTERVAL,
                "split_size": SPLIT_SIZE,
                "default_upload_mode": DEFAULT_UPLOAD_MODE,
                "auto_resume_jobs": AUTO_RESUME_JOBS,
                "authorized_users": sorted(AUTHORIZED_USERS),
                "DUMP_CHAT_ID": DUMP_CHAT_ID,
                "path_registry": recent_paths,
                "single_file_keys": single_keys,
                "speed_profile_v3": True,
                "updated_at": time.time(),
            }

    def _snapshot_stats(self) -> dict[str, Any]:
        with stats_lock:
            s = dict(stats)
        with state_lock:
            s["last_failed"] = list(last_failed)
            s["last_job_remote"] = last_job_remote
            s["last_job_mode"] = last_job_mode
        s.pop("current_files", None)
        s["updated_at"] = time.time()
        return s

    def _apply_settings(self, cfg: dict[str, Any]) -> None:
        global delete_after_upload, max_size_enabled, max_size_bytes, enabled_file_types
        global CONCURRENT_JOBS, UPLOAD_WORKERS, RCLONE_STREAMS, RCLONE_BUFFER_SIZE
        global BW_LIMIT, RCLONE_FLAGS, BOARD_REFRESH_INTERVAL
        global SPLIT_SIZE, DEFAULT_UPLOAD_MODE, AUTO_RESUME_JOBS, DUMP_CHAT_ID

        is_v3 = bool(cfg.get("speed_profile_v3"))
        with state_lock:
            if isinstance(cfg.get("delete_after_upload"), bool):
                delete_after_upload = cfg["delete_after_upload"]
            if isinstance(cfg.get("max_size_enabled"), bool):
                max_size_enabled = cfg["max_size_enabled"]
            if isinstance(cfg.get("max_size_bytes"), int) and cfg["max_size_bytes"] > 0:
                max_size_bytes = cfg["max_size_bytes"]
            if cfg.get("enabled_file_types") is not None:
                enabled_file_types = _parse_file_types(cfg["enabled_file_types"])
            if isinstance(cfg.get("concurrent_jobs"), int):
                CONCURRENT_JOBS = max(1, min(5, cfg["concurrent_jobs"]))
            if isinstance(cfg.get("upload_workers"), int):
                UPLOAD_WORKERS = max(1, min(16, cfg["upload_workers"]))
            if isinstance(cfg.get("rclone_streams"), (int, str)):
                try:
                    RCLONE_STREAMS = max(1, min(32, int(cfg["rclone_streams"])))
                except ValueError:
                    pass
            if isinstance(cfg.get("rclone_buffer_size"), str) and cfg["rclone_buffer_size"].strip():
                RCLONE_BUFFER_SIZE = cfg["rclone_buffer_size"].strip()

            if not is_v3:
                if UPLOAD_WORKERS <= 3 or UPLOAD_WORKERS == 8:
                    UPLOAD_WORKERS = 4
                if RCLONE_STREAMS <= 4:
                    RCLONE_STREAMS = 8
                if RCLONE_BUFFER_SIZE == "32M":
                    RCLONE_BUFFER_SIZE = "64M"

            if isinstance(cfg.get("bw_limit"), str):
                BW_LIMIT = cfg["bw_limit"]
            if isinstance(cfg.get("rclone_flags"), str):
                RCLONE_FLAGS = cfg["rclone_flags"].strip()
            if isinstance(cfg.get("board_refresh_interval"), (int, float)):
                BOARD_REFRESH_INTERVAL = max(2.0, min(30.0, float(cfg["board_refresh_interval"])))
            if isinstance(cfg.get("split_size"), int) and cfg["split_size"] > 50_000_000:
                SPLIT_SIZE = cfg["split_size"]
            if cfg.get("default_upload_mode") in ("video", "document"):
                DEFAULT_UPLOAD_MODE = cfg["default_upload_mode"]
            if isinstance(cfg.get("auto_resume_jobs"), bool):
                AUTO_RESUME_JOBS = cfg["auto_resume_jobs"]
            if isinstance(cfg.get("authorized_users"), list):
                AUTHORIZED_USERS.clear()
                AUTHORIZED_USERS.add(OWNER_ID)
                for uid in cfg["authorized_users"]:
                    if isinstance(uid, int):
                        AUTHORIZED_USERS.add(uid)
            if isinstance(cfg.get("DUMP_CHAT_ID"), int):
                DUMP_CHAT_ID = cfg["DUMP_CHAT_ID"]
            if isinstance(cfg.get("path_registry"), dict):
                for k, v in cfg["path_registry"].items():
                    if isinstance(k, str) and isinstance(v, str):
                        _path_registry[k] = v
            if isinstance(cfg.get("single_file_keys"), list):
                for k in cfg["single_file_keys"]:
                    if isinstance(k, str):
                        _single_file_keys.add(k)
        _sync_client_concurrency()

    def _apply_stats(self, data: dict[str, Any]) -> None:
        global last_failed, last_job_remote, last_job_mode
        with stats_lock:
            for k in (
                "total_done", "total_failed", "total_skipped", "total_bytes",
                "floodwait_count", "floodwait_last_secs",
            ):
                if isinstance(data.get(k), int):
                    stats[k] = data[k]
            for k in ("last_file", "last_error", "floodwait_last_time"):
                if data.get(k) is not None:
                    stats[k] = data[k]
        with state_lock:
            if isinstance(data.get("last_failed"), list):
                last_failed = [str(x) for x in data["last_failed"]]
            if isinstance(data.get("last_job_remote"), str):
                last_job_remote = data["last_job_remote"]
            if data.get("last_job_mode") in ("video", "document"):
                last_job_mode = data["last_job_mode"]

    async def load_all(self) -> None:
        if self.fallback_file.exists():
            try:
                raw = json.loads(self.fallback_file.read_text(encoding="utf-8"))
                if isinstance(raw.get("settings"), dict):
                    self._apply_settings(raw["settings"])
                if isinstance(raw, dict):
                    self._apply_stats(raw)
            except Exception as e:
                log.debug(f"Local stats load warning: {e}")

        if not self.connected or self.db is None:
            return

        try:
            cfg_doc = await self.db.bot_state.find_one({"_id": "settings"})
            if cfg_doc:
                was_v3 = bool(cfg_doc.get("speed_profile_v3"))
                self._apply_settings(cfg_doc)
                log.info(
                    f"Restored bot configuration from MongoDB "
                    f"(CONCURRENT_JOBS={CONCURRENT_JOBS}x, UPLOAD_WORKERS={UPLOAD_WORKERS}w, "
                    f"STREAMS={RCLONE_STREAMS}s, BUF={RCLONE_BUFFER_SIZE}, "
                    f"DUMP={DUMP_CHAT_ID}, TYPES={','.join(sorted(enabled_file_types))})"
                )
                if not was_v3:
                    await self.save_all()

            stats_doc = await self.db.bot_state.find_one({"_id": "stats"})
            if stats_doc:
                self._apply_stats(stats_doc)
                log.info("Restored transfer statistics from MongoDB")

            rc_doc = await self.db.bot_state.find_one({"_id": "rclone_conf"})
            if rc_doc and isinstance(rc_doc.get("content"), str) and rc_doc["content"].strip():
                self._has_db_rclone_conf = True
                RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
                RCLONE_CONF_PATH.write_text(rc_doc["content"], encoding="utf-8")
                os.environ["RCLONE_CONFIG"] = str(RCLONE_CONF_PATH)
                remotes = re.findall(r"^\[([^\]]+)\]", rc_doc["content"], re.MULTILINE)
                log.info(f"Restored rclone.conf from MongoDB ({len(remotes)} remote(s))")
            self.last_sync_ts = time.time()
        except Exception as e:
            self.last_error = str(e)[:120]
            log.warning(f"MongoDB load_all error: {e}")

    async def save_all(self) -> None:
        cfg = self._snapshot_settings()
        st = self._snapshot_stats()

        try:
            combined = dict(st)
            combined["settings"] = cfg
            self.fallback_file.write_text(json.dumps(combined), encoding="utf-8")
        except Exception as e:
            log.debug(f"Local JSON save error: {e}")

        if not self.connected or self.db is None:
            return
        try:
            await self.db.bot_state.update_one(
                {"_id": "settings"}, {"$set": cfg}, upsert=True
            )
            await self.db.bot_state.update_one(
                {"_id": "stats"}, {"$set": st}, upsert=True
            )
            self.last_sync_ts = time.time()
        except Exception as e:
            self.last_error = str(e)[:120]
            log.debug(f"MongoDB save_all error: {e}")

    async def save_rclone_conf(self, content: str, source: str = "telegram_upload") -> None:
        if not self.connected or self.db is None:
            return
        try:
            remotes = re.findall(r"^\[([^\]]+)\]", content, re.MULTILINE)
            await self.db.bot_state.update_one(
                {"_id": "rclone_conf"},
                {
                    "$set": {
                        "content": content,
                        "remotes": remotes,
                        "source": source,
                        "updated_at": time.time(),
                    }
                },
                upsert=True,
            )
            self._has_db_rclone_conf = True
            self.last_sync_ts = time.time()
            log.info(f"Saved rclone.conf ({len(remotes)} remotes) to MongoDB")
        except Exception as e:
            self.last_error = str(e)[:120]
            log.warning(f"Failed to save rclone.conf to MongoDB: {e}")

    async def save_active_job(
        self,
        remote_path: str,
        mode: str,
        do_delete: bool,
        user_id: int,
        chat_id: int,
        status_msg_id: int,
        all_files: list[str],
        done_files: list[str],
        failed_files: list[str],
        skipped_files: list[str],
    ) -> None:
        with state_lock:
            conc = CONCURRENT_JOBS
            up_w = UPLOAD_WORKERS
            streams = RCLONE_STREAMS
            buf = RCLONE_BUFFER_SIZE
            ft_list = sorted(enabled_file_types)
        payload = {
            "active": True,
            "remote_path": remote_path,
            "mode": mode,
            "do_delete": do_delete,
            "enabled_file_types": ft_list,
            "concurrent_jobs": conc,
            "upload_workers": up_w,
            "rclone_streams": streams,
            "rclone_buffer_size": buf,
            "user_id": user_id,
            "chat_id": chat_id,
            "status_msg_id": status_msg_id,
            "all_files": list(all_files),
            "done_files": list(done_files),
            "failed_files": list(failed_files),
            "skipped_files": list(skipped_files),
            "updated_at": time.time(),
        }
        try:
            Path(f"{STATS_FILE}.job.json").write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            pass
        if self.connected and self.db is not None:
            try:
                await self.db.bot_state.update_one(
                    {"_id": "active_job"}, {"$set": payload}, upsert=True
                )
                self.last_sync_ts = time.time()
            except Exception as e:
                log.debug(f"save_active_job error: {e}")

    async def clear_active_job(self) -> None:
        try:
            Path(f"{STATS_FILE}.job.json").unlink(missing_ok=True)
        except Exception:
            pass
        if self.connected and self.db is not None:
            try:
                await self.db.bot_state.delete_one({"_id": "active_job"})
            except Exception as e:
                log.debug(f"clear_active_job error: {e}")

    async def get_unfinished_job(self) -> dict[str, Any] | None:
        if self.connected and self.db is not None:
            try:
                doc = await self.db.bot_state.find_one({"_id": "active_job"})
                if doc and doc.get("active"):
                    return doc
            except Exception as e:
                log.debug(f"get_unfinished_job mongo error: {e}")
        job_file = Path(f"{STATS_FILE}.job.json")
        if job_file.exists():
            try:
                doc = json.loads(job_file.read_text(encoding="utf-8"))
                if doc.get("active"):
                    return doc
            except Exception:
                pass
        return None

    async def save_restart_marker(self, chat_id: int, message_id: int) -> None:
        marker = {"chat_id": chat_id, "message_id": message_id, "ts": time.time()}
        try:
            Path(f"{STATS_FILE}.restart.json").write_text(json.dumps(marker), encoding="utf-8")
        except Exception:
            pass
        if self.connected and self.db is not None:
            try:
                await self.db.bot_state.update_one(
                    {"_id": "restart_marker"}, {"$set": marker}, upsert=True
                )
            except Exception:
                pass

    async def pop_restart_marker(self) -> dict[str, Any] | None:
        marker = None
        if self.connected and self.db is not None:
            try:
                marker = await self.db.bot_state.find_one_and_delete({"_id": "restart_marker"})
            except Exception:
                pass
        p = Path(f"{STATS_FILE}.restart.json")
        if p.exists():
            try:
                local_m = json.loads(p.read_text(encoding="utf-8"))
                p.unlink(missing_ok=True)
                if not marker:
                    marker = local_m
            except Exception:
                pass
        return marker


db_mgr = DatabaseManager(MONGO_URI, MONGO_DB_NAME, STATS_FILE)


# ==============================================================================
#  STEP 6: Health Check HTTP Server & Graceful Shutdown/Restart Cleanup
# ==============================================================================
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/ping", "/"):
            uptime = int(time.time() - BOT_START_TIME)
            h, rem = divmod(uptime, 3600)
            m, s = divmod(rem, 60)
            with stats_lock:
                done = stats["total_done"]
                failed = stats["total_failed"]
                tbytes = stats["total_bytes"]
            with state_lock:
                conc = CONCURRENT_JOBS
                up_w = UPLOAD_WORKERS
                streams = RCLONE_STREAMS
                buf = RCLONE_BUFFER_SIZE
                dump_id = DUMP_CHAT_ID
                act_cnt = len(active_sessions)
                ft_str = ",".join(sorted(enabled_file_types))
            body = (
                f"status=ok\nengine={MTPROTO_ENGINE}\nuptime={h}h {m}m {s}s\n"
                f"mongodb={'connected' if db_mgr.connected else 'local_fallback'}\n"
                f"dump_chat_id={dump_id}\n"
                f"active_jobs={act_cnt}\n"
                f"concurrent_limit={conc}\n"
                f"upload_workers={up_w}\n"
                f"rclone_streams={streams}\n"
                f"rclone_buffer={buf}\n"
                f"file_types={ft_str}\n"
                f"files_done={done}\n"
                f"files_failed={failed}\n"
                f"bytes_uploaded={tbytes}\n"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        pass


def _run_health():
    global _health_server
    try:
        HTTPServer.allow_reuse_address = True
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        _health_server = server
        server.serve_forever()
    except Exception as e:
        log.error(f"Health server failed on port {HEALTH_PORT}: {e}")


async def _graceful_restart_cleanup() -> None:
    """Close health socket and stop WZGram client cleanly before os.execv()."""
    global _health_server
    if _health_server is not None:
        try:
            _health_server.server_close()
        except Exception:
            pass
        _health_server = None
    try:
        await asyncio.wait_for(app.stop(), timeout=3.0)
    except Exception:
        pass


# ==============================================================================
#  STEP 7: Clean UI Formatters & Visual Progress Components
# ==============================================================================
def fmt_size(size_bytes: float) -> str:
    if size_bytes < 0:
        return "0.0 B"
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def fmt_uptime() -> str:
    u = max(0, int(time.time() - BOT_START_TIME))
    d, rem = divmod(u, 86400)
    h, r = divmod(rem, 3600)
    m, s = divmod(r, 60)
    if d > 0:
        return f"{d}d {h}h {m}m"
    if h > 0:
        return f"{h}h {m}m {s}s"
    return f"{m}m {s}s"


def fmt_eta(seconds: float) -> str:
    if seconds <= 0 or seconds > 86400:
        return "—"
    h, r = divmod(int(seconds), 3600)
    m, s = divmod(r, 60)
    if h > 0:
        return f"{h}h {m}m"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def progress_bar(done: float, total: float, width: int = 10) -> str:
    if total <= 0:
        return "▱" * width
    pct = max(0.0, min(1.0, done / total))
    filled = max(0, min(width, round(width * pct)))
    return "▰" * filled + "▱" * (width - filled)


def _server_resources() -> str:
    parts = []
    if psutil is not None:
        try:
            cpu = psutil.cpu_percent(interval=None)
            vm = psutil.virtual_memory()
            parts.append(f"🖥 CPU `{cpu:.0f}%`")
            parts.append(f"🧠 RAM `{vm.percent:.0f}%`")
        except Exception:
            pass
    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        parts.append(f"💾 Free `{fmt_size(du.free)}`")
    except Exception:
        pass
    return " · ".join(parts)


def make_progress_line(
    label: str,
    done_bytes: int,
    total_bytes: int,
    speed_str: str = "",
    eta_str: str = "",
    width: int = 10,
    elapsed_str: str = "",
    part_info: str = "",
) -> str:
    """Clean, 2-line compact file progress item."""
    if total_bytes > 0:
        pct = min(100, int(done_bytes * 100 / total_bytes))
        bar = progress_bar(done_bytes, total_bytes, width)
        size_str = f"{fmt_size(done_bytes)}/{fmt_size(total_bytes)}"
    else:
        pct = 0
        bar = "▱" * width
        size_str = fmt_size(done_bytes) if done_bytes else "…"

    part_tag = f" `(Part {part_info})`" if part_info else ""
    meta = [f"`{bar}` **{pct}%**", f"`{size_str}`"]
    if speed_str:
        meta.append(f"⚡ `{speed_str}`")
    if eta_str and eta_str != "—":
        meta.append(f"⏳ `{eta_str}`")
    return f"{label}{part_tag}\n" + " · ".join(meta)


# ==============================================================================
#  STEP 8: Bot Control Center Keyboards & Screen Builders (100% Bot Control)
# ==============================================================================
_CONC_OPTIONS = [1, 2, 3, 4, 5]
_UP_WORKER_OPTIONS = [2, 4, 6, 8]
_STREAM_OPTIONS = [4, 8, 12, 16]
_BUFFER_OPTIONS = ["32M", "64M", "128M", "256M"]
_SIZE_PRESETS_MB = [250, 500, 1024, 2000, 4000]
_SPLIT_PRESETS_MB = [500, 1000, 1500, 1950, 2000]
_BW_PRESETS = ["", "5M", "10M", "25M", "50M", "100M"]
_REFRESH_PRESETS = [3.0, 5.0, 8.0, 12.0, 15.0]
_POPULAR_FLAGS = [
    ("--fast-list", "⚡ Fast-List"),
    ("--drive-acknowledge-abuse", "🛡 GDrive Abuse OK"),
    ("--no-check-certificate", "🔓 No Cert Check"),
    ("--ignore-checksum", "⏩ Ignore Checksum"),
]


def _resolve_return_button(return_tag: str) -> tuple[str, str]:
    """
    Resolve a compact `return_tag` into `(button_label, callback_data)` so
    sub-menus (`File Types`, `Speed & Concurrency`) always return to the exact
    screen that opened them:
      • `d_<key>` ➔ Folder Transfer Launchpad (`dl:open:<key>`)
      • `s_<key>` ➔ Single-File Transfer Launchpad (`dl:file:<key>`)
      • `f_<key>` ➔ Files in Folder Browser (`br:files:<key>:0`)
      • `b_<key>` ➔ Remote Folder Browser (`br:dir:<key>:0`)
      • `r_0`     ➔ Remotes List (`nav:remotes:0`)
      • `home`    ➔ Home Menu (`nav:home`)
      • `""`      ➔ Control Panel (`nav:settings`)
    """
    if not return_tag:
        return ("⬅️ Control Panel", "nav:settings")
    if return_tag == "home":
        return ("⬅️ Home", "nav:home")
    if return_tag == "r_0":
        return ("⬅️ Remotes", "nav:remotes:0")
    if return_tag.startswith("d_"):
        return ("⬅️ Back to Transfer", f"dl:open:{return_tag[2:]}")
    if return_tag.startswith("s_"):
        return ("⬅️ Back to File", f"dl:file:{return_tag[2:]}")
    if return_tag.startswith("f_"):
        return ("⬅️ Back to Files", f"br:files:{return_tag[2:]}:0")
    if return_tag.startswith("b_"):
        return ("⬅️ Back to Folder", f"br:dir:{return_tag[2:]}:0")
    return ("⬅️ Back to Transfer", f"dl:open:{return_tag}")


def home_keyboard() -> InlineKeyboardMarkup:
    """Clean 2-column main menu keyboard giving full access to every bot control."""
    with state_lock:
        failed_cnt = len(last_failed)
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        is_running = bool(active_sessions)
        ft_short = _fmt_file_types_short(enabled_file_types)

    rows = []
    if is_running:
        rows.append([
            InlineKeyboardButton("📡 View Active Transfer", callback_data="nav:queue")
        ])
    elif failed_cnt > 0:
        rows.append([
            InlineKeyboardButton(f"🔁 Retry Failed ({failed_cnt})", callback_data="action:retry")
        ])

    rows += [
        [
            InlineKeyboardButton("🗂 Browse Remotes", callback_data="nav:remotes:0"),
            InlineKeyboardButton("📊 Status & Tools", callback_data="nav:status"),
        ],
        [
            InlineKeyboardButton(f"🎯 Types: {ft_short}", callback_data="nav:filetypes:home"),
            InlineKeyboardButton(f"⚡ Speed: {conc}x·{up_w}w", callback_data="nav:concurrent:home"),
        ],
        [
            InlineKeyboardButton("⚙️ Control Panel", callback_data="nav:settings"),
            InlineKeyboardButton("🔧 rclone.conf", callback_data="nav:rclone"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def status_keyboard() -> InlineKeyboardMarkup:
    with state_lock:
        failed_cnt = len(last_failed)
        is_running = bool(active_sessions)
    rows = []
    if is_running:
        rows.append([
            InlineKeyboardButton("🛑 Stop After Current", callback_data="job:cancel"),
            InlineKeyboardButton("⛔ Kill Now", callback_data="job:kill"),
        ])
    elif failed_cnt > 0:
        rows.append([
            InlineKeyboardButton(f"🔁 Retry Failed ({failed_cnt})", callback_data="action:retry")
        ])
    rows += [
        [
            InlineKeyboardButton("📜 Logs", callback_data="nav:logs"),
            InlineKeyboardButton("🗄 MongoDB", callback_data="nav:db"),
        ],
        [
            InlineKeyboardButton("🧹 Clean Temp", callback_data="action:clean_temp"),
            InlineKeyboardButton("🔄 Reset Stats", callback_data="action:reset_stats"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="nav:status"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def filetypes_keyboard(return_tag: str = "") -> InlineKeyboardMarkup:
    """File Types selector keyboard (Videos, Images, Documents)."""
    with state_lock:
        ft = set(enabled_file_types)

    suffix = f":{return_tag}" if return_tag else ""

    only_vid = (ft == {"video"})
    only_img = (ft == {"image"})
    only_doc = (ft == {"document"})
    is_all = (ft >= {"video", "image", "document"})
    is_media = (ft == {"video", "image"})

    vid_on = "video" in ft
    img_on = "image" in ft
    doc_on = "document" in ft

    back_lbl, back_cb = _resolve_return_button(return_tag)

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ 🎬 Videos Only" if only_vid else "🎬 Videos Only", callback_data=f"ft:only:video{suffix}"),
            InlineKeyboardButton("✅ 🖼 Images Only" if only_img else "🖼 Images Only", callback_data=f"ft:only:image{suffix}"),
            InlineKeyboardButton("✅ 📄 Docs Only" if only_doc else "📄 Docs Only", callback_data=f"ft:only:document{suffix}"),
        ],
        [
            InlineKeyboardButton("✅ 📦 All Types" if is_all else "📦 All Types", callback_data=f"ft:preset:all{suffix}"),
            InlineKeyboardButton("✅ 🎞 Video + Image" if is_media else "🎞 Video + Image", callback_data=f"ft:preset:media{suffix}"),
        ],
        [
            InlineKeyboardButton(f"🎬 Video: {'ON 🟢' if vid_on else 'OFF 🔴'}", callback_data=f"ft:toggle:video{suffix}"),
            InlineKeyboardButton(f"🖼 Image: {'ON 🟢' if img_on else 'OFF 🔴'}", callback_data=f"ft:toggle:image{suffix}"),
            InlineKeyboardButton(f"📄 Doc: {'ON 🟢' if doc_on else 'OFF 🔴'}", callback_data=f"ft:toggle:document{suffix}"),
        ],
        [
            InlineKeyboardButton(back_lbl, callback_data=back_cb),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def concurrent_keyboard(return_tag: str = "") -> InlineKeyboardMarkup:
    """Speed & Concurrency Setup Keyboard."""
    with state_lock:
        cur_conc = CONCURRENT_JOBS
        cur_upw = UPLOAD_WORKERS
        cur_streams = RCLONE_STREAMS
        cur_buf = RCLONE_BUFFER_SIZE

    suffix = f":{return_tag}" if return_tag else ""

    is_safe = (cur_conc == 1 and cur_upw <= 2 and cur_streams == 4)
    is_fast = (cur_conc == 2 and cur_upw == 4 and cur_streams == 8)
    is_turbo = (cur_conc >= 3 and cur_upw >= 6 and cur_streams >= 12)

    preset_row = [
        InlineKeyboardButton("✅ 🛡 Safe" if is_safe else "🛡 Safe", callback_data=f"conc:preset:safe{suffix}"),
        InlineKeyboardButton("✅ ⚡ Fast" if is_fast else "⚡ Fast", callback_data=f"conc:preset:fast{suffix}"),
        InlineKeyboardButton("✅ 🚀 Turbo" if is_turbo else "🚀 Turbo", callback_data=f"conc:preset:turbo{suffix}"),
    ]

    conc_row = [
        InlineKeyboardButton(
            f"✅ {c}x" if c == cur_conc else f"{c}x",
            callback_data=f"conc:jobs:{c}{suffix}",
        )
        for c in _CONC_OPTIONS
    ]

    upw_row = [
        InlineKeyboardButton(
            f"✅ {w}w" if w == cur_upw else f"{w}w",
            callback_data=f"conc:upw:{w}{suffix}",
        )
        for w in _UP_WORKER_OPTIONS
    ]

    rclone_row = [
        InlineKeyboardButton(f"🌊 Streams: {cur_streams}s ▸", callback_data=f"conc:cycle_streams{suffix}"),
        InlineKeyboardButton(f"📦 Buffer: {cur_buf} ▸", callback_data=f"conc:cycle_buf{suffix}"),
    ]

    back_lbl, back_cb = _resolve_return_button(return_tag)

    return InlineKeyboardMarkup([
        preset_row,
        conc_row,
        upw_row,
        rclone_row,
        [
            InlineKeyboardButton(back_lbl, callback_data=back_cb),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def settings_keyboard() -> InlineKeyboardMarkup:
    """Master Bot Control Panel — every setting controllable from inline buttons."""
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        split_mb = SPLIT_SIZE // (1024 * 1024)
        bw = BW_LIMIT or "OFF"
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        refr = int(BOARD_REFRESH_INTERVAL)
        auto_res = AUTO_RESUME_JOBS
        def_mode = DEFAULT_UPLOAD_MODE
        ft_short = _fmt_file_types_short(enabled_file_types)

    mode_lbl = "🎬 Media" if def_mode == "video" else "📄 Doc"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"🎯 Types: {ft_short} ▸", callback_data="nav:filetypes"),
            InlineKeyboardButton(f"⚡ Speed: {conc}x·{up_w}w ▸", callback_data="nav:concurrent"),
        ],
        [
            InlineKeyboardButton("📢 Dump Chat ▸", callback_data="nav:dump"),
            InlineKeyboardButton(f"📤 Mode: {mode_lbl}", callback_data="cfg:toggle_mode"),
        ],
        [
            InlineKeyboardButton(f"🗑 Auto-Del: {'ON 🟢' if del_on else 'OFF 🔴'}", callback_data="cfg:toggle_del"),
            InlineKeyboardButton(f"▶️ Resume: {'ON 🟢' if auto_res else 'OFF 🔴'}", callback_data="cfg:toggle_resume"),
        ],
        [
            InlineKeyboardButton(f"📏 Cap: {f'{cap_mb}M 🟢' if size_on else 'OFF 🔴'}", callback_data="cfg:toggle_size"),
            InlineKeyboardButton(f"📐 {cap_mb}M ▸", callback_data="cfg:cycle_size"),
            InlineKeyboardButton("✏️ Custom", callback_data="prompt:max_size"),
        ],
        [
            InlineKeyboardButton(f"✂️ Split: {split_mb}M ▸", callback_data="cfg:cycle_split"),
            InlineKeyboardButton(f"🚦 BW: {bw} ▸", callback_data="cfg:cycle_bw"),
            InlineKeyboardButton(f"⏱ {refr}s ▸", callback_data="cfg:cycle_refresh"),
        ],
        [
            InlineKeyboardButton("🚩 Flags ▸", callback_data="nav:flags"),
            InlineKeyboardButton("👥 Users ▸", callback_data="nav:users"),
            InlineKeyboardButton("🗄 MongoDB ▸", callback_data="nav:db"),
        ],
        [
            InlineKeyboardButton("🔄 Restart", callback_data="action:restart_bot"),
            InlineKeyboardButton("🔌 Stop", callback_data="action:stop_bot"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def dump_chat_keyboard() -> InlineKeyboardMarkup:
    """Interactive Dump Chat / Target Channel control panel."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📍 Use This Chat", callback_data="dump:here"),
            InlineKeyboardButton("👤 Use Owner DM", callback_data="dump:owner"),
        ],
        [
            InlineKeyboardButton("✏️ Enter ID / Forward Msg", callback_data="prompt:dump_chat"),
            InlineKeyboardButton("🔔 Test Upload Chat", callback_data="dump:test"),
        ],
        [
            InlineKeyboardButton("⬅️ Control Panel", callback_data="nav:settings"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def rclone_manager_keyboard(remotes: list[str]) -> InlineKeyboardMarkup:
    """Full Rclone Config Manager inside the bot."""
    rows = [
        [
            InlineKeyboardButton("📤 Upload / Paste .conf", callback_data="prompt:rclone_conf"),
            InlineKeyboardButton("📥 Download .conf", callback_data="rc:download"),
        ]
    ]
    if remotes:
        del_row = []
        for r in remotes[:10]:
            rname = r.rstrip(":")
            k = _reg_path(rname)
            del_row.append(InlineKeyboardButton(f"🗑 {rname[:12]}", callback_data=f"rc:del:{k}"))
            if len(del_row) == 2:
                rows.append(del_row)
                del_row = []
        if del_row:
            rows.append(del_row)
    rows.append([
        InlineKeyboardButton("🗂 Browse Remotes", callback_data="nav:remotes:0"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
    ])
    return InlineKeyboardMarkup(rows)


def users_manager_keyboard() -> InlineKeyboardMarkup:
    """Authorized Users Manager keyboard (Owner control from bot)."""
    with state_lock:
        users = sorted(AUTHORIZED_USERS)
    rows = [
        [InlineKeyboardButton("➕ Add User ID", callback_data="prompt:add_user")]
    ]
    del_row = []
    for uid in users:
        if uid == OWNER_ID:
            continue
        del_row.append(InlineKeyboardButton(f"🗑 {uid}", callback_data=f"usr:del:{uid}"))
        if len(del_row) == 2:
            rows.append(del_row)
            del_row = []
    if del_row:
        rows.append(del_row)
    rows.append([
        InlineKeyboardButton("⬅️ Control Panel", callback_data="nav:settings"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
    ])
    return InlineKeyboardMarkup(rows)


def flags_manager_keyboard() -> InlineKeyboardMarkup:
    """Rclone Extra Flags Manager keyboard."""
    cur_tokens = set(_rclone_extra_flags())
    rows = []
    pair = []
    for flag_str, label in _POPULAR_FLAGS:
        on = flag_str in cur_tokens
        pair.append(
            InlineKeyboardButton(
                f"{'✅ ' if on else ''}{label}",
                callback_data=f"flg:toggle:{flag_str}",
            )
        )
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)

    rows.append([
        InlineKeyboardButton("✏️ Set Custom Flags", callback_data="prompt:rclone_flags"),
        InlineKeyboardButton("🧹 Clear All Flags", callback_data="flg:clear"),
    ])
    rows.append([
        InlineKeyboardButton("⬅️ Control Panel", callback_data="nav:settings"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
    ])
    return InlineKeyboardMarkup(rows)


def db_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💾 Save Now", callback_data="db:save"),
            InlineKeyboardButton("📥 Reload DB", callback_data="db:reload"),
        ],
        [
            InlineKeyboardButton("▶️ Resume Job", callback_data="action:resume_job"),
            InlineKeyboardButton("🗑 Clear Job", callback_data="db:clear_job"),
        ],
        [
            InlineKeyboardButton("⬅️ Control Panel", callback_data="nav:settings"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def logs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="nav:logs"),
            InlineKeyboardButton("🧹 Clear", callback_data="action:clear_logs"),
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="nav:status"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ],
    ])


def active_job_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="job:refresh"),
            InlineKeyboardButton("🛑 Stop", callback_data="job:cancel"),
            InlineKeyboardButton("⛔ Kill", callback_data="job:kill"),
        ]
    ])


def mode_keyboard(remote_path: str, is_single_file: bool | None = None) -> InlineKeyboardMarkup:
    """
    4-row Transfer Launchpad keyboard:
      • Row 1 (Start):       [▶️ Start (Media)] | [📄 Start (Document)]
      • Row 2 (File Types):  [📦 All] | [🎬 Video] | [🖼 Image] | [📄 Doc]  (or Delete File on Remote if single file)
      • Row 3 (Quick Opts):  [🗑 Del: OFF] | [📏 Cap: OFF] | [⚡ 2x·4w]
      • Row 4 (Navigation):  [⬅️ Back] | [🏠 Home] | [✖️ Close]
    """
    key = _reg_path(remote_path, is_single_file=is_single_file)
    single_mode = _is_single_file_key(key) if is_single_file is None else is_single_file

    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        ft = set(enabled_file_types)

    is_all = (ft >= {"video", "image", "document"})
    only_vid = (ft == {"video"})
    only_img = (ft == {"image"})
    only_doc = (ft == {"document"})

    rows = [
        [
            InlineKeyboardButton("▶️ Start (Media)", callback_data=f"dl:go:video:{key}"),
            InlineKeyboardButton("📄 Start (Document)", callback_data=f"dl:go:doc:{key}"),
        ],
    ]
    if single_mode:
        rows.append([
            InlineKeyboardButton("🗑 Delete This File from Remote", callback_data=f"dl:rmfile:{key}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("✅ All" if is_all else "📦 All", callback_data=f"dl:ft:all:{key}"),
            InlineKeyboardButton("✅ Video" if only_vid else "🎬 Video", callback_data=f"dl:ft:video:{key}"),
            InlineKeyboardButton("✅ Image" if only_img else "🖼 Image", callback_data=f"dl:ft:image:{key}"),
            InlineKeyboardButton("✅ Doc" if only_doc else "📄 Doc", callback_data=f"dl:ft:document:{key}"),
        ])

    parent_cb = "nav:remotes:0"
    if ":" in remote_path:
        r_prefix, r_rest = remote_path.split(":", 1)
        r_rest = r_rest.strip("/")
        if r_rest:
            if single_mode:
                parent_rest = r_rest.rsplit("/", 1)[0] if "/" in r_rest else ""
                parent_path = f"{r_prefix}:{parent_rest}" if parent_rest else f"{r_prefix}:"
                parent_cb = f"br:files:{_reg_path(parent_path, is_single_file=False)}:0"
            else:
                parent_cb = f"br:dir:{key}:0"

    ret_tag = f"s_{key}" if single_mode else f"d_{key}"
    rows += [
        [
            InlineKeyboardButton(f"🗑 Del: {'ON' if del_on else 'OFF'}", callback_data=f"dl:del:{key}"),
            InlineKeyboardButton(f"📏 Cap: {f'{cap_mb}M' if size_on else 'OFF'}", callback_data=f"dl:size:{key}"),
            InlineKeyboardButton(f"⚡ {conc}x·{up_w}w", callback_data=f"nav:concurrent:{ret_tag}"),
        ],
        [
            InlineKeyboardButton("⬅️ Back", callback_data=parent_cb),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
            InlineKeyboardButton("✖️ Close", callback_data="action:close"),
        ],
    ]
    return InlineKeyboardMarkup(rows)


def _build_home_text() -> str:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        streams = RCLONE_STREAMS
        dump_id = DUMP_CHAT_ID
        is_running = bool(active_sessions)
        def_mode = "Media" if DEFAULT_UPLOAD_MODE == "video" else "Document"
        ft_badge = _fmt_file_types_badge(enabled_file_types)
    with stats_lock:
        done = stats["total_done"]
        failed = stats["total_failed"]
        tbytes = stats["total_bytes"]

    state_str = "🟡 **Transferring**" if is_running else "🟢 **Ready**"
    db_str = "MongoDB" if db_mgr.connected else "Local JSON"

    return (
        f"☁️ **Rclone ➔ Telegram Bot**\n"
        f"{state_str} · ⏱ `{fmt_uptime()}` · 🗄 `{db_str}`\n\n"
        f"• 🎯 **File Types:** `{ft_badge}`\n"
        f"• ⚡ **Speed:** `{conc}x Jobs · {up_w}w Upload · {streams}s Rclone`\n"
        f"• 📢 **Dump Chat:** `{dump_id}` · 📤 **Mode:** `{def_mode}`\n"
        f"• 🗑 **Auto-Delete:** `{'ON' if del_on else 'OFF'}` · 📏 **Size Cap:** `{f'{cap_mb} MB' if size_on else 'OFF'}`\n"
        f"• 📊 **Transferred:** `{done}` files (`{fmt_size(tbytes)}`) · `{failed}` failed"
    )


def _build_filetypes_text() -> str:
    with state_lock:
        ft = set(enabled_file_types)
        ft_badge = _fmt_file_types_badge(ft)

    vid_st = "ON 🟢" if "video" in ft else "OFF 🔴"
    img_st = "ON 🟢" if "image" in ft else "OFF 🔴"
    doc_st = "ON 🟢" if "document" in ft else "OFF 🔴"

    return (
        f"🎯 **File Types Filter**\n"
        f"Active: **`{ft_badge}`** _(Saved in MongoDB)_\n\n"
        f"• 🎬 **Videos:** `{vid_st}` (`mp4, mkv, mov, avi, webm, ts…`)\n"
        f"• 🖼 **Images:** `{img_st}` (`jpg, png, webp, gif, heic…`)\n"
        f"• 📄 **Documents:** `{doc_st}` (`pdf, zip, rar, docx, xlsx, txt, apk…`)\n\n"
        f"_Choose a 1-tap mode or toggle individual types below:_"
    )


def _build_concurrent_text() -> str:
    with state_lock:
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        streams = RCLONE_STREAMS
        buf = RCLONE_BUFFER_SIZE
        bw = BW_LIMIT or "Unlimited"

    return (
        f"⚡ **Speed & Concurrency** _(Saved in MongoDB)_\n\n"
        f"• **Parallel Jobs:** `{conc}x` (Pipelined Download + Upload)\n"
        f"• **Upload Workers:** `{up_w}w` per file\n"
        f"• **Rclone Streams:** `{streams}s` · **Buffer:** `{buf}`\n"
        f"• **Bandwidth Limit:** `{bw}`\n\n"
        f"**Presets:** `🛡 Safe (1x·2w)` · `⚡ Fast (2x·4w)` · `🚀 Turbo (3x·6w)`"
    )


def _build_status_text() -> str:
    with stats_lock:
        snap = dict(stats)
    with state_lock:
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        streams = RCLONE_STREAMS
        buf = RCLONE_BUFFER_SIZE
        dump_id = DUMP_CHAT_ID
        is_running = bool(active_sessions)
        ft_badge = _fmt_file_types_badge(enabled_file_types)

    badge = "🟡 **Transferring**" if is_running else "🟢 **Online & Idle**"
    db_state = f"MongoDB (`{MONGO_DB_NAME}`)" if db_mgr.connected else "Local JSON"
    cur = snap["current_files"]

    lines = [
        f"📊 **System Status & Tools**",
        f"{badge} · ⏱ `{fmt_uptime()}` · 🗄 {db_state}",
        "",
        f"• ✅ **Done:** `{snap['total_done']}` (`{fmt_size(snap['total_bytes'])}`) · ❌ **Failed:** `{snap['total_failed']}` · ⏭ **Skipped:** `{snap.get('total_skipped', 0)}`",
        f"• 🎯 **Types:** `{ft_badge}`",
        f"• ⚡ **Speed:** `{conc}x Jobs · {up_w}w Upload · {streams}s/{buf} Rclone`",
        f"• 📢 **Dump Chat:** `{dump_id}`",
    ]
    res = _server_resources()
    if res:
        lines.append(f"• {res}")
    if snap["floodwait_count"] > 0:
        lines.append(f"• ⏳ **FloodWaits Handled:** `{snap['floodwait_count']}x` (last `{snap['floodwait_last_secs']}s`)")
    if cur:
        lines.append(f"\n🔄 **Active:** `{'`, `'.join(cur[:3])}`")
    if snap["last_file"]:
        lines.append(f"📄 **Last Sent:** `{_remote_basename(snap['last_file'])}`")
    if snap["last_error"]:
        lines.append(f"⚠️ **Last Error:** `{snap['last_error'][:90]}`")
    return "\n".join(lines)


def _build_settings_text() -> str:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        split_mb = SPLIT_SIZE // (1024 * 1024)
        bw = BW_LIMIT or "Unlimited"
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        streams = RCLONE_STREAMS
        buf = RCLONE_BUFFER_SIZE
        refr = BOARD_REFRESH_INTERVAL
        auto_res = AUTO_RESUME_JOBS
        dump_id = DUMP_CHAT_ID
        def_mode = "🎬 Media" if DEFAULT_UPLOAD_MODE == "video" else "📄 Document"
        flags_str = RCLONE_FLAGS or "None"
        users_cnt = len(AUTHORIZED_USERS)
        ft_badge = _fmt_file_types_badge(enabled_file_types)

    return (
        f"⚙️ **Bot Control Panel** _(100% Synced to MongoDB)_\n\n"
        f"• 🎯 **File Types:** `{ft_badge}`\n"
        f"• ⚡ **Speed:** `{conc}x Jobs · {up_w}w Upload · {streams}s/{buf}`\n"
        f"• 📢 **Dump Chat ID:** `{dump_id}` · 📤 **Default Mode:** `{def_mode}`\n"
        f"• 🗑 **Auto-Delete Remote:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"• ▶️ **Auto-Resume Jobs:** `{'ON 🟢' if auto_res else 'OFF 🔴'}`\n"
        f"• 📏 **Max File Size Cap:** `{f'{cap_mb} MB 🟢' if size_on else 'OFF 🔴'}`\n"
        f"• ✂️ **Split Threshold:** `{split_mb} MB` · 🚦 **BW Limit:** `{bw}`\n"
        f"• 🚩 **Rclone Flags:** `{flags_str}`\n"
        f"• 👥 **Authorized Users:** `{users_cnt}` · ⏱ **Refresh:** `{refr:.0f}s`"
    )


def _build_dump_chat_text() -> str:
    with state_lock:
        dump_id = DUMP_CHAT_ID
    is_owner = (dump_id == OWNER_ID)
    label = f"`{dump_id}` _(Owner DM)_" if is_owner else f"`{dump_id}` _(Channel / Group)_"
    return (
        f"📢 **Dump Chat / Upload Destination**\n\n"
        f"• **Current Target:** {label}\n\n"
        f"**How to change from the bot:**\n"
        f"1. Tap **📍 Use This Chat** to upload files right here.\n"
        f"2. Tap **👤 Use Owner DM** to upload to your private chat.\n"
        f"3. Tap **✏️ Enter ID / Forward Msg** and send a channel/group ID (e.g. `-1001234567890`) or forward any message from that channel."
    )


async def _build_rclone_manager_text() -> tuple[str, list[str]]:
    remotes = await rclone_list_remotes()
    rc_exists = _safe_conf_exists()
    size_b = RCLONE_CONF_PATH.stat().st_size if rc_exists else 0
    rem_list = ", ".join(f"`{r.rstrip(':')}`" for r in remotes) if remotes else "_None configured_"
    text = (
        f"🔧 **Rclone Config Manager (`rclone.conf`)**\n\n"
        f"• 📄 **Status:** `{f'Active ({fmt_size(size_b)})' if rc_exists else 'Missing'}`\n"
        f"• ☁️ **Remotes (`{len(remotes)}`):** {rem_list}\n\n"
        f"_Upload or paste a new `rclone.conf`, download your current config, or tap a remote below to remove it:_"
    )
    return text, remotes


def _build_users_manager_text() -> str:
    with state_lock:
        users = sorted(AUTHORIZED_USERS)
    ulist = "\n".join(
        f"• `{u}`" + (" _(Owner — Permanent)_" if u == OWNER_ID else "")
        for u in users
    )
    return (
        f"👥 **Authorized Users Manager**\n\n"
        f"{ulist}\n\n"
        f"_Tap **➕ Add User ID** to authorize a new user, or tap a user ID button below to revoke access:_"
    )


def _build_flags_manager_text() -> str:
    with state_lock:
        flags_raw = RCLONE_FLAGS
    return (
        f"🚩 **Rclone Extra Flags**\n\n"
        f"• **Active Flags:** `{flags_raw or 'None'}`\n\n"
        f"_Toggle popular flags below with 1 tap, or set custom Rclone flags:_"
    )


async def _build_db_text() -> str:
    unfinished = await db_mgr.get_unfinished_job()
    rc_exists = _safe_conf_exists()
    remotes_cnt = 0
    if rc_exists:
        try:
            remotes_cnt = len(re.findall(r"^\[([^\]]+)\]", RCLONE_CONF_PATH.read_text(encoding="utf-8"), re.MULTILINE))
        except Exception:
            pass

    sync_ago = f"{int(time.time() - db_mgr.last_sync_ts)}s ago" if db_mgr.last_sync_ts > 0 else "never"
    if unfinished:
        done_c = len(unfinished.get("done_files", []))
        tot_c = len(unfinished.get("all_files", []))
        job_str = f"`{unfinished.get('remote_path')}` (`{done_c}/{tot_c}` done)"
    else:
        job_str = "`None`"

    return (
        f"🗄 **MongoDB Persistence**\n\n"
        f"• 📡 **Backend:** `{'MongoDB (' + MONGO_DB_NAME + ')' if db_mgr.connected else 'Local JSON Fallback'}`\n"
        f"• ⏱ **Last Sync:** `{sync_ago}`\n"
        f"• 🔧 **rclone.conf:** `{f'{remotes_cnt} remotes saved' if rc_exists else 'Missing'}`\n"
        f"• 🔄 **Unfinished Job:** {job_str}"
        + (f"\n• ⚠️ **Warning:** `{db_mgr.last_error}`" if db_mgr.last_error else "")
    )


def _build_dl_prompt_text(remote_path: str, is_single_file: bool | None = None) -> str:
    key = _reg_path(remote_path, is_single_file=is_single_file)
    single_mode = _is_single_file_key(key) if is_single_file is None else is_single_file

    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        streams = RCLONE_STREAMS
        dump_id = DUMP_CHAT_ID
        ft_badge = _fmt_file_types_badge(enabled_file_types)

    if single_mode:
        fname_disp = _remote_basename(remote_path, is_remote=True)
        cat = (_classify_file_ext(fname_disp) or "document").capitalize()
        return (
            f"📄 **Single File Transfer**\n"
            f"📂 `{remote_path}`\n\n"
            f"• 🏷 **Detected Type:** `{cat}`\n"
            f"• ⚡ **Speed:** `{conc}x Jobs · {up_w}w Upload · {streams}s Rclone`\n"
            f"• 📢 **Dump Chat:** `{dump_id}`\n"
            f"• 🗑 **Auto-Delete:** `{'ON' if del_on else 'OFF'}` · 📏 **Size Cap:** `{f'{cap_mb} MB' if size_on else 'OFF'}`\n\n"
            f"_Choose upload mode or adjust options below:_"
        )

    return (
        f"🚀 **Folder Transfer Launchpad**\n"
        f"📂 `{remote_path}`\n\n"
        f"• 🎯 **Types:** `{ft_badge}`\n"
        f"• ⚡ **Speed:** `{conc}x Jobs · {up_w}w Upload · {streams}s Rclone`\n"
        f"• 📢 **Dump Chat:** `{dump_id}`\n"
        f"• 🗑 **Auto-Delete:** `{'ON' if del_on else 'OFF'}` · 📏 **Size Cap:** `{f'{cap_mb} MB' if size_on else 'OFF'}`\n\n"
        f"_Choose upload mode or adjust options below:_"
    )


# ==============================================================================
#  STEP 9: High-Speed Async Subprocess Engine (rclone / ffmpeg / ffprobe)
# ==============================================================================
_RCLONE_STATS_RE = re.compile(
    r"(?:Transferred:\s+)?([\d.]+\s*\S+)\s*/\s*([\d.]+\s*\S+),\s*(\d+)%,\s*([\d.]+\s*\S+/s)(?:,\s*ETA\s*(\S+))?",
    re.IGNORECASE,
)


def _parse_rclone_line(line: str) -> dict | None:
    m = _RCLONE_STATS_RE.search(line)
    if not m:
        return None
    return {
        "done_str": m.group(1).strip(),
        "total_str": m.group(2).strip(),
        "pct": int(m.group(3)),
        "speed": m.group(4).strip(),
        "eta": (m.group(5) or "").strip(),
    }


def _parse_size_str(s: str) -> int:
    units = {
        "b": 1, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3,
        "tib": 1024 ** 4, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3,
    }
    s = s.strip().replace(",", "")
    for suffix, mult in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.lower().endswith(suffix):
            try:
                return int(float(s[:-len(suffix)].strip()) * mult)
            except ValueError:
                return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


_last_edits: collections.OrderedDict[tuple[int, int], tuple[str, str]] = collections.OrderedDict()


def _markup_sig(markup: InlineKeyboardMarkup | None) -> str:
    if not markup or not getattr(markup, "inline_keyboard", None):
        return ""
    return "|".join(
        ",".join(f"{getattr(b, 'text', '')}:{getattr(b, 'callback_data', '')}" for b in row)
        for row in markup.inline_keyboard
    )


async def safe_edit(
    msg: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    if len(text) > 4000:
        text = text[:3990] + "\n…"
    key = (msg.chat.id, msg.id)
    sig = _markup_sig(reply_markup)
    if _last_edits.get(key) == (text, sig):
        return
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
        _last_edits[key] = (text, sig)
        while len(_last_edits) > 200:
            _last_edits.popitem(last=False)
    except MessageNotModified:
        _last_edits[key] = (text, sig)
    except FloodWait as e:
        await asyncio.sleep(e.value + 3)
        try:
            await msg.edit_text(text, reply_markup=reply_markup)
            _last_edits[key] = (text, sig)
        except Exception as retry_err:
            log.warning(f"safe_edit retry failed: {retry_err}")
    except Exception as e:
        log.debug(f"safe_edit warning: {e}")


def _rclone_env() -> dict:
    return os.environ.copy()


def _bwlimit_args() -> list[str]:
    with state_lock:
        bw = BW_LIMIT
    if bw and bw.lower() != "off":
        return ["--bwlimit", bw]
    return []


def _register_proc(user_id: int, proc: asyncio.subprocess.Process) -> None:
    if user_id:
        with state_lock:
            active_procs[user_id].add(proc)


def _unregister_proc(user_id: int, proc: asyncio.subprocess.Process) -> None:
    if user_id:
        with state_lock:
            if user_id in active_procs:
                active_procs[user_id].discard(proc)


def kill_user_procs(user_id: int) -> int:
    with state_lock:
        procs = list(active_procs.get(user_id, ()))
        active_procs.pop(user_id, None)
    killed = 0
    for p in procs:
        if p.returncode is None:
            try:
                p.kill()
                killed += 1
            except Exception:
                pass
    return killed


async def _run_cmd_async(cmd: list[str], user_id: int = 0, timeout: float = 120.0) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_rclone_env(),
    )
    _register_proc(user_id, proc)
    try:
        stdout_b, stderr_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return (
            proc.returncode or 0,
            stdout_b.decode("utf-8", errors="replace"),
            stderr_b.decode("utf-8", errors="replace"),
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return (-1, "", "Operation timed out")
    finally:
        _unregister_proc(user_id, proc)


async def rclone_list_remotes() -> list[str]:
    code, out, _ = await _run_cmd_async(["rclone", "listremotes"], timeout=15.0)
    if code != 0:
        return []
    return [r.strip() for r in out.splitlines() if r.strip()]


async def rclone_list_dirs(remote_path: str) -> tuple[list[str], str]:
    remote_path = _normalize_remote_path(remote_path)
    cmd = ["rclone", "lsf", "--dirs-only", remote_path] + _rclone_extra_flags()
    code, out, err = await _run_cmd_async(cmd, timeout=30.0)
    if code != 0:
        return [], (err.strip() or "Failed to list directories")
    dirs = [d.strip().rstrip("/") for d in out.splitlines() if d.strip()]
    return dirs, ""


async def rclone_list_folder_files(remote_path: str) -> tuple[list[tuple[str, int]], str]:
    """List immediate files (non-recursive) in `remote_path` with sizes for the in-bot File Browser."""
    remote_path = _normalize_remote_path(remote_path)
    with state_lock:
        allowed_types = set(enabled_file_types)
    cmd = [
        "rclone", "lsjson", "--files-only", "--no-modtime", "--no-mimetype", remote_path
    ] + _rclone_extra_flags()
    code, out, err = await _run_cmd_async(cmd, timeout=45.0)
    if code != 0:
        return [], (err.strip() or "Failed to list files")
    try:
        items = json.loads(out)
        if not isinstance(items, list):
            return [], ""
        result: list[tuple[str, int]] = []
        for entry in items:
            if entry.get("IsDir"):
                continue
            name = (entry.get("Name") or entry.get("Path") or "").strip().lstrip("/")
            if not name or not _file_matches_types(name, allowed_types):
                continue
            sz = int(entry.get("Size") or 0)
            result.append((name, sz))
        return result, ""
    except Exception as e:
        return [], str(e)


async def rclone_list(
    remote_path: str,
    user_id: int = 0,
    allowed_types: set[str] | None = None,
    skip_type_filter: bool = False,
) -> list[str]:
    if allowed_types is None:
        with state_lock:
            allowed_types = set(enabled_file_types)

    remote_path = _normalize_remote_path(remote_path)
    cmd = ["rclone", "lsf", "--files-only", "-R", "--fast-list", remote_path] + _rclone_extra_flags()
    code, out, err = await _run_cmd_async(cmd, user_id=user_id, timeout=180.0)
    if code != 0:
        raise RuntimeError(err.strip() or "rclone lsf failed")

    raw_files = [f.strip().lstrip("/") for f in out.splitlines() if f.strip()]
    if skip_type_filter:
        return raw_files
    # If the user pointed directly to a single file on the remote, never filter it out
    if len(raw_files) == 1 and (
        remote_path.endswith("/" + raw_files[0])
        or remote_path.endswith(":" + raw_files[0])
    ):
        return raw_files
    return [f for f in raw_files if _file_matches_types(f, allowed_types)]


async def rclone_size_map(remote_path: str, user_id: int = 0) -> dict[str, int]:
    remote_path = _normalize_remote_path(remote_path)
    cmd = [
        "rclone", "lsjson", "-R", "--files-only", "--fast-list",
        "--no-modtime", "--no-mimetype", remote_path,
    ] + _rclone_extra_flags()
    try:
        code, out, _ = await _run_cmd_async(cmd, user_id=user_id, timeout=180.0)
        if code != 0:
            return {}
        data = json.loads(out)
        if not isinstance(data, list):
            return {}
        res = {}
        for entry in data:
            if entry.get("IsDir"):
                continue
            rel = (entry.get("Path") or entry.get("Name") or "").lstrip("/")
            size = entry.get("Size", -1)
            if rel and isinstance(size, (int, float)) and size >= 0:
                res[rel] = int(size)
        return res
    except Exception:
        return {}


async def rclone_size(remote_path: str, user_id: int = 0) -> int:
    remote_path = _normalize_remote_path(remote_path)
    cmd = ["rclone", "size", "--json", remote_path] + _rclone_extra_flags()
    try:
        code, out, _ = await _run_cmd_async(cmd, user_id=user_id, timeout=30.0)
        if code == 0 and out:
            data = json.loads(out)
            b = data.get("bytes", -1)
            if isinstance(b, (int, float)) and b >= 0:
                return int(b)
    except Exception:
        pass
    return -1


async def rclone_download(
    remote_path: str,
    dest_dir: str,
    progress_callback=None,
    user_id: int = 0,
    dest_filename: str | None = None,
) -> str:
    """
    High-throughput Rclone downloader:
      • Uses chunked `stderr.read(4096)` splitting on both `\\r` and `\\n`.
      • Preserves filenames containing colons (`:`).
    """
    _filename = (dest_filename or _remote_basename(remote_path, is_remote=True)).replace("\x00", "").strip() or "file"
    local_dest = os.path.abspath(os.path.join(dest_dir, _filename))

    with state_lock:
        buf_size = RCLONE_BUFFER_SIZE
        streams_cnt = str(RCLONE_STREAMS)

    cmd = [
        "rclone", "copyto",
        "--stats=2s",
        "--stats-one-line",
        "--stats-log-level", "NOTICE",
        "--buffer-size", buf_size,
        "--multi-thread-streams", streams_cnt,
        "--multi-thread-cutoff", "16M",
        "--multi-thread-chunk-size", "32M",
        "--transfers", "4",
        "--checkers", "8",
    ] + _bwlimit_args() + _rclone_extra_flags() + [
        remote_path, local_dest,
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stderr=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        env=_rclone_env(),
    )
    _register_proc(user_id, proc)

    last_line = ""
    last_progress_call = 0.0
    _ansi_re = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    partial_buf = ""

    try:
        assert proc.stderr is not None
        while True:
            if force_kill_flags.get(user_id):
                proc.kill()
                raise RuntimeError("Job aborted by user")
            try:
                chunk_bytes = await asyncio.wait_for(proc.stderr.read(4096), timeout=180.0)
            except asyncio.TimeoutError:
                proc.kill()
                raise RuntimeError("Download stalled (180s timeout)")
            if not chunk_bytes:
                break

            partial_buf += chunk_bytes.decode("utf-8", errors="replace")
            if len(partial_buf) > 32768:
                partial_buf = partial_buf[-16384:]

            parts = re.split(r"[\r\n]+", partial_buf)
            partial_buf = parts.pop() if parts else ""

            for raw_line in parts:
                line = _ansi_re.sub("", raw_line).strip()
                if not line:
                    continue
                last_line = line
                parsed = _parse_rclone_line(line)
                if not parsed:
                    log.info(f"rclone: {line}")
                elif progress_callback:
                    now = time.monotonic()
                    if last_progress_call == 0.0 or now - last_progress_call >= 1.8:
                        last_progress_call = now
                        done_bytes = _parse_size_str(parsed["done_str"])
                        total_bytes = _parse_size_str(parsed["total_str"])
                        progress_callback(
                            done_bytes,
                            total_bytes,
                            parsed["pct"],
                            parsed["speed"],
                            parsed["eta"],
                        )

        if partial_buf.strip():
            line = _ansi_re.sub("", partial_buf).strip()
            if line:
                last_line = line
        await proc.wait()
    finally:
        _unregister_proc(user_id, proc)

    if force_kill_flags.get(user_id):
        raise RuntimeError("Job aborted by user")
    if proc.returncode != 0:
        raise RuntimeError(last_line or "rclone copyto failed")
    if not os.path.exists(local_dest):
        raise FileNotFoundError(f"Downloaded file not found: {local_dest}")
    return local_dest


async def rclone_delete(remote_path: str, user_id: int = 0) -> None:
    cmd = ["rclone", "deletefile", remote_path] + _rclone_extra_flags()
    code, _, err = await _run_cmd_async(cmd, user_id=user_id, timeout=60.0)
    if code != 0:
        raise RuntimeError(err.strip() or "rclone deletefile failed")
    log.info(f"🗑 Deleted from remote: {remote_path}")


def _remove_rclone_remote_section(conf_text: str, remote_name: str) -> str:
    """Remove `[remote_name]` section from `rclone.conf` text."""
    pattern = re.compile(
        rf"^\[{re.escape(remote_name)}\]\s*\n(?:(?!^\[).*\n?)*",
        re.MULTILINE,
    )
    cleaned = pattern.sub("", conf_text).strip()
    return (cleaned + "\n") if cleaned else ""


def _parse_ffprobe_duration(data: dict[str, Any]) -> float:
    fmt_dur = data.get("format", {}).get("duration")
    if fmt_dur is not None:
        try:
            val = float(fmt_dur)
            if val > 0:
                return val
        except (ValueError, TypeError):
            pass

    for stream in data.get("streams", []):
        s_dur = stream.get("duration")
        if s_dur is not None:
            try:
                val = float(s_dur)
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass
        tags = stream.get("tags") or {}
        for k, v in tags.items():
            if k.upper().startswith("DURATION") and isinstance(v, str):
                m = re.match(r"^(\d+):(\d+):([\d.]+)", v.strip())
                if m:
                    try:
                        val = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
                        if val > 0:
                            return val
                    except ValueError:
                        pass
    return 0.0


async def probe_video_metadata(local_path: str, user_id: int = 0) -> tuple[int, int, int]:
    rc, stdout, _ = await _run_cmd_async(
        [
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", "-show_streams",
            local_path,
        ],
        user_id=user_id,
        timeout=25.0,
    )
    if rc != 0 or not stdout:
        return (0, 0, 0)
    try:
        data = json.loads(stdout)
        duration = round(_parse_ffprobe_duration(data))
        width, height = 0, 0
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                width = int(s.get("width") or 0)
                height = int(s.get("height") or 0)
                if width > 0 and height > 0:
                    break
        return (max(0, duration), max(0, width), max(0, height))
    except Exception:
        return (0, 0, 0)


async def generate_thumbnail(
    local_path: str,
    user_id: int = 0,
    duration: float | int | None = None,
) -> str | None:
    thumb_path = f"{local_path}_thumb.jpg"
    try:
        dur_val = float(duration or 0)
        if dur_val <= 0:
            probed_dur, _, _ = await probe_video_metadata(local_path, user_id=user_id)
            dur_val = float(probed_dur)

        if dur_val > 0:
            mid_point = round(dur_val / 2.0, 2)
            candidate_timestamps = [
                f"{mid_point:.2f}",
                f"{max(0.0, dur_val * 0.25):.2f}",
                "0",
            ]
        else:
            candidate_timestamps = ["3", "0"]

        vf_scale = "scale='min(320,iw)':'min(320,ih)':force_original_aspect_ratio=decrease"

        for seek_ts in candidate_timestamps:
            await _run_cmd_async(
                [
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-ss", seek_ts,
                    "-i", local_path,
                    "-frames:v", "1",
                    "-vf", vf_scale,
                    "-q:v", "3",
                    "-threads", "1",
                    thumb_path,
                ],
                user_id=user_id,
                timeout=25.0,
            )
            if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
                log.info(
                    f"🖼 Midpoint thumbnail extracted at {seek_ts}s "
                    f"(video duration={dur_val:.1f}s) for {os.path.basename(local_path)}"
                )
                return thumb_path
    except Exception as e:
        log.debug(f"Thumbnail extraction warning for {local_path}: {e}")
    finally:
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) == 0:
            try:
                os.remove(thumb_path)
            except OSError:
                pass
    return None


def _split_binary_file_sync(local_path: str, split_dir_path: Path, chunk_size: int, user_id: int = 0) -> list[str]:
    filename = os.path.basename(local_path)
    out_paths: list[str] = []
    buf_len = 16 * 1024 * 1024
    with open(local_path, "rb") as src:
        part_num = 1
        while True:
            if force_kill_flags.get(user_id):
                raise RuntimeError("Job aborted by user")
            first_block = src.read(buf_len)
            if not first_block:
                break
            part_path = str(split_dir_path / f"{filename}.part{part_num:03d}")
            written = 0
            with open(part_path, "wb") as dst:
                dst.write(first_block)
                written += len(first_block)
                while written < chunk_size:
                    if force_kill_flags.get(user_id):
                        raise RuntimeError("Job aborted by user")
                    to_read = min(buf_len, chunk_size - written)
                    blk = src.read(to_read)
                    if not blk:
                        break
                    dst.write(blk)
                    written += len(blk)
            out_paths.append(part_path)
            part_num += 1
    return out_paths


async def split_file_async(local_path: str, split_dir: str, user_id: int = 0) -> list[str]:
    with state_lock:
        cur_split_limit = SPLIT_SIZE
    size = os.path.getsize(local_path)
    if size <= cur_split_limit:
        return [local_path]

    split_dir_path = Path(split_dir)
    split_dir_path.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(local_path)
    stem, extension = os.path.splitext(filename)
    ext_clean = extension.lstrip(".").lower()

    async def _binary_fallback() -> list[str]:
        try:
            parts = await asyncio.to_thread(
                _split_binary_file_sync,
                local_path,
                split_dir_path,
                cur_split_limit - 2_000_000,
                user_id,
            )
            return parts or [local_path]
        except Exception as e:
            if force_kill_flags.get(user_id):
                raise
            log.warning(f"Binary split fallback warning for {filename}: {e}")
            shutil.rmtree(split_dir_path, ignore_errors=True)
            return [local_path]

    if ext_clean not in VIDEO_EXTS:
        return await _binary_fallback()

    duration, _, _ = await probe_video_metadata(local_path, user_id=user_id)
    if duration <= 0:
        return await _binary_fallback()

    split_size = cur_split_limit - 3_000_000
    start_time = 0
    i = 1
    multi_streams = True
    out_paths: list[str] = []

    while start_time < duration - 4 and i <= 100:
        if force_kill_flags.get(user_id):
            raise RuntimeError("Job aborted by user")
        out_path = str(split_dir_path / f"{stem}.part{i:03}{extension}")

        prefix = ["taskset", "-c", f"{cores}"] if (cores != "0" and _HAS_TASKSET) else []
        head = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-ss", str(start_time),
            "-i", local_path,
            "-fs", str(split_size),
        ]
        maps = ["-map", "0"] if multi_streams else []
        tail = [
            "-map_chapters", "-1",
            "-async", "1",
            "-strict", "-2",
            "-c", "copy",
            "-threads", f"{threads}",
            out_path,
        ]
        rc, _, _ = await _run_cmd_async(prefix + head + maps + tail, user_id=user_id, timeout=600.0)

        if rc != 0 or not os.path.exists(out_path):
            try:
                os.remove(out_path)
            except OSError:
                pass
            if multi_streams:
                multi_streams = False
                continue
            for p in out_paths:
                try:
                    os.remove(p)
                except OSError:
                    pass
            return await _binary_fallback()

        out_size = os.path.getsize(out_path)
        if out_size > cur_split_limit:
            new_split_size = split_size - (out_size - cur_split_limit) - 5_000_000
            try:
                os.remove(out_path)
            except OSError:
                pass
            if new_split_size < 50_000_000:
                for p in out_paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
                return await _binary_fallback()
            split_size = new_split_size
            continue

        lpd, _, _ = await probe_video_metadata(out_path, user_id=user_id)
        if lpd <= 0:
            try:
                os.remove(out_path)
            except OSError:
                pass
            break
        elif lpd >= duration:
            out_paths.append(out_path)
            break
        elif lpd <= 3:
            try:
                os.remove(out_path)
            except OSError:
                pass
            break

        out_paths.append(out_path)
        start_time += max(1, lpd - 2)
        i += 1

    return out_paths if out_paths else await _binary_fallback()


async def _send_with_retry(coro_fn, *args, max_retries: int = 5, on_retry=None, **kwargs):
    attempts = 0
    while True:
        try:
            return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            attempts += 1
            wait = e.value + 3
            log.warning(f"FloodWait {e.value}s (attempt {attempts}/{max_retries})")
            with stats_lock:
                stats["floodwait_count"] += 1
                stats["floodwait_last_secs"] = e.value
                stats["floodwait_last_time"] = time.time()
            if attempts >= max_retries:
                raise
            await asyncio.sleep(wait)
            if on_retry is not None:
                on_retry()


# ==============================================================================
#  STEP 10: Live Transfer Progress Board (_BoardState)
# ==============================================================================
def _render_board(total_files: int, concurrent: int, progress_map: dict, user_id: int = 0) -> str:
    is_cancelling = cancel_flags.get(user_id, False)
    with state_lock:
        up_w = UPLOAD_WORKERS
        ft_short = _fmt_file_types_short(enabled_file_types)
        snap_items = list(progress_map.items())

    done = sum(1 for _, v in snap_items if v.startswith(("✅ ", "✅🗑", "✅⚠️")))
    failed = sum(1 for _, v in snap_items if v.startswith("❌"))
    skipped = sum(1 for _, v in snap_items if v.startswith("⏭"))
    aborted = sum(1 for _, v in snap_items if v.startswith("🛑"))

    active_entries = {
        k: v for k, v in snap_items
        if not (v.startswith(("✅ ", "✅🗑", "✅⚠️")) or v.startswith("❌") or v.startswith("⏭") or v.startswith("🛑"))
    }
    queued = max(0, total_files - done - failed - skipped - aborted - len(active_entries))

    overall_done = done + failed + skipped + aborted
    overall_bar = progress_bar(overall_done, total_files, 10)
    overall_pct = int(overall_done * 100 / total_files) if total_files > 0 else 0

    state_tag = "🛑 **Stopping…**" if is_cancelling else f"⚡ `{concurrent}x·{up_w}w` · 🎯 `{ft_short}`"
    header = (
        f"📡 **Transferring** · {state_tag}\n"
        f"`{overall_bar}` **{overall_pct}%** (`{overall_done}/{total_files}` files)"
    )

    if active_entries:
        body = "\n\n".join(active_entries[k] for k in sorted(active_entries))
    else:
        body = "⏳ _Preparing next file…_"

    counts = (
        f"✅ `{done}` Done · 🔄 `{len(active_entries)}` Active · ⏳ `{queued}` Queued · ❌ `{failed}`"
        + (f" · ⏭ `{skipped}`" if skipped else "")
    )
    res = _server_resources()
    footer = f"{counts}\n{res}" if res else counts

    text = f"{header}\n\n{body}\n\n{footer}"
    if len(text) > 4000:
        truncated = text[:3980]
        cut = truncated.rfind("\n")
        text = (truncated[:cut] if cut > 0 else truncated) + "\n…"
    return text


class _BoardState:
    def __init__(self, status_msg: Message, total_files: int, concurrent: int, progress_map: dict, user_id: int):
        self.status_msg = status_msg
        self.total_files = total_files
        self.concurrent = concurrent
        self.progress_map = progress_map
        self.user_id = user_id
        self.dirty = False
        self.running = False
        self._task: asyncio.Task | None = None

    def mark_dirty(self):
        self.dirty = True

    async def force_refresh(self):
        self.dirty = False
        text = _render_board(self.total_files, self.concurrent, self.progress_map, self.user_id)
        markup = active_job_keyboard() if self.running else None
        await safe_edit(self.status_msg, text, reply_markup=markup)

    async def start(self):
        self.running = True
        self.dirty = True
        active_boards[self.user_id] = self
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self.running = False
        active_boards.pop(self.user_id, None)
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        while self.running:
            with state_lock:
                interval = BOARD_REFRESH_INTERVAL
            await asyncio.sleep(interval)
            if self.dirty and self.running:
                await self.force_refresh()


async def _push_board(
    status_msg: Message,
    total_files: int,
    concurrent: int,
    progress_map: dict,
    board_state: "_BoardState | None" = None,
    user_id: int = 0,
) -> None:
    if board_state is not None:
        board_state.mark_dirty()
    else:
        text = _render_board(total_files, concurrent, progress_map, user_id)
        await safe_edit(status_msg, text, reply_markup=active_job_keyboard())


# ==============================================================================
#  STEP 11: Pipelined Transfer Engine (Parallel Download + Upload Semaphores)
# ==============================================================================
async def upload_part_with_progress(
    part_path: str,
    caption: str,
    mode: str,
    thumb: str | None,
    idx: int,
    total_files: int,
    fname: str,
    part_idx: int,
    total_parts: int,
    file_size: int,
    progress_map: dict,
    status_msg: Message,
    user_id: int,
    reply_to_message_id: int | None = None,
    board_state: "_BoardState | None" = None,
    duration: int = 0,
    width: int = 0,
    height: int = 0,
) -> Message | None:
    loop = asyncio.get_running_loop()
    fname_disp = _remote_basename(fname, is_remote=False)
    base_label = f"📤 `[{idx}/{total_files}]` `{fname_disp}`"
    part_str = f"{part_idx}/{total_parts}" if total_parts > 1 else ""
    last_cb_time = [0.0]
    upload_start = [loop.time()]

    with state_lock:
        target_chat_id = DUMP_CHAT_ID

    def _reset_timer():
        upload_start[0] = loop.time()
        last_cb_time[0] = 0.0

    async def progress_handler(current: int, total: int, *_) -> None:
        if force_kill_flags.get(user_id):
            raise RuntimeError("Upload aborted by user")
        now = loop.time()
        if current < total and (now - last_cb_time[0]) < 1.5:
            return
        last_cb_time[0] = now

        elapsed = max(0.001, now - upload_start[0])
        speed_bps = current / elapsed if elapsed > 0 else 0
        remaining = max(0.0, (total - current) / speed_bps) if speed_bps > 0 else 0

        speed_str = fmt_size(int(speed_bps)) + "/s" if speed_bps > 0 else ""
        eta_str = fmt_eta(remaining)

        with state_lock:
            progress_map[idx] = make_progress_line(
                base_label,
                current,
                total,
                speed_str=speed_str,
                eta_str=eta_str,
                elapsed_str=fmt_eta(elapsed),
                part_info=part_str,
            )
        if board_state is not None:
            board_state.mark_dirty()

    with state_lock:
        progress_map[idx] = make_progress_line(base_label, 0, file_size, part_info=part_str)
    if board_state is not None:
        board_state.mark_dirty()

    part_ext = os.path.basename(part_path).rsplit(".", 1)[-1].lower() if "." in os.path.basename(part_path) else ""

    if mode == "video" and part_ext in VIDEO_EXTS:
        reply_kw = _build_reply_kwargs(app.send_video, reply_to_message_id)
        thumb_kw = _build_thumb_kwargs(app.send_video, thumb)
        video_meta_kw: dict[str, Any] = {}
        if duration > 0:
            video_meta_kw["duration"] = duration
        if width > 0 and height > 0:
            video_meta_kw["width"] = width
            video_meta_kw["height"] = height
        return await _send_with_retry(
            app.send_video, target_chat_id, part_path,
            caption=caption, supports_streaming=True,
            progress=progress_handler,
            on_retry=_reset_timer,
            **thumb_kw,
            **video_meta_kw,
            **reply_kw,
        )
    elif part_ext in AUDIO_EXTS and mode != "document":
        reply_kw = _build_reply_kwargs(app.send_audio, reply_to_message_id)
        thumb_kw = _build_thumb_kwargs(app.send_audio, thumb)
        return await _send_with_retry(
            app.send_audio, target_chat_id, part_path,
            caption=caption,
            progress=progress_handler,
            on_retry=_reset_timer,
            **thumb_kw,
            **reply_kw,
        )
    elif part_ext in PHOTO_SENDABLE_EXTS and mode != "document" and file_size <= 10 * 1024 * 1024:
        reply_kw = _build_reply_kwargs(app.send_photo, reply_to_message_id)
        try:
            return await _send_with_retry(
                app.send_photo, target_chat_id, part_path,
                caption=caption,
                progress=progress_handler,
                on_retry=_reset_timer,
                **reply_kw,
            )
        except Exception as photo_err:
            log.info(f"send_photo fallback to send_document for {fname_disp}: {photo_err}")
            _reset_timer()
            reply_doc_kw = _build_reply_kwargs(app.send_document, reply_to_message_id)
            return await _send_with_retry(
                app.send_document, target_chat_id, part_path,
                caption=caption,
                progress=progress_handler,
                on_retry=_reset_timer,
                **reply_doc_kw,
            )
    else:
        reply_kw = _build_reply_kwargs(app.send_document, reply_to_message_id)
        thumb_kw = _build_thumb_kwargs(app.send_document, thumb)
        return await _send_with_retry(
            app.send_document, target_chat_id, part_path,
            caption=caption,
            progress=progress_handler,
            on_retry=_reset_timer,
            **thumb_kw,
            **reply_kw,
        )


async def process_one_file(
    fname: str,
    full_remote: str,
    idx: int,
    total: int,
    mode: str,
    user_id: int,
    dl_semaphore: asyncio.Semaphore,
    ul_semaphore: asyncio.Semaphore,
    pipeline_semaphore: asyncio.Semaphore,
    progress_map: dict,
    status_msg: Message,
    results: dict,
    job_concurrent: int,
    do_delete: bool = False,
    board_state: "_BoardState | None" = None,
    size_map: dict[str, int] | None = None,
    checkpoint_cb=None,
) -> None:
    async with pipeline_semaphore:
        if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
            return

        file_dir = os.path.join(DOWNLOAD_DIR, f"job_{user_id}_{idx}")
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        file_cat = _classify_file_ext(fname) or "document"
        cat_icon = "🎬" if file_cat == "video" else ("🖼" if file_cat == "image" else "📄")
        split_dir_cleanup: str | None = None
        fname_display = _remote_basename(fname, is_remote=False)

        try:
            # ── Phase 1: Rclone Download (under dl_semaphore) ──
            async with dl_semaphore:
                if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
                    return

                with stats_lock:
                    if fname_display not in stats["current_files"]:
                        stats["current_files"] = (stats["current_files"] + [fname_display])[-4:]

                dl_start = time.monotonic()

                def dl_progress(done_bytes, total_bytes, pct, speed, eta):
                    with state_lock:
                        progress_map[idx] = make_progress_line(
                            f"📥 `[{idx}/{total}]` `{fname_display}`",
                            done_bytes,
                            total_bytes,
                            speed_str=speed,
                            eta_str=eta,
                            elapsed_str=fmt_eta(time.monotonic() - dl_start),
                        )
                    if board_state is not None:
                        board_state.mark_dirty()

                with state_lock:
                    progress_map[idx] = f"📥 `[{idx}/{total}]` `{fname_display}`\n⏳ _Connecting to remote…_"
                await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)

                with state_lock:
                    size_cap_on = max_size_enabled
                    size_cap = max_size_bytes
                if size_cap_on:
                    remote_size = size_map.get(fname, -1) if size_map else -1
                    if remote_size < 0:
                        remote_size = await rclone_size(full_remote, user_id=user_id)
                    if remote_size >= 0 and remote_size > size_cap:
                        with state_lock:
                            progress_map[idx] = (
                                f"⏭ `[{idx}/{total}]` Skipped: `{fname_display}` "
                                f"(`{fmt_size(remote_size)}` > `{fmt_size(size_cap)}`)"
                            )
                            results["skipped"].append(fname)
                        with stats_lock:
                            stats["total_skipped"] = stats.get("total_skipped", 0) + 1
                        if checkpoint_cb:
                            await checkpoint_cb()
                        return

                try:
                    local_path = await rclone_download(
                        full_remote,
                        file_dir,
                        dl_progress,
                        user_id=user_id,
                        dest_filename=fname_display,
                    )
                except Exception as e:
                    if force_kill_flags.get(user_id):
                        with state_lock:
                            progress_map[idx] = f"🛑 `[{idx}/{total}]` Aborted: `{fname_display}`"
                        return
                    log.error(f"[{idx}/{total}] Download failed: {fname} — {e}")
                    with state_lock:
                        progress_map[idx] = f"❌ `[{idx}/{total}]` DL failed: `{fname_display}`"
                        results["failed"].append(fname)
                    with stats_lock:
                        stats["total_failed"] += 1
                        stats["last_error"] = f"DL {fname_display}: {str(e)[:90]}"
                    if checkpoint_cb:
                        await checkpoint_cb()
                    return

                file_size = os.path.getsize(local_path)
                dl_elapsed = time.monotonic() - dl_start
                dl_speed = fmt_size(int(file_size / dl_elapsed)) + "/s" if dl_elapsed > 0 else ""

                with state_lock:
                    progress_map[idx] = (
                        f"📥 `[{idx}/{total}]` `{fname_display}`\n"
                        f"`{'▰' * 10}` **100%** · `{fmt_size(file_size)}`"
                        + (f" · ⚡ `{dl_speed}`" if dl_speed else "")
                    )
                await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)

            # ── Phase 2: Split, Thumbnail & Telegram Upload (under ul_semaphore) ──
            async with ul_semaphore:
                if force_kill_flags.get(user_id):
                    with state_lock:
                        progress_map[idx] = f"🛑 `[{idx}/{total}]` Aborted: `{fname_display}`"
                    return

                with state_lock:
                    cur_split_limit = SPLIT_SIZE
                needs_split = file_size > cur_split_limit

                try:
                    if needs_split:
                        with state_lock:
                            progress_map[idx] = (
                                f"✂️ `[{idx}/{total}]` `{fname_display}`\n"
                                f"🔄 _Splitting `{fmt_size(file_size)}` into `{fmt_size(cur_split_limit)}` parts…_"
                            )
                        await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)
                        split_dir = local_path + "_parts"
                        split_dir_cleanup = split_dir
                        parts = await split_file_async(local_path, split_dir, user_id=user_id)
                        if local_path not in parts:
                            try:
                                os.remove(local_path)
                            except OSError:
                                pass
                    else:
                        parts = [local_path]

                    total_parts = len(parts)
                    prev_msg_id: int | None = None
                    all_parts_sent = True

                    for part_idx, part_path in enumerate(parts, 1):
                        if force_kill_flags.get(user_id):
                            raise RuntimeError("Job aborted by user")
                        part_size = os.path.getsize(part_path)
                        part_filename = os.path.basename(part_path)
                        part_ext = part_filename.rsplit(".", 1)[-1].lower() if "." in part_filename else ""

                        if total_parts > 1:
                            caption = (
                                f"{cat_icon} `{part_filename}`\n"
                                f"🧩 **Part {part_idx}/{total_parts}** · `{fmt_size(part_size)}` "
                                f"(_Total: `{fmt_size(file_size)}`)"
                            )
                        else:
                            caption = f"{cat_icon} `{fname_display}` · `{fmt_size(file_size)}`"

                        part_thumb: str | None = None
                        part_dur, part_w, part_h = 0, 0, 0
                        if part_ext in VIDEO_EXTS:
                            with state_lock:
                                progress_map[idx] = (
                                    f"🖼 `[{idx}/{total}]` `{fname_display}`\n"
                                    f"🎨 _Extracting midpoint thumbnail…_"
                                )
                            await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)
                            part_dur, part_w, part_h = await probe_video_metadata(part_path, user_id=user_id)
                            part_thumb = await generate_thumbnail(
                                part_path,
                                user_id=user_id,
                                duration=part_dur,
                            )

                        try:
                            sent = await upload_part_with_progress(
                                part_path=part_path,
                                caption=caption,
                                mode=mode,
                                thumb=part_thumb,
                                idx=idx,
                                total_files=total,
                                fname=fname,
                                part_idx=part_idx,
                                total_parts=total_parts,
                                file_size=part_size,
                                progress_map=progress_map,
                                status_msg=status_msg,
                                user_id=user_id,
                                reply_to_message_id=prev_msg_id,
                                board_state=board_state,
                                duration=part_dur,
                                width=part_w,
                                height=part_h,
                            )
                        finally:
                            if part_thumb and os.path.exists(part_thumb):
                                try:
                                    os.remove(part_thumb)
                                except OSError:
                                    pass

                        if sent:
                            prev_msg_id = sent.id
                            try:
                                os.remove(part_path)
                            except OSError:
                                pass
                        else:
                            all_parts_sent = False
                        with stats_lock:
                            stats["total_bytes"] += part_size

                    if do_delete and not all_parts_sent:
                        with state_lock:
                            progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` `{fname_display}` (del skipped)"
                            results["delete_failed"].append(fname)
                    elif do_delete:
                        with state_lock:
                            progress_map[idx] = f"🗑 `[{idx}/{total}]` `{fname_display}`\n_Deleting from remote…_"
                        await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)
                        try:
                            await rclone_delete(full_remote, user_id=user_id)
                            with state_lock:
                                progress_map[idx] = f"✅🗑 `[{idx}/{total}]` `{fname_display}` · `{fmt_size(file_size)}`"
                                results["deleted"] += 1
                        except Exception:
                            with state_lock:
                                progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` `{fname_display}` (del failed)"
                                results["delete_failed"].append(fname)
                    else:
                        with state_lock:
                            progress_map[idx] = f"✅ `[{idx}/{total}]` `{fname_display}` · `{fmt_size(file_size)}`"

                    with state_lock:
                        results["success"] += 1
                        results["done_files"].append(fname)
                    with stats_lock:
                        stats["total_done"] += 1
                        stats["last_file"] = fname
                    if checkpoint_cb:
                        await checkpoint_cb()
                    await db_mgr.save_all()
                    if board_state is not None:
                        board_state.mark_dirty()

                except Exception as e:
                    if force_kill_flags.get(user_id):
                        with state_lock:
                            progress_map[idx] = f"🛑 `[{idx}/{total}]` Aborted: `{fname_display}`"
                        return
                    log.error(f"[{idx}/{total}] Transfer failed: {fname} — {e}")
                    with state_lock:
                        progress_map[idx] = f"❌ `[{idx}/{total}]` Failed: `{fname_display}`"
                        results["failed"].append(fname)
                    with stats_lock:
                        stats["total_failed"] += 1
                        stats["last_error"] = f"UL {fname_display}: {str(e)[:90]}"
                    if checkpoint_cb:
                        await checkpoint_cb()
        finally:
            with stats_lock:
                stats["current_files"] = [x for x in stats["current_files"] if x != fname_display]
            if split_dir_cleanup:
                shutil.rmtree(split_dir_cleanup, ignore_errors=True)
            shutil.rmtree(file_dir, ignore_errors=True)


async def _run_job(
    status_msg: Message,
    files: list[str],
    remote_path: str,
    mode: str,
    user_id: int,
    do_delete: bool,
    size_map: dict[str, int] | None = None,
    already_done: list[str] | None = None,
) -> dict:
    global last_failed, last_job_remote, last_job_mode

    remote_path = _normalize_remote_path(remote_path)
    done_ordered = list(dict.fromkeys(already_done or []))
    done_set = set(done_ordered)
    pending_files = [f for f in files if f not in done_set]
    total = len(files)
    mode_icon = "🎬 Media" if mode == "video" else "📄 Document"
    with state_lock:
        job_concurrent = CONCURRENT_JOBS
        up_w = UPLOAD_WORKERS
        ft_badge = _fmt_file_types_badge(enabled_file_types)
    _sync_client_concurrency()

    await safe_edit(
        status_msg,
        f"🚀 **Starting Transfer**\n"
        f"📂 `{remote_path}`\n\n"
        f"• 📦 **Files:** `{len(pending_files)}/{total}` (`{ft_badge}`)\n"
        f"• ⚡ **Speed:** `{job_concurrent}x Jobs · {up_w}w Upload` ({mode_icon})",
        reply_markup=active_job_keyboard(),
    )

    with state_lock:
        active_sessions.add(user_id)
        cancel_flags[user_id] = False
        force_kill_flags[user_id] = False
        upload_mode[user_id] = mode
    with stats_lock:
        stats["current_files"] = [_remote_basename(f, is_remote=False) for f in pending_files[:3]]

    dl_semaphore = asyncio.Semaphore(job_concurrent)
    ul_semaphore = asyncio.Semaphore(job_concurrent)
    pipeline_semaphore = asyncio.Semaphore(max(2, job_concurrent * 2))
    checkpoint_lock = asyncio.Lock()

    progress_map: dict[int, str] = {}
    for idx_pre, fname_pre in enumerate(files, 1):
        if fname_pre in done_set:
            progress_map[idx_pre] = f"✅ `[{idx_pre}/{total}]` `{_remote_basename(fname_pre, is_remote=False)}` · _resumed_"

    results = {
        "success": len(done_ordered),
        "done_files": list(done_ordered),
        "failed": [],
        "deleted": 0,
        "delete_failed": [],
        "skipped": [],
    }
    board_state = _BoardState(status_msg, total, job_concurrent, progress_map, user_id)
    job_start = time.monotonic()

    async def _checkpoint():
        async with checkpoint_lock:
            with state_lock:
                done_copy = list(results["done_files"])
                failed_copy = list(results["failed"])
                skipped_copy = list(results["skipped"])
            await db_mgr.save_active_job(
                remote_path=remote_path,
                mode=mode,
                do_delete=do_delete,
                user_id=user_id,
                chat_id=status_msg.chat.id,
                status_msg_id=status_msg.id,
                all_files=files,
                done_files=done_copy,
                failed_files=failed_copy,
                skipped_files=skipped_copy,
            )

    await _checkpoint()

    try:
        tasks = []

        async def _guarded(fname, full_remote, idx):
            if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
                return
            await process_one_file(
                fname, full_remote, idx, total,
                mode, user_id,
                dl_semaphore, ul_semaphore, pipeline_semaphore,
                progress_map, status_msg, results,
                job_concurrent,
                do_delete=do_delete,
                board_state=board_state,
                size_map=size_map,
                checkpoint_cb=_checkpoint,
            )

        for idx, fname in enumerate(files, 1):
            if fname in done_set:
                continue
            _rp = remote_path
            if (
                total == 1
                and (_rp.endswith("/" + fname) or _rp.endswith(":" + fname) or _rp == fname)
            ):
                full_remote = _rp
            elif _rp.endswith(":"):
                full_remote = _rp + fname
            else:
                full_remote = _rp + "/" + fname
            tasks.append(asyncio.create_task(_guarded(fname, full_remote, idx)))

        await board_state.start()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await board_state.stop()
        kill_user_procs(user_id)
        with state_lock:
            active_sessions.discard(user_id)
            was_cancelled = cancel_flags.pop(user_id, False) or force_kill_flags.pop(user_id, False)
        with stats_lock:
            stats["current_files"] = []

    if not was_cancelled or len(results["done_files"]) + len(results["failed"]) + len(results["skipped"]) >= total:
        await db_mgr.clear_active_job()

    with state_lock:
        last_failed = list(results["failed"])
        last_job_remote = remote_path
        last_job_mode = mode
    await db_mgr.save_all()

    return results | {
        "total": total,
        "job_concurrent": job_concurrent,
        "mode_icon": mode_icon,
        "ft_badge": ft_badge,
        "do_delete": do_delete,
        "remote_path": remote_path,
        "elapsed": fmt_eta(time.monotonic() - job_start),
        "was_cancelled": was_cancelled,
    }


async def _send_summary(status_msg: Message, results: dict) -> None:
    total = results["total"]
    success = results["success"]
    failed = results["failed"]
    skipped = results.get("skipped", [])
    deleted = results["deleted"]
    delete_failed = results["delete_failed"]
    mode_icon = results["mode_icon"]
    ft_badge = results.get("ft_badge", _fmt_file_types_badge())
    do_delete = results["do_delete"]
    remote_path = results.get("remote_path", "")
    elapsed = results.get("elapsed", "—")
    was_cancelled = results.get("was_cancelled", False)

    title = "🛑 **Transfer Stopped**" if was_cancelled else "✅ **Transfer Complete**"
    summary = (
        f"{title}\n"
        f"📂 `{remote_path}`\n\n"
        f"• **Uploaded:** `{success}/{total}` files ({mode_icon})\n"
        f"• **Filter:** `{ft_badge}`\n"
        f"• **Duration:** `{elapsed}`"
    )
    if do_delete:
        summary += f"\n• **Remote Deleted:** `{deleted}/{success}`"
    if skipped:
        skip_list = "\n".join(f"• `{_remote_basename(f, is_remote=False)}`" for f in skipped[:6])
        if len(skipped) > 6:
            skip_list += f"\n_…and {len(skipped) - 6} more_"
        summary += f"\n\n⏭ **Skipped ({len(skipped)}):**\n{skip_list}"
    if failed:
        fail_list = "\n".join(f"• `{_remote_basename(f, is_remote=False)}`" for f in failed[:6])
        if len(failed) > 6:
            fail_list += f"\n_…and {len(failed) - 6} more_"
        summary += f"\n\n❌ **Failed ({len(failed)}):**\n{fail_list}"
    if delete_failed:
        df_list = "\n".join(f"• `{_remote_basename(f, is_remote=False)}`" for f in delete_failed[:4])
        summary += f"\n\n⚠️ **Delete Failed ({len(delete_failed)}):**\n{df_list}"

    rows = []
    if failed:
        rows.append([InlineKeyboardButton(f"🔁 Retry Failed ({len(failed)})", callback_data="action:retry")])
    if remote_path:
        key = _reg_path(remote_path)
        open_cb = f"dl:file:{key}" if _is_single_file_key(key) else f"dl:open:{key}"
        rows.append([InlineKeyboardButton("📂 Transfer Options", callback_data=open_cb)])
    rows.append([
        InlineKeyboardButton("🗂 Remotes", callback_data="nav:remotes:0"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
    ])

    await asyncio.sleep(0.8)
    await safe_edit(status_msg, summary, reply_markup=InlineKeyboardMarkup(rows))


async def _resume_saved_job(saved_job: dict[str, Any], notify_chat_id: int | None = None) -> None:
    try:
        if _has_active_session():
            return
        remote_path = _normalize_remote_path(saved_job.get("remote_path", ""))
        mode = saved_job.get("mode", DEFAULT_UPLOAD_MODE)
        do_delete = bool(saved_job.get("do_delete", False))
        user_id = int(saved_job.get("user_id") or OWNER_ID)
        chat_id = int(notify_chat_id or saved_job.get("chat_id") or OWNER_ID)
        all_files = list(saved_job.get("all_files") or [])
        done_files = list(saved_job.get("done_files") or [])

        _sync_client_concurrency()

        if not remote_path or not all_files:
            await db_mgr.clear_active_job()
            return

        remaining = [f for f in all_files if f not in set(done_files)]
        if not remaining:
            await db_mgr.clear_active_job()
            return

        status_msg = await app.send_message(
            chat_id,
            f"🔄 **Resuming Unfinished Job**\n"
            f"📂 `{remote_path}`\n"
            f"⏳ `{len(remaining)}` of `{len(all_files)}` file(s) remaining…",
            reply_markup=active_job_keyboard(),
        )

        with state_lock:
            size_cap_on = max_size_enabled
        size_map = await rclone_size_map(remote_path, user_id=user_id) if size_cap_on else None
        results = await _run_job(
            status_msg,
            all_files,
            remote_path,
            mode,
            user_id,
            do_delete,
            size_map=size_map,
            already_done=done_files,
        )
        await _send_summary(status_msg, results)
    except Exception as e:
        log.error(f"Auto-resume job error: {e}")


# ==============================================================================
#  STEP 12: Interactive Remote, Folder & Single-File Browser
# ==============================================================================
async def _build_remotes_browser(remote_path: str = "", page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    PAGE_SIZE = 8
    if not remote_path:
        remotes = await rclone_list_remotes()
        if not remotes:
            text = (
                "🗂 **Rclone Remotes**\n\n"
                "⚠️ No remotes found. Tap **🔧 rclone.conf** below to upload or paste your config."
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 rclone.conf Manager", callback_data="nav:rclone")],
                [InlineKeyboardButton("🏠 Home", callback_data="nav:home")],
            ])
            return text, kb

        total_pages = max(1, (len(remotes) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        chunk = remotes[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        rows = []
        for i in range(0, len(chunk), 2):
            row = []
            for r in chunk[i:i + 2]:
                k = _reg_path(r, is_single_file=False)
                row.append(InlineKeyboardButton(f"☁️ {r.rstrip(':')}", callback_data=f"br:dir:{k}:0"))
            rows.append(row)

        if total_pages > 1:
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"nav:remotes:{page - 1}"))
            nav_row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("➡️", callback_data=f"nav:remotes:{page + 1}"))
            rows.append(nav_row)

        with state_lock:
            ft_short = _fmt_file_types_short(enabled_file_types)

        rows.append([
            InlineKeyboardButton(f"🎯 Types: {ft_short}", callback_data="nav:filetypes:r_0"),
            InlineKeyboardButton("🔧 rclone.conf", callback_data="nav:rclone"),
            InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
        ])
        text = (
            f"🗂 **Select a Remote** (`{len(remotes)}` available)\n"
            f"🎯 Filter: `{_fmt_file_types_badge()}`"
        )
        return text, InlineKeyboardMarkup(rows)

    remote_path = _normalize_remote_path(remote_path)
    dirs, err_msg = await rclone_list_dirs(remote_path)
    total_pages = max(1, (len(dirs) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    chunk = dirs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    with state_lock:
        ft_short = _fmt_file_types_short(enabled_file_types)
        ft_badge = _fmt_file_types_badge(enabled_file_types)

    cur_key = _reg_path(remote_path, is_single_file=False)
    rows = [
        [
            InlineKeyboardButton(f"📥 Download Folder ({ft_short})", callback_data=f"dl:open:{cur_key}"),
            InlineKeyboardButton("📄 View Files", callback_data=f"br:files:{cur_key}:0"),
        ]
    ]

    for i in range(0, len(chunk), 2):
        row = []
        for d in chunk[i:i + 2]:
            sub_path = f"{remote_path}{d}" if remote_path.endswith(":") else f"{remote_path}/{d}"
            sub_key = _reg_path(sub_path, is_single_file=False)
            label = d if len(d) <= 18 else d[:16] + "…"
            row.append(InlineKeyboardButton(f"📁 {label}", callback_data=f"br:dir:{sub_key}:0"))
        rows.append(row)

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"br:dir:{cur_key}:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"br:dir:{cur_key}:{page + 1}"))
        rows.append(nav_row)

    if ":" in remote_path:
        r_prefix, r_rest = remote_path.split(":", 1)
        r_rest = r_rest.strip("/")
        if not r_rest:
            rows.append([
                InlineKeyboardButton("⬆️ Remotes", callback_data="nav:remotes:0"),
                InlineKeyboardButton(f"🎯 Types: {ft_short}", callback_data=f"nav:filetypes:b_{cur_key}"),
                InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
            ])
        else:
            parent_rest = r_rest.rsplit("/", 1)[0] if "/" in r_rest else ""
            parent_path = f"{r_prefix}:{parent_rest}" if parent_rest else f"{r_prefix}:"
            p_key = _reg_path(parent_path, is_single_file=False)
            rows.append([
                InlineKeyboardButton("⬆️ Back", callback_data=f"br:dir:{p_key}:0"),
                InlineKeyboardButton(f"🎯 Types: {ft_short}", callback_data=f"nav:filetypes:b_{cur_key}"),
                InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
            ])

    err_banner = f"\n⚠️ `{err_msg[:120]}`" if err_msg else ""
    text = (
        f"🗂 **Remote Folder Browser**\n"
        f"📍 `{remote_path}` (`{len(dirs)}` subfolders)\n"
        f"🎯 Filter: `{ft_badge}`{err_banner}\n\n"
        f"_Tap a subfolder, **📄 View Files** to pick individual files, or **📥 Download Folder**:_"
    )
    return text, InlineKeyboardMarkup(rows)


async def _build_files_browser(remote_path: str, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Browse individual files inside `remote_path` and trigger 1-tap single-file transfers."""
    PAGE_SIZE = 8
    remote_path = _normalize_remote_path(remote_path)
    file_entries, err_msg = await rclone_list_folder_files(remote_path)
    total_pages = max(1, (len(file_entries) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    chunk = file_entries[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    with state_lock:
        ft_short = _fmt_file_types_short(enabled_file_types)
        ft_badge = _fmt_file_types_badge(enabled_file_types)

    cur_key = _reg_path(remote_path, is_single_file=False)
    rows = [
        [
            InlineKeyboardButton(f"📥 Download All ({len(file_entries)})", callback_data=f"dl:open:{cur_key}"),
            InlineKeyboardButton("📁 View Folders", callback_data=f"br:dir:{cur_key}:0"),
        ]
    ]

    for fname, fsize in chunk:
        full_file_path = f"{remote_path}{fname}" if remote_path.endswith(":") else f"{remote_path}/{fname}"
        fkey = _reg_path(full_file_path, is_single_file=True)
        cat = _classify_file_ext(fname) or "document"
        icon = "🎬" if cat == "video" else ("🖼" if cat == "image" else "📄")
        short_name = fname if len(fname) <= 24 else fname[:21] + "…"
        rows.append([
            InlineKeyboardButton(
                f"{icon} {short_name} ({fmt_size(fsize)})",
                callback_data=f"dl:file:{fkey}",
            )
        ])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️", callback_data=f"br:files:{cur_key}:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("➡️", callback_data=f"br:files:{cur_key}:{page + 1}"))
        rows.append(nav_row)

    rows.append([
        InlineKeyboardButton("⬅️ Folders", callback_data=f"br:dir:{cur_key}:0"),
        InlineKeyboardButton(f"🎯 Types: {ft_short}", callback_data=f"nav:filetypes:f_{cur_key}"),
        InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
    ])

    err_banner = f"\n⚠️ `{err_msg[:120]}`" if err_msg else ""
    text = (
        f"📄 **Files in Folder**\n"
        f"📍 `{remote_path}` (`{len(file_entries)}` matching files)\n"
        f"🎯 Filter: `{ft_badge}`{err_banner}\n\n"
        f"_Tap any file below to transfer or manage it individually:_"
    )
    return text, InlineKeyboardMarkup(rows)


# ==============================================================================
#  STEP 13: Command Handlers, Interactive Input Handler & Callback Router
# ==============================================================================
BOT_COMMANDS = [
    BotCommand("start", "🏠 Main control dashboard"),
    BotCommand("dl", "📥 Download from rclone remote"),
    BotCommand("remotes", "🗂 Browse rclone remotes, folders & files"),
    BotCommand("filetypes", "🎯 Filter file types: videos, images, document"),
    BotCommand("concurrent", "⚡ Speed & Concurrency setup"),
    BotCommand("settings", "⚙️ Full Bot Control Panel"),
    BotCommand("setdump", "📢 Set Dump Chat / Channel ID"),
    BotCommand("setrclone", "🔧 Manage rclone.conf (upload/paste/download)"),
    BotCommand("setmode", "📤 Toggle default mode (Media / Document)"),
    BotCommand("setdelete", "🗑 Toggle remote auto-delete (on/off)"),
    BotCommand("setmaxsize", "📏 Set max file size cap (MB)"),
    BotCommand("setsplit", "✂️ Set split threshold (MB)"),
    BotCommand("setbwlimit", "🚦 Set bandwidth limit"),
    BotCommand("setflags", "🚩 Set custom rclone flags"),
    BotCommand("setrefresh", "⏱ Set progress refresh rate (2-30s)"),
    BotCommand("status", "📊 Live bot stats & cleanup tools"),
    BotCommand("db", "🗄 MongoDB status & sync"),
    BotCommand("resume", "▶️ Resume unfinished job"),
    BotCommand("queue", "🔄 View active transfer queue"),
    BotCommand("retry", "🔁 Retry failed files"),
    BotCommand("auth", "👥 Manage authorized user IDs"),
    BotCommand("logs", "📜 View recent bot logs"),
    BotCommand("cancel", "🛑 Stop job or cancel input prompt"),
    BotCommand("forcestop", "⛔ Kill active transfers immediately"),
    BotCommand("restart", "🔄 Save & restart bot"),
    BotCommand("stop", "🔌 Shut down the bot"),
]


async def register_bot_commands_auto() -> bool:
    scopes = [
        BotCommandScopeDefault(),
        BotCommandScopeAllPrivateChats(),
        BotCommandScopeAllGroupChats(),
    ]
    ok = False
    for scope in scopes:
        try:
            await app.set_bot_commands(BOT_COMMANDS, scope=scope)
            ok = True
        except Exception as e:
            log.debug(f"set_bot_commands scope {type(scope).__name__} note: {e}")
    if ok:
        log.info(f"✅ Auto-registered {len(BOT_COMMANDS)} bot commands with Telegram")
    return ok


def _validate_rclone_conf_text(text: str) -> list[str]:
    remotes = re.findall(r"^\[([^\]]+)\]", text, re.MULTILINE)
    if not remotes:
        raise ValueError("No `[remote]` sections found in config.")
    return remotes


async def _apply_new_rclone_conf(text: str, source: str = "telegram_upload") -> list[str]:
    remotes = _validate_rclone_conf_text(text)
    RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _safe_conf_exists():
        shutil.copy2(RCLONE_CONF_PATH, RCLONE_CONF_PATH.with_suffix(".conf.bak"))
    RCLONE_CONF_PATH.write_text(text, encoding="utf-8")
    os.environ["RCLONE_CONFIG"] = str(RCLONE_CONF_PATH)
    await db_mgr.save_rclone_conf(text, source=source)
    return remotes


@app.on_message(filters.command(["start", "help"]) & auth_filter)
async def cmd_start(_, msg: Message):
    awaiting_input.pop(msg.from_user.id, None)
    await msg.reply(_build_home_text(), reply_markup=home_keyboard())


@app.on_message(filters.command("status") & auth_filter)
async def cmd_status(_, msg: Message):
    await msg.reply(_build_status_text(), reply_markup=status_keyboard())


@app.on_message(filters.command("settings") & auth_filter)
async def cmd_settings(_, msg: Message):
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("filetypes") & auth_filter)
async def cmd_filetypes(_, msg: Message):
    global enabled_file_types
    if len(msg.command) >= 2:
        raw_arg = " ".join(msg.command[1:]).strip()
        parsed = _parse_file_types(raw_arg)
        with state_lock:
            enabled_file_types = parsed
        await db_mgr.save_all()
    await msg.reply(_build_filetypes_text(), reply_markup=filetypes_keyboard())


@app.on_message(filters.command("setdump") & auth_filter)
async def cmd_setdump(_, msg: Message):
    global DUMP_CHAT_ID
    if len(msg.command) >= 2:
        arg = msg.command[1].strip().lower()
        if arg in ("here", "this"):
            with state_lock:
                DUMP_CHAT_ID = msg.chat.id
            await db_mgr.save_all()
        elif arg in ("owner", "me", "dm"):
            with state_lock:
                DUMP_CHAT_ID = OWNER_ID
            await db_mgr.save_all()
        else:
            try:
                val = int(arg)
                with state_lock:
                    DUMP_CHAT_ID = val
                await db_mgr.save_all()
            except ValueError:
                await msg.reply("❌ Usage: `/setdump -1001234567890` or `/setdump here`")
                return
    await msg.reply(_build_dump_chat_text(), reply_markup=dump_chat_keyboard())


@app.on_message(filters.command("setmode") & auth_filter)
async def cmd_setmode(_, msg: Message):
    global DEFAULT_UPLOAD_MODE
    with state_lock:
        if len(msg.command) >= 2 and msg.command[1].lower().startswith("doc"):
            DEFAULT_UPLOAD_MODE = "document"
        elif len(msg.command) >= 2 and msg.command[1].lower().startswith(("vid", "med")):
            DEFAULT_UPLOAD_MODE = "video"
        else:
            DEFAULT_UPLOAD_MODE = "document" if DEFAULT_UPLOAD_MODE == "video" else "video"
    await db_mgr.save_all()
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("setsplit") & auth_filter)
async def cmd_setsplit(_, msg: Message):
    global SPLIT_SIZE
    if len(msg.command) >= 2:
        try:
            mb = int(msg.command[1].strip())
            if not 50 <= mb <= 4000:
                raise ValueError
            with state_lock:
                SPLIT_SIZE = mb * 1024 * 1024
            await db_mgr.save_all()
        except ValueError:
            await msg.reply("❌ Usage: `/setsplit <50-4000>` (MB).")
            return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("setflags") & auth_filter)
async def cmd_setflags(_, msg: Message):
    global RCLONE_FLAGS
    if len(msg.command) >= 2:
        raw = msg.text.split(None, 1)[1].strip()
        with state_lock:
            RCLONE_FLAGS = "" if raw.lower() in ("off", "none", "clear") else raw
        await db_mgr.save_all()
    await msg.reply(_build_flags_manager_text(), reply_markup=flags_manager_keyboard())


@app.on_message(filters.command("db") & auth_filter)
async def cmd_db(_, msg: Message):
    await msg.reply(await _build_db_text(), reply_markup=db_keyboard())


@app.on_message(filters.command("resume") & auth_filter)
async def cmd_resume(_, msg: Message):
    if _has_active_session():
        await msg.reply("⚠️ A job is already running.", reply_markup=active_job_keyboard())
        return
    saved = await db_mgr.get_unfinished_job()
    if not saved:
        await msg.reply("ℹ️ No unfinished job found in MongoDB.", reply_markup=home_keyboard())
        return
    asyncio.create_task(_resume_saved_job(saved, notify_chat_id=msg.chat.id))


@app.on_message(filters.command("auth") & auth_filter)
async def cmd_auth(_, msg: Message):
    if msg.from_user.id != OWNER_ID:
        await msg.reply("⛔ Only the bot owner can manage authorized users.")
        return
    if len(msg.command) >= 2:
        arg = msg.command[1].strip()
        try:
            uid = int(arg.lstrip("+-"))
            if arg.startswith("-") and uid != OWNER_ID:
                with state_lock:
                    AUTHORIZED_USERS.discard(uid)
                await db_mgr.save_all()
            else:
                with state_lock:
                    AUTHORIZED_USERS.add(uid)
                await db_mgr.save_all()
        except ValueError:
            pass
    await msg.reply(_build_users_manager_text(), reply_markup=users_manager_keyboard())


@app.on_message(filters.command("remotes") & auth_filter)
async def cmd_remotes(_, msg: Message):
    wait_msg = await msg.reply("🔍 _Loading remotes…_")
    text, kb = await _build_remotes_browser("", 0)
    await safe_edit(wait_msg, text, reply_markup=kb)


@app.on_message(filters.command("queue") & auth_filter)
async def cmd_queue(_, msg: Message):
    with stats_lock:
        cur = list(stats["current_files"])
    if not _has_active_session():
        await msg.reply("📭 **No active transfers right now.**", reply_markup=home_keyboard())
        return
    body = "\n".join(f"• `{f}`" for f in cur) if cur else "_Preparing files…_"
    await msg.reply(
        f"🔄 **Active Queue**\n\n{body}",
        reply_markup=active_job_keyboard(),
    )


@app.on_message(filters.command("logs") & auth_filter)
async def cmd_logs(_, msg: Message):
    lines = list(log_buffer)[-30:]
    body = "\n".join(lines) if lines else "No logs recorded yet."
    await msg.reply(f"📜 **Recent Logs**\n```\n{body[-3600:]}\n```", reply_markup=logs_keyboard())


@app.on_message(filters.command("cancel") & auth_filter)
async def cmd_cancel(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in awaiting_input:
        awaiting_input.pop(user_id, None)
        await msg.reply("❌ Input prompt cancelled.", reply_markup=home_keyboard())
        return
    if not _has_active_session():
        await msg.reply("ℹ️ No active job to cancel.", reply_markup=home_keyboard())
        return
    with state_lock:
        sessions = list(active_sessions)
        for uid in sessions:
            cancel_flags[uid] = True
    for uid in sessions:
        if uid in active_boards:
            active_boards[uid].mark_dirty()
    await msg.reply(
        "🛑 **Stopping after current file(s) finish.**",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⛔ Kill Immediately", callback_data="job:kill")]
        ]),
    )


@app.on_message(filters.command("forcestop") & auth_filter)
async def cmd_forcestop(_, msg: Message):
    if not _has_active_session():
        await msg.reply("ℹ️ No active job is running.", reply_markup=home_keyboard())
        return
    with state_lock:
        sessions = list(active_sessions)
        for uid in sessions:
            cancel_flags[uid] = True
            force_kill_flags[uid] = True
    total_killed = sum(kill_user_procs(uid) for uid in sessions)
    await db_mgr.clear_active_job()
    await db_mgr.save_all()
    await msg.reply(
        f"⛔ **Job Force-Stopped** (`{total_killed}` process(es) terminated).",
        reply_markup=home_keyboard(),
    )


@app.on_message(filters.command("stop") & auth_filter)
async def cmd_stop(_, msg: Message):
    with state_lock:
        sessions = list(active_sessions)
    for uid in sessions:
        kill_user_procs(uid)
    await db_mgr.save_all()
    await msg.reply("🔌 **Saved to MongoDB. Shutting down…**")
    await _graceful_restart_cleanup()
    os._exit(0)


@app.on_message(filters.command("restart") & auth_filter)
async def cmd_restart(_, msg: Message):
    with state_lock:
        sessions = list(active_sessions)
    for uid in sessions:
        kill_user_procs(uid)
    await db_mgr.save_all()
    sent = await msg.reply("🔄 **Saving state & restarting…**")
    await db_mgr.save_restart_marker(sent.chat.id, sent.id)
    await _graceful_restart_cleanup()
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("setdelete") & auth_filter)
async def cmd_setdelete(_, msg: Message):
    global delete_after_upload
    if len(msg.command) >= 2:
        arg = msg.command[1].strip().lower()
        if arg in ("on", "off"):
            with state_lock:
                delete_after_upload = (arg == "on")
            await db_mgr.save_all()
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("setmaxsize") & auth_filter)
async def cmd_setmaxsize(_, msg: Message):
    global max_size_enabled, max_size_bytes
    if len(msg.command) >= 2:
        arg = msg.command[1].strip().lower()
        if arg == "on":
            with state_lock:
                max_size_enabled = True
            await db_mgr.save_all()
        elif arg == "off":
            with state_lock:
                max_size_enabled = False
            await db_mgr.save_all()
        else:
            try:
                mb = int(arg)
                if mb <= 0:
                    raise ValueError
                with state_lock:
                    max_size_bytes = mb * 1024 * 1024
                    max_size_enabled = True
                await db_mgr.save_all()
            except ValueError:
                await msg.reply("❌ Usage: `/setmaxsize on|off` or `/setmaxsize 2000` (MB).")
                return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("concurrent") & auth_filter)
async def cmd_concurrent(_, msg: Message):
    global CONCURRENT_JOBS, UPLOAD_WORKERS, RCLONE_STREAMS
    if len(msg.command) >= 2:
        try:
            val = int(msg.command[1].strip())
            if not 1 <= val <= 5:
                raise ValueError
            with state_lock:
                CONCURRENT_JOBS = val
                if len(msg.command) >= 3:
                    uw_val = int(msg.command[2].strip())
                    if 1 <= uw_val <= 16:
                        UPLOAD_WORKERS = uw_val
                if len(msg.command) >= 4:
                    st_val = int(msg.command[3].strip())
                    if 1 <= st_val <= 32:
                        RCLONE_STREAMS = st_val
            _sync_client_concurrency()
            await db_mgr.save_all()
        except ValueError:
            await msg.reply("❌ Usage: `/concurrent <jobs 1-5> [workers 1-16] [streams 1-32]`")
            return
    await msg.reply(_build_concurrent_text(), reply_markup=concurrent_keyboard())


@app.on_message(filters.command("setbwlimit") & auth_filter)
async def cmd_setbwlimit(_, msg: Message):
    global BW_LIMIT
    if len(msg.command) >= 2:
        val = msg.command[1].strip()
        if val.lower() == "off":
            with state_lock:
                BW_LIMIT = ""
            await db_mgr.save_all()
        elif re.fullmatch(r"\d+(\.\d+)?[kKmMgG]?", val):
            with state_lock:
                BW_LIMIT = val
            await db_mgr.save_all()
        else:
            await msg.reply("❌ Invalid limit. Examples: `10M`, `50M`, or `off`.")
            return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("setrefresh") & auth_filter)
async def cmd_setrefresh(_, msg: Message):
    global BOARD_REFRESH_INTERVAL
    if len(msg.command) >= 2:
        try:
            val = float(msg.command[1].strip())
            if not 2.0 <= val <= 30.0:
                raise ValueError
            with state_lock:
                BOARD_REFRESH_INTERVAL = val
            await db_mgr.save_all()
        except ValueError:
            await msg.reply("❌ Enter a value between 2 and 30 seconds.")
            return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("dl") & auth_filter)
async def cmd_dl(_, msg: Message):
    if _has_active_session():
        await msg.reply("⚠️ **A transfer is already running.**", reply_markup=active_job_keyboard())
        return
    if len(msg.command) < 2:
        wait_msg = await msg.reply("🔍 _Opening remotes…_")
        text, kb = await _build_remotes_browser("", 0)
        await safe_edit(wait_msg, text, reply_markup=kb)
        return
    remote_path = msg.text.split(None, 1)[1].strip()
    if ":" not in remote_path:
        remote_path += ":"
    remote_path = _normalize_remote_path(remote_path)
    await msg.reply(
        _build_dl_prompt_text(remote_path),
        reply_markup=mode_keyboard(remote_path),
    )


@app.on_message(filters.command("retry") & auth_filter)
async def cmd_retry(_, msg: Message):
    user_id = msg.from_user.id
    if _has_active_session():
        await msg.reply("⚠️ A transfer is already running.", reply_markup=active_job_keyboard())
        return
    with state_lock:
        files = list(last_failed)
        remote_path = last_job_remote
        mode = last_job_mode
        do_delete = delete_after_upload
        size_cap_on = max_size_enabled
    if not files or not remote_path:
        await msg.reply("ℹ️ No failed files to retry.", reply_markup=home_keyboard())
        return
    status_msg = await msg.reply(f"🔁 **Retrying `{len(files)}` failed file(s)…**")
    size_map = await rclone_size_map(remote_path, user_id=user_id) if size_cap_on else None
    results = await _run_job(status_msg, files, remote_path, mode, user_id, do_delete, size_map=size_map)
    await _send_summary(status_msg, results)


@app.on_message(filters.command("setrclone") & auth_filter)
async def cmd_setrclone(_, msg: Message):
    text, remotes = await _build_rclone_manager_text()
    await msg.reply(text, reply_markup=rclone_manager_keyboard(remotes))


@app.on_message(filters.document & auth_filter)
async def on_document_upload(_, msg: Message):
    """Accept `rclone.conf` either when prompted or whenever a `.conf` file is sent."""
    user_id = msg.from_user.id
    doc = msg.document
    fname = (getattr(doc, "file_name", "") or "").lower()
    state = awaiting_input.get(user_id, {})

    if state.get("kind") != "rclone_conf" and not fname.endswith(".conf"):
        return

    if _has_active_session():
        await msg.reply("⚠️ Stop the active job before updating `rclone.conf`.", reply_markup=active_job_keyboard())
        return

    awaiting_input.pop(user_id, None)
    if doc.file_size and doc.file_size > 1024 * 1024:
        await msg.reply("❌ File is too large (>1 MB) for `rclone.conf`.", reply_markup=home_keyboard())
        return

    status = await msg.reply("⬇️ _Validating & saving `rclone.conf` to MongoDB…_")
    tmp_path = os.path.join(DOWNLOAD_DIR, f"rclone_upload_{user_id}_{int(time.time())}.conf")
    try:
        await msg.download(file_name=tmp_path)
        text = Path(tmp_path).read_text(encoding="utf-8", errors="strict")
        remotes = await _apply_new_rclone_conf(text, source="telegram_upload")
    except Exception as e:
        await safe_edit(status, f"❌ **Invalid `rclone.conf`:**\n`{e}`", reply_markup=home_keyboard())
        return
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    mgr_text, rem_list = await _build_rclone_manager_text()
    await safe_edit(
        status,
        f"✅ **`rclone.conf` Saved!** (`{len(remotes)}` remotes)\n\n" + mgr_text,
        reply_markup=rclone_manager_keyboard(rem_list),
    )


@app.on_message(interactive_input_filter)
async def on_interactive_input(_, msg: Message):
    """Handles interactive bot-side prompts (Dump Chat, Rclone Config paste, Custom Cap, Split, BW, Flags, Add User)."""
    global DUMP_CHAT_ID, max_size_bytes, max_size_enabled, SPLIT_SIZE, BW_LIMIT, RCLONE_FLAGS

    user_id = msg.from_user.id
    state = awaiting_input.pop(user_id, None)
    if not state:
        return

    kind = state.get("kind")
    raw_text = (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()

    if kind == "rclone_conf":
        if not raw_text:
            text, rem_list = await _build_rclone_manager_text()
            await msg.reply(
                "❌ Please send `rclone.conf` as a file or paste its `[remote]` text.\n\n" + text,
                reply_markup=rclone_manager_keyboard(rem_list),
            )
            return
        try:
            remotes = await _apply_new_rclone_conf(raw_text, source="telegram_upload")
            mgr_text, rem_list = await _build_rclone_manager_text()
            await msg.reply(
                f"✅ **`rclone.conf` Saved from Text!** (`{len(remotes)}` remotes)\n\n" + mgr_text,
                reply_markup=rclone_manager_keyboard(rem_list),
            )
        except Exception as e:
            text, rem_list = await _build_rclone_manager_text()
            await msg.reply(
                f"❌ **Invalid `rclone.conf` text:** `{e}`\n\n" + text,
                reply_markup=rclone_manager_keyboard(rem_list),
            )
        return

    if kind == "dump_chat":
        fwd_id = _extract_forwarded_chat_id(msg)
        if fwd_id is not None:
            new_id = fwd_id
        else:
            try:
                new_id = int(raw_text)
            except ValueError:
                await msg.reply(
                    "❌ Invalid Chat ID. Send a numeric ID like `-1001234567890` or forward a message from the target channel.",
                    reply_markup=dump_chat_keyboard(),
                )
                return
        with state_lock:
            DUMP_CHAT_ID = new_id
        await db_mgr.save_all()
        await msg.reply(
            f"✅ **Dump Chat updated to `{new_id}`!**\n\n" + _build_dump_chat_text(),
            reply_markup=dump_chat_keyboard(),
        )
        return

    if kind == "max_size":
        try:
            mb = int(raw_text.lower().replace("mb", "").replace("m", "").strip())
            if mb <= 0:
                raise ValueError
            with state_lock:
                max_size_bytes = mb * 1024 * 1024
                max_size_enabled = True
            await db_mgr.save_all()
            await msg.reply(
                f"✅ **Max File Size Cap set to `{mb} MB`!**\n\n" + _build_settings_text(),
                reply_markup=settings_keyboard(),
            )
        except ValueError:
            await msg.reply("❌ Send a positive number in MB (e.g. `1500`).", reply_markup=settings_keyboard())
        return

    if kind == "split_size":
        try:
            mb = int(raw_text.lower().replace("mb", "").replace("m", "").strip())
            if not 50 <= mb <= 4000:
                raise ValueError
            with state_lock:
                SPLIT_SIZE = mb * 1024 * 1024
            await db_mgr.save_all()
            await msg.reply(
                f"✅ **Split Threshold set to `{mb} MB`!**\n\n" + _build_settings_text(),
                reply_markup=settings_keyboard(),
            )
        except ValueError:
            await msg.reply("❌ Send a split threshold between `50` and `4000` MB.", reply_markup=settings_keyboard())
        return

    if kind == "bw_limit":
        if raw_text.lower() in ("off", "none", "0", "unlimited"):
            with state_lock:
                BW_LIMIT = ""
            await db_mgr.save_all()
            await msg.reply(
                "✅ **Bandwidth Limit disabled (Unlimited)!**\n\n" + _build_settings_text(),
                reply_markup=settings_keyboard(),
            )
        elif re.fullmatch(r"\d+(\.\d+)?[kKmMgG]?", raw_text):
            with state_lock:
                BW_LIMIT = raw_text
            await db_mgr.save_all()
            await msg.reply(
                f"✅ **Bandwidth Limit set to `{raw_text}`!**\n\n" + _build_settings_text(),
                reply_markup=settings_keyboard(),
            )
        else:
            await msg.reply("❌ Invalid bandwidth limit. Examples: `10M`, `50M`, or `off`.", reply_markup=settings_keyboard())
        return

    if kind == "rclone_flags":
        with state_lock:
            RCLONE_FLAGS = "" if raw_text.lower() in ("off", "none", "clear") else raw_text
        await db_mgr.save_all()
        await msg.reply(
            "✅ **Rclone Flags updated!**\n\n" + _build_flags_manager_text(),
            reply_markup=flags_manager_keyboard(),
        )
        return

    if kind == "add_user":
        if user_id != OWNER_ID:
            await msg.reply("⛔ Only the owner can add users.")
            return
        try:
            new_uid = int(raw_text.lstrip("+"))
            with state_lock:
                AUTHORIZED_USERS.add(new_uid)
            await db_mgr.save_all()
            await msg.reply(
                f"✅ **Added `{new_uid}` to Authorized Users!**\n\n" + _build_users_manager_text(),
                reply_markup=users_manager_keyboard(),
            )
        except ValueError:
            await msg.reply("❌ Send a valid numeric Telegram User ID.", reply_markup=users_manager_keyboard())
        return


# ── Unified Callback Query Router ────────────────────────────────────────────
@app.on_callback_query(auth_filter)
async def on_callback(_, cq: CallbackQuery):
    global delete_after_upload, max_size_enabled, max_size_bytes, enabled_file_types
    global CONCURRENT_JOBS, UPLOAD_WORKERS, RCLONE_STREAMS, RCLONE_BUFFER_SIZE
    global BW_LIMIT, RCLONE_FLAGS, BOARD_REFRESH_INTERVAL
    global SPLIT_SIZE, AUTO_RESUME_JOBS, DEFAULT_UPLOAD_MODE, DUMP_CHAT_ID

    data = cq.data or ""
    user_id = cq.from_user.id

    if data == "noop":
        await cq.answer()
        return

    if data == "action:close":
        awaiting_input.pop(user_id, None)
        await cq.answer("Closed")
        try:
            await cq.message.delete()
        except Exception:
            pass
        return

    if data == "action:cancel_input":
        prev_state = awaiting_input.pop(user_id, None) or {}
        cancel_nav = prev_state.get("cancel_nav", "nav:settings")
        await cq.answer("Cancelled")
        if cancel_nav == "nav:rclone":
            text, remotes = await _build_rclone_manager_text()
            await safe_edit(cq.message, text, reply_markup=rclone_manager_keyboard(remotes))
        elif cancel_nav == "nav:dump":
            await safe_edit(cq.message, _build_dump_chat_text(), reply_markup=dump_chat_keyboard())
        elif cancel_nav == "nav:flags":
            await safe_edit(cq.message, _build_flags_manager_text(), reply_markup=flags_manager_keyboard())
        elif cancel_nav == "nav:users":
            await safe_edit(cq.message, _build_users_manager_text(), reply_markup=users_manager_keyboard())
        else:
            await safe_edit(cq.message, _build_settings_text(), reply_markup=settings_keyboard())
        return

    if data == "action:clear_logs":
        log_buffer.clear()
        await cq.answer("Logs cleared")
        await safe_edit(
            cq.message,
            "📜 **Recent Logs**\n```\nLog buffer cleared.\n```",
            reply_markup=logs_keyboard(),
        )
        return

    if data == "action:clean_temp":
        if _has_active_session():
            await cq.answer("⚠️ Cannot clean temp dir while a transfer is active!", show_alert=True)
            return
        removed = 0
        try:
            for item in Path(DOWNLOAD_DIR).iterdir():
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                    removed += 1
                elif item.is_file():
                    item.unlink(missing_ok=True)
                    removed += 1
        except Exception:
            pass
        await cq.answer(f"🧹 Cleaned {removed} temp item(s)!", show_alert=True)
        await safe_edit(cq.message, _build_status_text(), reply_markup=status_keyboard())
        return

    if data == "action:reset_stats":
        with stats_lock:
            stats["total_done"] = 0
            stats["total_failed"] = 0
            stats["total_skipped"] = 0
            stats["total_bytes"] = 0
            stats["last_file"] = None
            stats["last_error"] = None
            stats["floodwait_count"] = 0
            stats["floodwait_last_secs"] = 0
            stats["floodwait_last_time"] = None
        with state_lock:
            last_failed.clear()
        await db_mgr.save_all()
        await cq.answer("🔄 Transfer statistics reset!", show_alert=True)
        await safe_edit(cq.message, _build_status_text(), reply_markup=status_keyboard())
        return

    if data == "action:restart_bot":
        with state_lock:
            sessions = list(active_sessions)
        for uid in sessions:
            kill_user_procs(uid)
        await db_mgr.save_all()
        await cq.answer("🔄 Restarting…", show_alert=False)
        await safe_edit(cq.message, "🔄 **Saving state & restarting…**")
        await db_mgr.save_restart_marker(cq.message.chat.id, cq.message.id)
        await _graceful_restart_cleanup()
        os.execv(sys.executable, [sys.executable] + sys.argv)
        return

    if data == "action:stop_bot":
        with state_lock:
            sessions = list(active_sessions)
        for uid in sessions:
            kill_user_procs(uid)
        await db_mgr.save_all()
        await cq.answer("🔌 Shutting down…", show_alert=True)
        await safe_edit(cq.message, "🔌 **State saved to MongoDB. Bot shut down.**")
        await _graceful_restart_cleanup()
        os._exit(0)
        return

    if data == "action:resume_job":
        if _has_active_session():
            await cq.answer("⚠️ A job is already running!", show_alert=True)
            return
        saved = await db_mgr.get_unfinished_job()
        if not saved:
            await cq.answer("ℹ️ No unfinished job in MongoDB.", show_alert=True)
            return
        await cq.answer("▶️ Resuming job…")
        asyncio.create_task(_resume_saved_job(saved, notify_chat_id=cq.message.chat.id))
        return

    # ── Interactive Prompt Triggers (`prompt:...`) ──
    if data.startswith("prompt:"):
        pkind = data.split(":", 1)[1]
        cancel_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="action:cancel_input")]
        ])
        if pkind == "rclone_conf":
            if _has_active_session():
                await cq.answer("⚠️ Stop the active job first!", show_alert=True)
                return
            awaiting_input[user_id] = {"kind": "rclone_conf", "cancel_nav": "nav:rclone"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "🔧 **Upload or Paste `rclone.conf`**\n\n"
                "• Send your `rclone.conf` file as a document, **OR**\n"
                "• Paste your `[remote]` configuration text directly in chat.",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "dump_chat":
            awaiting_input[user_id] = {"kind": "dump_chat", "cancel_nav": "nav:dump"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "📢 **Set Dump Chat / Channel**\n\n"
                "• Send the numeric Chat ID (e.g. `-1001234567890`), **OR**\n"
                "• Forward any message from your target channel/group to me now.",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "max_size":
            awaiting_input[user_id] = {"kind": "max_size", "cancel_nav": "nav:settings"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "📏 **Set Custom Max File Size Cap**\n\n"
                "Send the maximum file size in **MB** (e.g. `1500` or `3500`):",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "split_size":
            awaiting_input[user_id] = {"kind": "split_size", "cancel_nav": "nav:settings"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "✂️ **Set Custom Split Threshold**\n\n"
                "Send the split threshold in **MB** between `50` and `4000` (e.g. `1950`):",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "bw_limit":
            awaiting_input[user_id] = {"kind": "bw_limit", "cancel_nav": "nav:settings"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "🚦 **Set Custom Bandwidth Limit**\n\n"
                "Send a limit like `10M`, `50M`, `512K`, or `off` for unlimited:",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "rclone_flags":
            awaiting_input[user_id] = {"kind": "rclone_flags", "cancel_nav": "nav:flags"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "🚩 **Set Custom Rclone Flags**\n\n"
                "Send the extra flags to pass to `rclone` (e.g. `--drive-chunk-size 64M --fast-list`) or `clear`:",
                reply_markup=cancel_kb,
            )
            return
        if pkind == "add_user":
            if user_id != OWNER_ID:
                await cq.answer("⛔ Only the bot owner can manage users.", show_alert=True)
                return
            awaiting_input[user_id] = {"kind": "add_user", "cancel_nav": "nav:users"}
            await cq.answer()
            await safe_edit(
                cq.message,
                "👥 **Add Authorized User**\n\n"
                "Send the numeric Telegram User ID to authorize:",
                reply_markup=cancel_kb,
            )
            return

    # ── Navigation callbacks (`nav:...`) ──
    if data == "nav:home":
        awaiting_input.pop(user_id, None)
        await cq.answer()
        await safe_edit(cq.message, _build_home_text(), reply_markup=home_keyboard())
        return

    if data == "nav:status":
        await cq.answer()
        await safe_edit(cq.message, _build_status_text(), reply_markup=status_keyboard())
        return

    if data == "nav:settings":
        awaiting_input.pop(user_id, None)
        await cq.answer()
        await safe_edit(cq.message, _build_settings_text(), reply_markup=settings_keyboard())
        return

    if data == "nav:dump":
        awaiting_input.pop(user_id, None)
        await cq.answer()
        await safe_edit(cq.message, _build_dump_chat_text(), reply_markup=dump_chat_keyboard())
        return

    if data in ("nav:rclone", "nav:setrclone"):
        awaiting_input.pop(user_id, None)
        await cq.answer()
        text, remotes = await _build_rclone_manager_text()
        await safe_edit(cq.message, text, reply_markup=rclone_manager_keyboard(remotes))
        return

    if data == "nav:users":
        if user_id != OWNER_ID:
            await cq.answer("⛔ Owner only.", show_alert=True)
            return
        awaiting_input.pop(user_id, None)
        await cq.answer()
        await safe_edit(cq.message, _build_users_manager_text(), reply_markup=users_manager_keyboard())
        return

    if data == "nav:flags":
        awaiting_input.pop(user_id, None)
        await cq.answer()
        await safe_edit(cq.message, _build_flags_manager_text(), reply_markup=flags_manager_keyboard())
        return

    if data.startswith("nav:filetypes"):
        parts = data.split(":", 2)
        ret_tag = parts[2] if len(parts) >= 3 else ""
        await cq.answer()
        await safe_edit(cq.message, _build_filetypes_text(), reply_markup=filetypes_keyboard(ret_tag))
        return

    if data.startswith("nav:concurrent"):
        parts = data.split(":", 2)
        ret_tag = parts[2] if len(parts) >= 3 else ""
        await cq.answer()
        await safe_edit(cq.message, _build_concurrent_text(), reply_markup=concurrent_keyboard(ret_tag))
        return

    if data == "nav:db":
        await cq.answer()
        await safe_edit(cq.message, await _build_db_text(), reply_markup=db_keyboard())
        return

    if data == "nav:queue":
        if not _has_active_session():
            await cq.answer("No active job right now.")
            await safe_edit(cq.message, _build_home_text(), reply_markup=home_keyboard())
            return
        await cq.answer()
        board = active_boards.get(user_id) or next(iter(active_boards.values()), None)
        if board and board.running:
            await board.force_refresh()
        else:
            with stats_lock:
                cur = list(stats["current_files"])
            body = "\n".join(f"• `{f}`" for f in cur) if cur else "_Preparing files…_"
            await safe_edit(
                cq.message,
                f"🔄 **Active Queue**\n\n{body}",
                reply_markup=active_job_keyboard(),
            )
        return

    if data == "nav:logs":
        await cq.answer()
        lines = list(log_buffer)[-30:]
        body = "\n".join(lines) if lines else "No logs recorded yet."
        await safe_edit(
            cq.message,
            f"📜 **Recent Logs**\n```\n{body[-3600:]}\n```",
            reply_markup=logs_keyboard(),
        )
        return

    if data.startswith("nav:remotes:"):
        try:
            page = int(data.split(":")[2])
        except (IndexError, ValueError):
            page = 0
        await cq.answer()
        text, kb = await _build_remotes_browser("", page)
        await safe_edit(cq.message, text, reply_markup=kb)
        return

    # ── Dump Chat callbacks (`dump:...`) ──
    if data.startswith("dump:"):
        sub = data.split(":", 1)[1]
        if sub == "here":
            with state_lock:
                DUMP_CHAT_ID = cq.message.chat.id
            await db_mgr.save_all()
            await cq.answer(f"📢 Dump Chat set to {DUMP_CHAT_ID}!")
        elif sub == "owner":
            with state_lock:
                DUMP_CHAT_ID = OWNER_ID
            await db_mgr.save_all()
            await cq.answer(f"👤 Dump Chat set to Owner ({OWNER_ID})!")
        elif sub == "test":
            with state_lock:
                target = DUMP_CHAT_ID
            try:
                await app.send_message(target, "✅ **Rclone Bot Test:** Upload destination is working!")
                await cq.answer(f"✅ Test message sent to {target}!", show_alert=True)
            except Exception as e:
                await cq.answer(f"❌ Failed to send to {target}: {str(e)[:120]}", show_alert=True)
            return
        await safe_edit(cq.message, _build_dump_chat_text(), reply_markup=dump_chat_keyboard())
        return

    # ── Rclone Config Manager callbacks (`rc:...`) ──
    if data.startswith("rc:"):
        parts = data.split(":")
        sub = parts[1]
        if sub == "download":
            if not _safe_conf_exists():
                await cq.answer("⚠️ No rclone.conf file on disk.", show_alert=True)
                return
            await cq.answer("📥 Sending rclone.conf…")
            try:
                await app.send_document(
                    cq.message.chat.id,
                    str(RCLONE_CONF_PATH),
                    caption="🔧 **Active `rclone.conf`**",
                )
            except Exception as e:
                await cq.answer(f"❌ Error: {e}", show_alert=True)
            return
        if sub == "del" and len(parts) >= 3:
            if _has_active_session():
                await cq.answer("⚠️ Stop the active job before editing remotes!", show_alert=True)
                return
            rname = _get_path(parts[2])
            if not rname or not _safe_conf_exists():
                await cq.answer("⚠️ Remote not found.", show_alert=True)
                return
            try:
                cur_text = RCLONE_CONF_PATH.read_text(encoding="utf-8")
                new_text = _remove_rclone_remote_section(cur_text, rname)
                RCLONE_CONF_PATH.write_text(new_text, encoding="utf-8")
                await db_mgr.save_rclone_conf(new_text, source="bot_edit")
                await cq.answer(f"🗑 Deleted remote [{rname}]")
            except Exception as e:
                await cq.answer(f"❌ Error: {e}", show_alert=True)
            text, remotes = await _build_rclone_manager_text()
            await safe_edit(cq.message, text, reply_markup=rclone_manager_keyboard(remotes))
            return

    # ── Users Manager callbacks (`usr:...`) ──
    if data.startswith("usr:del:"):
        if user_id != OWNER_ID:
            await cq.answer("⛔ Owner only.", show_alert=True)
            return
        try:
            rem_uid = int(data.split(":")[2])
            if rem_uid != OWNER_ID:
                with state_lock:
                    AUTHORIZED_USERS.discard(rem_uid)
                await db_mgr.save_all()
                await cq.answer(f"🗑 Removed {rem_uid}")
        except ValueError:
            pass
        await safe_edit(cq.message, _build_users_manager_text(), reply_markup=users_manager_keyboard())
        return

    # ── Rclone Flags Manager callbacks (`flg:...`) ──
    if data.startswith("flg:"):
        sub = data.split(":", 2)[1]
        if sub == "clear":
            with state_lock:
                RCLONE_FLAGS = ""
            await db_mgr.save_all()
            await cq.answer("🧹 Cleared all flags")
        elif sub == "toggle":
            flag_str = data.split(":", 2)[2]
            tokens = _rclone_extra_flags()
            if flag_str in tokens:
                tokens = [t for t in tokens if t != flag_str]
            else:
                tokens.append(flag_str)
            with state_lock:
                RCLONE_FLAGS = " ".join(tokens)
            await db_mgr.save_all()
            await cq.answer(f"🚩 Flags: {RCLONE_FLAGS or 'None'}")
        await safe_edit(cq.message, _build_flags_manager_text(), reply_markup=flags_manager_keyboard())
        return

    # ── File Types Filter Panel callbacks (`ft:...`) ──
    if data.startswith("ft:"):
        parts = data.split(":", 3)
        sub = parts[1]
        if sub in ("only", "preset", "toggle"):
            target = parts[2]
            ret_tag = parts[3] if len(parts) >= 4 else ""
        else:
            target = ""
            ret_tag = parts[2] if len(parts) >= 3 else ""

        if sub == "only" and target in ALL_FILE_TYPE_KEYS:
            with state_lock:
                enabled_file_types = {target}
                badge = _fmt_file_types_badge(enabled_file_types)
            await db_mgr.save_all()
            await cq.answer(f"🎯 Filter: {badge}")
        elif sub == "preset":
            with state_lock:
                if target == "media":
                    enabled_file_types = {"video", "image"}
                else:
                    enabled_file_types = {"video", "image", "document"}
                badge = _fmt_file_types_badge(enabled_file_types)
            await db_mgr.save_all()
            await cq.answer(f"🎯 Filter: {badge}")
        elif sub == "toggle" and target in ALL_FILE_TYPE_KEYS:
            with state_lock:
                if target in enabled_file_types:
                    if len(enabled_file_types) <= 1:
                        await cq.answer("⚠️ Keep at least one type enabled!", show_alert=True)
                        return
                    enabled_file_types.discard(target)
                else:
                    enabled_file_types.add(target)
                badge = _fmt_file_types_badge(enabled_file_types)
            await db_mgr.save_all()
            await cq.answer(f"🎯 Filter: {badge}")

        await safe_edit(cq.message, _build_filetypes_text(), reply_markup=filetypes_keyboard(ret_tag))
        return

    # ── Speed & Concurrency Setup callbacks (`conc:...`) ──
    if data.startswith("conc:"):
        parts = data.split(":", 3)
        sub = parts[1]
        if sub in ("preset", "jobs", "upw", "streams", "buf"):
            ret_tag = parts[3] if len(parts) >= 4 else ""
        else:
            ret_tag = parts[2] if len(parts) >= 3 else ""

        if sub == "preset":
            pname = parts[2]
            with state_lock:
                if pname == "safe":
                    CONCURRENT_JOBS = 1
                    UPLOAD_WORKERS = 2
                    RCLONE_STREAMS = 4
                    RCLONE_BUFFER_SIZE = "32M"
                    lbl = "🛡 Safe (1x · 2w · 4s)"
                elif pname == "fast":
                    CONCURRENT_JOBS = 2
                    UPLOAD_WORKERS = 4
                    RCLONE_STREAMS = 8
                    RCLONE_BUFFER_SIZE = "64M"
                    lbl = "⚡ Fast (2x · 4w · 8s)"
                else:
                    CONCURRENT_JOBS = 3
                    UPLOAD_WORKERS = 6
                    RCLONE_STREAMS = 16
                    RCLONE_BUFFER_SIZE = "128M"
                    lbl = "🚀 Turbo (3x · 6w · 16s)"
            _sync_client_concurrency()
            await db_mgr.save_all()
            await cq.answer(f"Applied {lbl}")
        elif sub == "jobs":
            val = max(1, min(5, int(parts[2])))
            with state_lock:
                CONCURRENT_JOBS = val
            _sync_client_concurrency()
            await db_mgr.save_all()
            await cq.answer(f"⚡ Jobs: {val}x")
        elif sub == "upw":
            val = max(1, min(16, int(parts[2])))
            with state_lock:
                UPLOAD_WORKERS = val
            _sync_client_concurrency()
            await db_mgr.save_all()
            await cq.answer(f"🚀 Upload Workers: {val}w")
        elif sub == "cycle_streams":
            with state_lock:
                idx_s = _STREAM_OPTIONS.index(RCLONE_STREAMS) if RCLONE_STREAMS in _STREAM_OPTIONS else 1
                RCLONE_STREAMS = _STREAM_OPTIONS[(idx_s + 1) % len(_STREAM_OPTIONS)]
                s_val = RCLONE_STREAMS
            await db_mgr.save_all()
            await cq.answer(f"🌊 Streams: {s_val}s")
        elif sub == "cycle_buf":
            with state_lock:
                idx_b = _BUFFER_OPTIONS.index(RCLONE_BUFFER_SIZE) if RCLONE_BUFFER_SIZE in _BUFFER_OPTIONS else 1
                RCLONE_BUFFER_SIZE = _BUFFER_OPTIONS[(idx_b + 1) % len(_BUFFER_OPTIONS)]
                b_val = RCLONE_BUFFER_SIZE
            await db_mgr.save_all()
            await cq.answer(f"📦 Buffer: {b_val}")

        await safe_edit(cq.message, _build_concurrent_text(), reply_markup=concurrent_keyboard(ret_tag))
        return

    # ── MongoDB Panel callbacks (`db:...`) ──
    if data.startswith("db:"):
        sub = data.split(":", 1)[1]
        if sub == "save":
            await db_mgr.save_all()
            if _safe_conf_exists():
                try:
                    await db_mgr.save_rclone_conf(RCLONE_CONF_PATH.read_text(encoding="utf-8"), source="manual_sync")
                except Exception:
                    pass
            await cq.answer("✅ Saved to MongoDB!")
        elif sub == "reload":
            await db_mgr.load_all()
            await cq.answer("📥 Reloaded from MongoDB!")
        elif sub == "clear_job":
            await db_mgr.clear_active_job()
            await cq.answer("🗑 Cleared saved job.")
        await safe_edit(cq.message, await _build_db_text(), reply_markup=db_keyboard())
        return

    # ── Folder & File Browser callbacks (`br:...`) ──
    if data.startswith("br:dir:"):
        _, _, key, page_s = data.split(":", 3)
        rpath = _get_path(key)
        if not rpath:
            await cq.answer("Session expired — reloading remotes")
            text, kb = await _build_remotes_browser("", 0)
            await safe_edit(cq.message, text, reply_markup=kb)
            return
        try:
            pg = int(page_s)
        except ValueError:
            pg = 0
        await cq.answer()
        text, kb = await _build_remotes_browser(rpath, pg)
        await safe_edit(cq.message, text, reply_markup=kb)
        return

    if data.startswith("br:files:"):
        _, _, key, page_s = data.split(":", 3)
        rpath = _get_path(key)
        if not rpath:
            await cq.answer("Session expired — reloading remotes")
            text, kb = await _build_remotes_browser("", 0)
            await safe_edit(cq.message, text, reply_markup=kb)
            return
        try:
            pg = int(page_s)
        except ValueError:
            pg = 0
        await cq.answer("Loading files…")
        text, kb = await _build_files_browser(rpath, pg)
        await safe_edit(cq.message, text, reply_markup=kb)
        return

    # ── Settings Panel callbacks (`cfg:...`) ──
    if data.startswith("cfg:"):
        action = data.split(":", 1)[1]
        if action == "toggle_del":
            with state_lock:
                delete_after_upload = not delete_after_upload
                state_str = "ON 🟢" if delete_after_upload else "OFF 🔴"
            await db_mgr.save_all()
            await cq.answer(f"🗑 Auto-Delete: {state_str}")
        elif action == "toggle_resume":
            with state_lock:
                AUTO_RESUME_JOBS = not AUTO_RESUME_JOBS
                state_str = "ON 🟢" if AUTO_RESUME_JOBS else "OFF 🔴"
            await db_mgr.save_all()
            await cq.answer(f"▶️ Auto-Resume: {state_str}")
        elif action == "toggle_mode":
            with state_lock:
                DEFAULT_UPLOAD_MODE = "document" if DEFAULT_UPLOAD_MODE == "video" else "video"
                m_str = "🎬 Media" if DEFAULT_UPLOAD_MODE == "video" else "📄 Document"
            await db_mgr.save_all()
            await cq.answer(f"📤 Default Mode: {m_str}")
        elif action == "toggle_size":
            with state_lock:
                max_size_enabled = not max_size_enabled
                state_str = "ON 🟢" if max_size_enabled else "OFF 🔴"
            await db_mgr.save_all()
            await cq.answer(f"📏 Size Cap: {state_str}")
        elif action == "cycle_size":
            with state_lock:
                cur_mb = max_size_bytes // (1024 * 1024)
                nxt = _SIZE_PRESETS_MB[(_SIZE_PRESETS_MB.index(cur_mb) + 1) % len(_SIZE_PRESETS_MB)] if cur_mb in _SIZE_PRESETS_MB else 2000
                max_size_bytes = nxt * 1024 * 1024
                max_size_enabled = True
            await db_mgr.save_all()
            await cq.answer(f"📐 Size Cap: {nxt} MB")
        elif action == "cycle_split":
            with state_lock:
                cur_s_mb = SPLIT_SIZE // (1024 * 1024)
                nxt_s = _SPLIT_PRESETS_MB[(_SPLIT_PRESETS_MB.index(cur_s_mb) + 1) % len(_SPLIT_PRESETS_MB)] if cur_s_mb in _SPLIT_PRESETS_MB else 2000
                SPLIT_SIZE = nxt_s * 1024 * 1024
            await db_mgr.save_all()
            await cq.answer(f"✂️ Split Threshold: {nxt_s} MB")
        elif action == "cycle_bw":
            with state_lock:
                idx_bw = _BW_PRESETS.index(BW_LIMIT) if BW_LIMIT in _BW_PRESETS else 0
                BW_LIMIT = _BW_PRESETS[(idx_bw + 1) % len(_BW_PRESETS)]
                bw_lbl = BW_LIMIT or "Unlimited"
            await db_mgr.save_all()
            await cq.answer(f"🚦 BW Limit: {bw_lbl}")
        elif action == "cycle_refresh":
            with state_lock:
                idx_r = _REFRESH_PRESETS.index(BOARD_REFRESH_INTERVAL) if BOARD_REFRESH_INTERVAL in _REFRESH_PRESETS else 1
                BOARD_REFRESH_INTERVAL = _REFRESH_PRESETS[(idx_r + 1) % len(_REFRESH_PRESETS)]
                r_val = int(BOARD_REFRESH_INTERVAL)
            await db_mgr.save_all()
            await cq.answer(f"⏱ Refresh: {r_val}s")

        await safe_edit(cq.message, _build_settings_text(), reply_markup=settings_keyboard())
        return

    # ── Live Job Control callbacks (`job:...`) ──
    if data == "job:refresh":
        board = active_boards.get(user_id) or next(iter(active_boards.values()), None)
        if board and board.running:
            await cq.answer("Refreshed")
            await board.force_refresh()
        else:
            await cq.answer("No active transfer.")
        return

    if data == "job:cancel":
        if not _has_active_session():
            await cq.answer("No active job.")
            return
        with state_lock:
            sessions = list(active_sessions)
            for uid in sessions:
                cancel_flags[uid] = True
        for uid in sessions:
            if uid in active_boards:
                active_boards[uid].mark_dirty()
        await cq.answer("🛑 Stopping after current file(s)…", show_alert=True)
        return

    if data == "job:kill":
        if not _has_active_session():
            await cq.answer("No active job.")
            return
        with state_lock:
            sessions = list(active_sessions)
            for uid in sessions:
                cancel_flags[uid] = True
                force_kill_flags[uid] = True
        killed = sum(kill_user_procs(uid) for uid in sessions)
        await db_mgr.clear_active_job()
        await cq.answer(f"⛔ Killed {killed} process(es).", show_alert=True)
        return

    # ── Retry callback (`action:retry`) ──
    if data == "action:retry":
        if _has_active_session():
            await cq.answer("⚠️ A job is already running!", show_alert=True)
            return
        with state_lock:
            files = list(last_failed)
            remote_path = last_job_remote
            mode = last_job_mode
            do_delete = delete_after_upload
            size_cap_on = max_size_enabled
        if not files or not remote_path:
            await cq.answer("ℹ️ No failed files to retry.", show_alert=True)
            return
        await cq.answer(f"🔁 Retrying {len(files)} file(s)…")
        await safe_edit(cq.message, f"🔁 **Retrying `{len(files)}` failed file(s)…**")
        size_map = await rclone_size_map(remote_path, user_id=user_id) if size_cap_on else None
        results = await _run_job(cq.message, files, remote_path, mode, user_id, do_delete, size_map=size_map)
        await _send_summary(cq.message, results)
        return

    # ── Transfer Launchpad callbacks (`dl:...`) ──
    if data.startswith("dl:"):
        parts = data.split(":")
        sub = parts[1]
        key = parts[-1]
        remote_path = _get_path(key)
        if not remote_path:
            await cq.answer("⚠️ Session expired — please open /remotes again.", show_alert=True)
            return
        is_single = _is_single_file_key(key)

        if sub == "open":
            await cq.answer()
            await safe_edit(
                cq.message,
                _build_dl_prompt_text(remote_path, is_single_file=False),
                reply_markup=mode_keyboard(remote_path, is_single_file=False),
            )
            return

        if sub == "file":
            await cq.answer()
            await safe_edit(
                cq.message,
                _build_dl_prompt_text(remote_path, is_single_file=True),
                reply_markup=mode_keyboard(remote_path, is_single_file=True),
            )
            return

        if sub == "rmfile":
            if _has_active_session():
                await cq.answer("⚠️ Wait for active transfer to finish.", show_alert=True)
                return
            try:
                await rclone_delete(remote_path, user_id=user_id)
                await cq.answer("🗑 File deleted from remote!", show_alert=True)
                if ":" in remote_path:
                    r_prefix, r_rest = remote_path.split(":", 1)
                    parent_rest = r_rest.strip("/").rsplit("/", 1)[0] if "/" in r_rest.strip("/") else ""
                    parent_path = f"{r_prefix}:{parent_rest}" if parent_rest else f"{r_prefix}:"
                    text, kb = await _build_files_browser(parent_path, 0)
                    await safe_edit(cq.message, text, reply_markup=kb)
            except Exception as e:
                await cq.answer(f"❌ Delete failed: {str(e)[:120]}", show_alert=True)
            return

        if sub == "ft":
            ft_choice = parts[2]
            with state_lock:
                if ft_choice == "all":
                    enabled_file_types = {"video", "image", "document"}
                elif ft_choice in ALL_FILE_TYPE_KEYS:
                    enabled_file_types = {ft_choice}
                badge = _fmt_file_types_badge(enabled_file_types)
            await db_mgr.save_all()
            await cq.answer(f"🎯 Filter: {badge}")
            await safe_edit(
                cq.message,
                _build_dl_prompt_text(remote_path, is_single_file=is_single),
                reply_markup=mode_keyboard(remote_path, is_single_file=is_single),
            )
            return

        if sub == "del":
            with state_lock:
                delete_after_upload = not delete_after_upload
                st = "ON" if delete_after_upload else "OFF"
            await db_mgr.save_all()
            await cq.answer(f"🗑 Auto-Delete: {st}")
            await safe_edit(
                cq.message,
                _build_dl_prompt_text(remote_path, is_single_file=is_single),
                reply_markup=mode_keyboard(remote_path, is_single_file=is_single),
            )
            return

        if sub == "size":
            with state_lock:
                max_size_enabled = not max_size_enabled
                st = "ON" if max_size_enabled else "OFF"
            await db_mgr.save_all()
            await cq.answer(f"📏 Size Cap: {st}")
            await safe_edit(
                cq.message,
                _build_dl_prompt_text(remote_path, is_single_file=is_single),
                reply_markup=mode_keyboard(remote_path, is_single_file=is_single),
            )
            return

        if sub == "go":
            chosen = parts[2]
            mode = "video" if chosen == "video" else "document"
            if _has_active_session():
                await cq.answer("⚠️ A job is already running!", show_alert=True)
                return
            with state_lock:
                do_delete = delete_after_upload
                size_cap_on = max_size_enabled
                job_types = set(enabled_file_types)
                ft_badge = _fmt_file_types_badge(job_types)

            await cq.answer("Starting transfer…")
            await safe_edit(
                cq.message,
                f"🔍 **Scanning Remote**\n"
                f"📂 `{remote_path}`\n"
                + ("" if is_single else f"🎯 Filter: `{ft_badge}`"),
            )

            try:
                files = await rclone_list(
                    remote_path,
                    user_id=user_id,
                    allowed_types=job_types,
                    skip_type_filter=is_single,
                )
            except Exception as e:
                with stats_lock:
                    stats["last_error"] = str(e)[:200]
                await safe_edit(
                    cq.message,
                    f"❌ **Failed to list files:**\n`{e}`",
                    reply_markup=home_keyboard(),
                )
                return

            if not files:
                await safe_edit(
                    cq.message,
                    f"📭 **No matching files found**\n"
                    f"📂 `{remote_path}`\n"
                    f"🎯 Filter: `{ft_badge}`\n\n"
                    f"_Switch the file type filter below and try again:_",
                    reply_markup=mode_keyboard(remote_path, is_single_file=is_single),
                )
                return

            size_map: dict[str, int] | None = None
            if size_cap_on:
                size_map = await rclone_size_map(remote_path, user_id=user_id)

            results = await _run_job(
                cq.message, files, remote_path, mode, user_id, do_delete, size_map=size_map
            )
            await _send_summary(cq.message, results)
            return


# ==============================================================================
#  STEP 14: Startup Lifecycle, Auto Command Registration & Auto-Resume Hook
# ==============================================================================
async def _async_main():
    if psutil is not None:
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

    threading.Thread(target=_run_health, daemon=True).start()
    log.info(f"Health check server listening on port {HEALTH_PORT} ({MTPROTO_ENGINE})")

    await db_mgr.connect()
    _sync_client_concurrency()

    await app.start()
    await register_bot_commands_auto()

    me = await app.get_me()
    log.info(
        f"🚀 Bot @{me.username} (ID: {me.id}) online via {MTPROTO_ENGINE} "
        f"[CONCURRENT_JOBS={CONCURRENT_JOBS}x, UPLOAD_WORKERS={UPLOAD_WORKERS}w, "
        f"STREAMS={RCLONE_STREAMS}s, BUF={RCLONE_BUFFER_SIZE}, DUMP={DUMP_CHAT_ID}, "
        f"TYPES={','.join(sorted(enabled_file_types))}]!"
    )

    marker = await db_mgr.pop_restart_marker()
    if marker and marker.get("chat_id") and marker.get("message_id"):
        try:
            await app.edit_message_text(
                int(marker["chat_id"]),
                int(marker["message_id"]),
                "✅ **Bot Restarted!**\n\n" + _build_home_text(),
                reply_markup=home_keyboard(),
            )
        except Exception as e:
            log.debug(f"Could not edit restart marker message: {e}")

    unfinished = await db_mgr.get_unfinished_job()
    if unfinished:
        with state_lock:
            should_auto_resume = AUTO_RESUME_JOBS
        if should_auto_resume:
            log.info(f"Found unfinished job in MongoDB for {unfinished.get('remote_path')} — auto-resuming!")
            asyncio.create_task(_resume_saved_job(unfinished))
        else:
            try:
                target_chat = int(unfinished.get("chat_id") or OWNER_ID)
                await app.send_message(
                    target_chat,
                    f"⏸ **Unfinished Job Found**\n"
                    f"📂 `{unfinished.get('remote_path')}`",
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("▶️ Resume", callback_data="action:resume_job"),
                            InlineKeyboardButton("🗑 Discard", callback_data="db:clear_job"),
                        ]
                    ]),
                )
            except Exception:
                pass

    await idle()
    await db_mgr.save_all()
    await _graceful_restart_cleanup()


if __name__ == "__main__":
    app.run(_async_main())