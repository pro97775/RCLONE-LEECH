# ==============================================================================
#  Rclone ➔ Telegram Transfer Bot (Async + MongoDB Persistence + Auto-Resume)
# ==============================================================================
#  Architecture Overview:
#    1. Bootstrap & Event Loop (uvloop)
#    2. Logging & In-Memory Ring Buffer
#    3. Environment & Static Configuration
#    4. MongoDB Persistence Engine (Settings, rclone.conf, Stats, Job Auto-Resume)
#    5. Runtime State & Short-ID Callback Registry
#    6. Health Check HTTP Server
#    7. UI Formatters & Visual Components
#    8. Inline Keyboard Builders & UI Screens
#    9. Non-Blocking Async Subprocess Engine (rclone / ffmpeg / ffprobe)
#   10. Live Transfer Progress Board
#   11. Transfer Pipeline, Checkpointing & Auto-Resume Runner
#   12. Telegram Command Handlers
#   13. Unified Callback Query Router
#   14. Startup Lifecycle, Auto Command Registration & Auto-Resume Hook
# ==============================================================================

import os
import sys
import asyncio
import logging
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

# ── 1. Bootstrap & Event Loop (uvloop) ────────────────────────────────────────
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

from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats,
)
from pyrogram.errors import FloodWait, MessageNotModified


# ── 2. Logging & In-Memory Ring Buffer ────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rclone_bot")
log_buffer: collections.deque[str] = collections.deque(maxlen=150)


class _RingBufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_buffer.append(self.format(record))


_bh = _RingBufferHandler()
_bh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(_bh)


# ── 3. Environment & Static Configuration ─────────────────────────────────────
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

RCLONE_CONF_PATH = Path(
    os.environ.get("RCLONE_CONFIG", "/root/.config/rclone/rclone.conf")
)


def _write_initial_rclone_conf() -> str | None:
    """Write rclone.conf from env/URL if provided and return its text content."""
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

    if _target.exists():
        os.environ["RCLONE_CONFIG"] = str(_target)
        log.info(f"rclone.conf found at {_target}")
        try:
            return _target.read_text(encoding="utf-8")
        except Exception:
            return None

    log.info("No local rclone.conf yet — will check MongoDB on startup or await /setrclone.")
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
_DUMP = _req("DUMP_CHAT_ID")

if _MISSING:
    for k in _MISSING:
        log.critical(f"Missing required variable: {k}")
    raise SystemExit(1)

API_ID = int(_API_ID)
OWNER_ID = int(_OWNER)
DUMP_CHAT_ID = int(_DUMP)

AUTHORIZED_USERS: set[int] = {OWNER_ID}
for _uid in os.environ.get("AUTHORIZED_USERS", "").replace(";", ",").split(","):
    _uid = _uid.strip()
    if _uid:
        try:
            AUTHORIZED_USERS.add(int(_uid))
        except ValueError:
            log.warning(f"Ignoring invalid AUTHORIZED_USERS entry: {_uid}")

# Optional runtime parameters (can be overridden by MongoDB persisted config)
MONGO_URI = (
    os.environ.get("MONGO_URI")
    or os.environ.get("DATABASE_URL")
    or os.environ.get("MONGODB_URI")
    or ""
).strip()
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "rclone_tg_bot").strip()

DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", "/tmp/rclone_dl").strip()
RCLONE_FLAGS = os.environ.get("RCLONE_FLAGS", "").strip()
RCLONE_BUFFER_SIZE = os.environ.get("RCLONE_BUFFER_SIZE", "32M").strip()
RCLONE_STREAMS = os.environ.get("RCLONE_STREAMS", "4").strip()
SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", str(2000 * 1024 * 1024)))
HEALTH_PORT = int(os.environ.get("PORT") or os.environ.get("HEALTH_PORT") or 8080)
CONCURRENT_JOBS = max(1, min(5, int(os.environ.get("CONCURRENT_JOBS", 1))))
BW_LIMIT = os.environ.get("BW_LIMIT", "").strip()
STATS_FILE = os.environ.get("STATS_FILE", "/tmp/rclone_bot_stats.json").strip()
BOARD_REFRESH_INTERVAL = max(2.0, min(30.0, float(os.environ.get("BOARD_REFRESH_INTERVAL", "6.0"))))
AUTO_RESUME_JOBS = os.environ.get("AUTO_RESUME_JOBS", "true").strip().lower() not in ("0", "false", "no", "off")
DEFAULT_UPLOAD_MODE = os.environ.get("DEFAULT_UPLOAD_MODE", "video").strip().lower()
if DEFAULT_UPLOAD_MODE not in ("video", "document"):
    DEFAULT_UPLOAD_MODE = "video"

cores = os.environ.get("CPU_CORES", "0").strip()
threads = int(os.environ.get("CPU_THREADS", os.cpu_count() or 1))

Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

VIDEO_EXTS = {
    "mp4", "mkv", "avi", "mov", "webm", "flv", "ts", "m4v", "3gp", "3g2",
    "wmv", "mpg", "mpeg", "m2ts", "mts", "vob", "divx", "xvid", "rm", "rmvb",
    "ogv", "f4v", "asf", "amv", "m2v", "mp2", "mpe", "mpv", "qt", "yuv", "roq", "nsv",
}
AUDIO_EXTS = {
    "mp3", "flac", "aac", "ogg", "opus", "wav", "m4a", "wma", "aiff", "ape",
    "dsd", "mka", "mpc", "ac3", "dts", "ra", "ram", "tta", "wv",
    "mid", "midi", "amr", "3ga", "awb", "gsm", "spx", "caf", "au",
}
IMAGE_EXTS = {"jpg", "jpeg", "png", "webp", "gif", "bmp", "tiff", "tif", "heic", "heif"}


# ── 5. Runtime State & Short-ID Callback Registry ─────────────────────────────
BOT_START_TIME = time.time()
active_sessions: set[int] = set()
cancel_flags: dict[int, bool] = {}
force_kill_flags: dict[int, bool] = {}
upload_mode: dict[int, str] = {}
awaiting_rclone_conf: set[int] = set()
active_procs: dict[int, set[asyncio.subprocess.Process]] = collections.defaultdict(set)
active_boards: dict[int, "_BoardState"] = {}

delete_after_upload: bool = False
max_size_enabled: bool = False
max_size_bytes: int = 2000 * 1024 * 1024
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

# Short-ID registry so callback_data never exceeds Telegram's 64-byte limit
_path_registry: collections.OrderedDict[str, str] = collections.OrderedDict()
_MAX_PATH_REGISTRY = 512


def _reg_path(remote_path: str) -> str:
    key = hashlib.blake2s(remote_path.encode("utf-8"), digest_size=5).hexdigest()
    with state_lock:
        _path_registry[key] = remote_path
        _path_registry.move_to_end(key)
        while len(_path_registry) > _MAX_PATH_REGISTRY:
            _path_registry.popitem(last=False)
    return key


def _get_path(key: str) -> str | None:
    with state_lock:
        return _path_registry.get(key)


def is_authorized(user_id: int) -> bool:
    with state_lock:
        return user_id in AUTHORIZED_USERS


auth_filter = filters.create(lambda _, __, m: bool(m.from_user and is_authorized(m.from_user.id)))


# ── 4. MongoDB Persistence Engine (with Local JSON Fallback) ──────────────────
class DatabaseManager:
    """
    Async MongoDB persistence layer using Motor.
    Automatically saves & restores:
      • Bot settings & authorized users (`config` document)
      • Full `rclone.conf` file content (`rclone_conf` document)
      • Lifetime transfer statistics (`stats` document)
      • In-progress job state (`active_job` document) for auto-resume on restart
      • Restart marker (`restart_marker` document)
    Falls back transparently to local JSON file if MONGO_URI is not configured.
    """

    def __init__(self, uri: str, db_name: str, fallback_file: str):
        self.uri = uri
        self.db_name = db_name
        self.fallback_file = Path(fallback_file)
        self.client: Any = None
        self.db: Any = None
        self.connected: bool = False
        self.last_sync_ts: float = 0.0
        self.last_error: str | None = None

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
            # If rclone.conf was provided via env/URL at boot, ensure it's backed up in MongoDB
            if _INITIAL_RCLONE_TEXT:
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
            return {
                "delete_after_upload": delete_after_upload,
                "max_size_enabled": max_size_enabled,
                "max_size_bytes": max_size_bytes,
                "concurrent_jobs": CONCURRENT_JOBS,
                "bw_limit": BW_LIMIT,
                "board_refresh_interval": BOARD_REFRESH_INTERVAL,
                "split_size": SPLIT_SIZE,
                "default_upload_mode": DEFAULT_UPLOAD_MODE,
                "auto_resume_jobs": AUTO_RESUME_JOBS,
                "authorized_users": sorted(AUTHORIZED_USERS),
                "DUMP_CHAT_ID": DUMP_CHAT_ID,
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
        global delete_after_upload, max_size_enabled, max_size_bytes
        global CONCURRENT_JOBS, BW_LIMIT, BOARD_REFRESH_INTERVAL
        global SPLIT_SIZE, DEFAULT_UPLOAD_MODE, AUTO_RESUME_JOBS, DUMP_CHAT_ID
        with state_lock:
            if isinstance(cfg.get("delete_after_upload"), bool):
                delete_after_upload = cfg["delete_after_upload"]
            if isinstance(cfg.get("max_size_enabled"), bool):
                max_size_enabled = cfg["max_size_enabled"]
            if isinstance(cfg.get("max_size_bytes"), int) and cfg["max_size_bytes"] > 0:
                max_size_bytes = cfg["max_size_bytes"]
            if isinstance(cfg.get("concurrent_jobs"), int):
                CONCURRENT_JOBS = max(1, min(5, cfg["concurrent_jobs"]))
            if isinstance(cfg.get("bw_limit"), str):
                BW_LIMIT = cfg["bw_limit"]
            if isinstance(cfg.get("board_refresh_interval"), (int, float)):
                BOARD_REFRESH_INTERVAL = max(2.0, min(30.0, float(cfg["board_refresh_interval"])))
            if isinstance(cfg.get("split_size"), int) and cfg["split_size"] > 50_000_000:
                SPLIT_SIZE = cfg["split_size"]
            if cfg.get("default_upload_mode") in ("video", "document"):
                DEFAULT_UPLOAD_MODE = cfg["default_upload_mode"]
            if isinstance(cfg.get("auto_resume_jobs"), bool):
                AUTO_RESUME_JOBS = cfg["auto_resume_jobs"]
            if isinstance(cfg.get("authorized_users"), list):
                for uid in cfg["authorized_users"]:
                    if isinstance(uid, int):
                        AUTHORIZED_USERS.add(uid)
            if isinstance(cfg.get("DUMP_CHAT_ID"), int):
                DUMP_CHAT_ID = cfg["DUMP_CHAT_ID"]

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
        """Load settings, stats, and rclone.conf from MongoDB (or local fallback)."""
        # First load local fallback if present
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
                self._apply_settings(cfg_doc)
                log.info("Restored bot configuration from MongoDB")

            stats_doc = await self.db.bot_state.find_one({"_id": "stats"})
            if stats_doc:
                self._apply_stats(stats_doc)
                log.info("Restored transfer statistics from MongoDB")

            # Restore rclone.conf from MongoDB if not already written or if stored in DB
            rc_doc = await self.db.bot_state.find_one({"_id": "rclone_conf"})
            if rc_doc and isinstance(rc_doc.get("content"), str) and rc_doc["content"].strip():
                if not RCLONE_CONF_PATH.exists() or rc_doc.get("source") == "telegram_upload":
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
        """Save both settings and stats to local disk and MongoDB."""
        cfg = self._snapshot_settings()
        st = self._snapshot_stats()

        # Always write local JSON cache
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

    def schedule_save(self) -> None:
        """Fire-and-forget save from sync or async contexts."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.save_all())
        except RuntimeError:
            pass

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
            self.last_sync_ts = time.time()
            log.info(f"Saved rclone.conf ({len(remotes)} remotes) to MongoDB")
        except Exception as e:
            self.last_error = str(e)[:120]
            log.warning(f"Failed to save rclone.conf to MongoDB: {e}")

    # ── Job Checkpointing & Auto-Resume ──
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
        payload = {
            "active": True,
            "remote_path": remote_path,
            "mode": mode,
            "do_delete": do_delete,
            "user_id": user_id,
            "chat_id": chat_id,
            "status_msg_id": status_msg_id,
            "all_files": all_files,
            "done_files": done_files,
            "failed_files": failed_files,
            "skipped_files": skipped_files,
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

    # ── Restart Marker ──
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

app = Client(
    "rclone_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=4,
    sleep_threshold=60,
    max_concurrent_transmissions=max(2, CONCURRENT_JOBS),
)


# ── 6. Health Check HTTP Server ───────────────────────────────────────────────
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
            body = (
                f"status=ok\nuptime={h}h {m}m {s}s\n"
                f"mongodb={'connected' if db_mgr.connected else 'local_fallback'}\n"
                f"active_jobs={len(active_sessions)}\n"
                f"concurrent_limit={CONCURRENT_JOBS}\n"
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
    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
        server.allow_reuse_address = True
        server.serve_forever()
    except Exception as e:
        log.error(f"Health server failed on port {HEALTH_PORT}: {e}")


# ── 7. UI Formatters & Visual Components ──────────────────────────────────────
def fmt_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def fmt_uptime() -> str:
    u = int(time.time() - BOT_START_TIME)
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


def progress_bar(done: float, total: float, width: int = 12) -> str:
    if total <= 0:
        return "▱" * width
    pct = max(0.0, min(1.0, done / total))
    filled = max(0, min(width, round(width * pct)))
    return "▰" * filled + "▱" * (width - filled)


def _server_resources() -> str:
    lines = []
    if psutil is not None:
        try:
            cpu = psutil.cpu_percent(interval=None)
            logical_cores = psutil.cpu_count(logical=True) or 1
            vm = psutil.virtual_memory()
            cpu_bar = progress_bar(cpu, 100, 8)
            ram_bar = progress_bar(vm.percent, 100, 8)
            lines.append(f"├ 🖥 **CPU:** `{cpu_bar}` `{cpu:.0f}%` ({logical_cores} vCPU)")
            lines.append(
                f"├ 🧠 **RAM:** `{ram_bar}` `{vm.percent:.0f}%` (`{fmt_size(vm.used)}/{fmt_size(vm.total)}`)"
            )
        except Exception:
            pass
    else:
        lines.append("├ 🖥 **CPU/RAM:** `psutil unavailable`")

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        pct = du.used * 100 / du.total if du.total else 0
        disk_bar = progress_bar(pct, 100, 8)
        lines.append(f"╰ 💾 **Disk:** `{disk_bar}` `{pct:.0f}%` (`{fmt_size(du.free)}` free)")
    except Exception:
        pass
    return "\n".join(lines)


def make_progress_line(
    label: str,
    done_bytes: int,
    total_bytes: int,
    speed_str: str = "",
    eta_str: str = "",
    width: int = 12,
    elapsed_str: str = "",
    part_info: str = "",
) -> str:
    if total_bytes > 0:
        pct = min(100, int(done_bytes * 100 / total_bytes))
        bar = progress_bar(done_bytes, total_bytes, width)
        size_str = f"{fmt_size(done_bytes)} / {fmt_size(total_bytes)}"
    else:
        pct = 0
        bar = "▱" * width
        size_str = fmt_size(done_bytes) if done_bytes else "calculating…"

    lines = [
        f"╭─ {label}",
        f"├ `{bar}` **{pct}%**",
    ]
    if part_info:
        lines.append(f"├ 🧩 **Part:** `{part_info}`")
    lines.append(f"├ 📦 **Size:** `{size_str}`")

    metrics = []
    if speed_str:
        metrics.append(f"⚡ `{speed_str}`")
    if eta_str and eta_str != "—":
        metrics.append(f"⏳ `{eta_str}`")
    if elapsed_str:
        metrics.append(f"⏱ `{elapsed_str}`")
    if metrics:
        lines.append("╰ " + " · ".join(metrics))
    else:
        lines[-1] = lines[-1].replace("├", "╰", 1)
    return "\n".join(lines)


# ── 8. Inline Keyboard Builders & UI Screens ─────────────────────────────────
def home_keyboard() -> InlineKeyboardMarkup:
    with state_lock:
        failed_cnt = len(last_failed)
    rows = [
        [
            InlineKeyboardButton("🗂 Browse Remotes", callback_data="nav:remotes:0"),
            InlineKeyboardButton("📊 Live Status", callback_data="nav:status"),
        ],
        [
            InlineKeyboardButton("⚙️ Settings Panel", callback_data="nav:settings"),
            InlineKeyboardButton("🗄 MongoDB & Config", callback_data="nav:db"),
        ],
        [
            InlineKeyboardButton("🔄 Active Queue", callback_data="nav:queue"),
            InlineKeyboardButton("📜 View Logs", callback_data="nav:logs"),
        ],
        [
            InlineKeyboardButton("🔧 Upload rclone.conf", callback_data="nav:setrclone"),
            InlineKeyboardButton("🔄 Restart Bot", callback_data="action:restart_bot"),
        ],
    ]
    if failed_cnt > 0:
        rows.append([
            InlineKeyboardButton(f"🔁 Retry Failed ({failed_cnt})", callback_data="action:retry")
        ])
    return InlineKeyboardMarkup(rows)


def status_keyboard() -> InlineKeyboardMarkup:
    with state_lock:
        failed_cnt = len(last_failed)
    rows = [
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="nav:status"),
            InlineKeyboardButton("⚙️ Settings", callback_data="nav:settings"),
        ],
        [
            InlineKeyboardButton("🗂 Remotes", callback_data="nav:remotes:0"),
            InlineKeyboardButton("🗄 MongoDB", callback_data="nav:db"),
        ],
    ]
    if active_sessions:
        rows.append([
            InlineKeyboardButton("🛑 Graceful Stop", callback_data="job:cancel"),
            InlineKeyboardButton("⛔ Kill Job Now", callback_data="job:kill"),
        ])
    elif failed_cnt > 0:
        rows.append([
            InlineKeyboardButton(f"🔁 Retry Failed ({failed_cnt})", callback_data="action:retry")
        ])
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home")])
    return InlineKeyboardMarkup(rows)


def settings_keyboard() -> InlineKeyboardMarkup:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        bw = BW_LIMIT or "Unlimited"
        conc = CONCURRENT_JOBS
        refr = int(BOARD_REFRESH_INTERVAL)
        auto_res = AUTO_RESUME_JOBS
        def_mode = DEFAULT_UPLOAD_MODE

    del_lbl = "🗑 Auto-Delete: ON 🟢" if del_on else "🗑 Auto-Delete: OFF 🔴"
    size_lbl = f"📏 Size Cap: ON 🟢 ({cap_mb}M)" if size_on else f"📏 Size Cap: OFF 🔴 ({cap_mb}M)"
    res_lbl = "▶️ Auto-Resume: ON 🟢" if auto_res else "▶️ Auto-Resume: OFF 🔴"
    mode_lbl = "🎬 Default: Video" if def_mode == "video" else "📄 Default: Document"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(del_lbl, callback_data="cfg:toggle_del"),
            InlineKeyboardButton(res_lbl, callback_data="cfg:toggle_resume"),
        ],
        [
            InlineKeyboardButton(size_lbl, callback_data="cfg:toggle_size"),
            InlineKeyboardButton("📐 Cap Preset ▸", callback_data="cfg:cycle_size"),
        ],
        [
            InlineKeyboardButton(f"⚡ Parallel: {conc}x ▸", callback_data="cfg:cycle_conc"),
            InlineKeyboardButton(f"🚦 BW: {bw} ▸", callback_data="cfg:cycle_bw"),
        ],
        [
            InlineKeyboardButton(f"⏱ Refresh: {refr}s ▸", callback_data="cfg:cycle_refresh"),
            InlineKeyboardButton(mode_lbl, callback_data="cfg:toggle_defmode"),
        ],
        [
            InlineKeyboardButton("💾 Sync to MongoDB", callback_data="cfg:save_db"),
            InlineKeyboardButton("📲 Sync Bot Cmds", callback_data="cfg:sync_cmds"),
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="nav:status"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home"),
        ],
    ])


def db_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💾 Force Save to DB", callback_data="db:save"),
            InlineKeyboardButton("📥 Reload from DB", callback_data="db:reload"),
        ],
        [
            InlineKeyboardButton("▶️ Resume Saved Job", callback_data="action:resume_job"),
            InlineKeyboardButton("🗑 Clear Saved Job", callback_data="db:clear_job"),
        ],
        [
            InlineKeyboardButton("🔧 Upload rclone.conf", callback_data="nav:setrclone"),
            InlineKeyboardButton("⚙️ Settings", callback_data="nav:settings"),
        ],
        [
            InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home"),
        ],
    ])


def logs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="nav:logs"),
            InlineKeyboardButton("🧹 Clear Buffer", callback_data="action:clear_logs"),
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="nav:status"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home"),
        ],
    ])


def active_job_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Refresh Board", callback_data="job:refresh"),
            InlineKeyboardButton("🛑 Graceful Stop", callback_data="job:cancel"),
        ],
        [
            InlineKeyboardButton("⛔ Kill Immediately", callback_data="job:kill"),
        ],
    ])


def mode_keyboard(remote_path: str) -> InlineKeyboardMarkup:
    key = _reg_path(remote_path)
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        bw = BW_LIMIT or "OFF"

    del_btn = "🗑 Delete: ON 🟢" if del_on else "🗑 Delete: OFF 🔴"
    size_btn = f"📏 Cap: {cap_mb}M 🟢" if size_on else "📏 Cap: OFF 🔴"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🎬 Start as Video", callback_data=f"dl:go:video:{key}"),
            InlineKeyboardButton("📄 Start as Document", callback_data=f"dl:go:doc:{key}"),
        ],
        [
            InlineKeyboardButton(del_btn, callback_data=f"dl:del:{key}"),
            InlineKeyboardButton(size_btn, callback_data=f"dl:size:{key}"),
        ],
        [
            InlineKeyboardButton(f"⚡ Parallel: {conc}x ▸", callback_data=f"dl:conc:{key}"),
            InlineKeyboardButton(f"🚦 BW: {bw} ▸", callback_data=f"dl:bw:{key}"),
        ],
        [
            InlineKeyboardButton("🗂 Browse Remotes", callback_data="nav:remotes:0"),
            InlineKeyboardButton("✖️ Close", callback_data="action:close"),
        ],
    ])


def _build_home_text() -> str:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        bw = BW_LIMIT or "Unlimited"
        conc = CONCURRENT_JOBS
        auto_res = AUTO_RESUME_JOBS
    with stats_lock:
        done = stats["total_done"]
        tbytes = stats["total_bytes"]

    state_badge = "🟡 **BUSY**" if active_sessions else "🟢 **READY**"
    db_badge = "🟢 MongoDB Synced" if db_mgr.connected else "🟡 Local JSON"
    return (
        f"☁️ **Rclone ➔ Telegram Transfer Bot** · {state_badge}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"╭─ **Configuration & Storage**\n"
        f"├ 🗄 **Storage:** `{db_badge}`\n"
        f"├ ▶️ **Auto-Resume on Restart:** `{'ON 🟢' if auto_res else 'OFF 🔴'}`\n"
        f"├ ⚡ **Concurrency:** `{conc}x` · 🚦 **BW:** `{bw}`\n"
        f"├ 🗑 **Auto-Delete:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"├ 📏 **Size Cap:** `{f'ON 🟢 ({cap_mb} MB)' if size_on else 'OFF 🔴'}`\n"
        f"╰ 📦 **Lifetime Uploaded:** `{done} files` (`{fmt_size(tbytes)}`)\n\n"
        f"💡 **Quick Actions:**\n"
        f"• Tap **🗂 Browse Remotes** to pick a cloud folder visually\n"
        f"• Use **⚙️ Settings Panel** to tune transfers (auto-saved to DB)\n"
        f"• Interrupted transfers auto-resume when the bot restarts"
    )


def _build_status_text() -> str:
    with stats_lock:
        snap = dict(stats)
    with state_lock:
        del_on = delete_after_upload
        bw = BW_LIMIT or "Unlimited"
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        refr = BOARD_REFRESH_INTERVAL
        auto_res = AUTO_RESUME_JOBS

    badge = "🟡 **TRANSFERRING**" if active_sessions else "🟢 **ONLINE & IDLE**"
    db_state = f"🟢 Connected (`{MONGO_DB_NAME}`)" if db_mgr.connected else "🟡 Local Fallback"
    cur = snap["current_files"]

    text = (
        f"📊 **System & Transfer Dashboard**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"╭─ **Bot Telemetry**\n"
        f"├ 📡 **State:** {badge}\n"
        f"├ 🗄 **Database:** {db_state}\n"
        f"├ ⏱ **Uptime:** `{fmt_uptime()}`\n"
        f"├ 🔄 **Active Jobs:** `{len(active_sessions)}` (Max `{conc}x` parallel)\n"
        f"├ ✅ **Completed:** `{snap['total_done']}`\n"
        f"├ ❌ **Failed:** `{snap['total_failed']}` · ⏭ **Skipped:** `{snap.get('total_skipped', 0)}`\n"
        f"╰ 📦 **Uploaded:** `{fmt_size(snap['total_bytes'])}`\n\n"
        f"╭─ **Active Settings**\n"
        f"├ ▶️ **Auto-Resume:** `{'ON 🟢' if auto_res else 'OFF 🔴'}` · 🔄 **Refresh:** `{refr:.0f}s`\n"
        f"├ 🚦 **BW Limit:** `{bw}` · 🗑 **Auto-Delete:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"╰ 📏 **Size Cap:** `{f'ON 🟢 ({cap_mb} MB)' if size_on else 'OFF 🔴'}`\n"
    )
    if cur:
        text += f"\n🔄 **In Progress:** `{'`, `'.join(cur[:3])}`\n"
    if snap["last_file"]:
        text += f"📄 **Last File:** `{snap['last_file'].split('/')[-1]}`\n"
    if snap["last_error"]:
        text += f"⚠️ **Last Error:** `{snap['last_error'][:90]}`\n"

    fw_count = snap["floodwait_count"]
    if fw_count > 0:
        fw_secs = snap["floodwait_last_secs"]
        fw_mins, fw_rem = divmod(fw_secs, 60)
        fw_fmt = f"{fw_mins}m {fw_rem}s" if fw_mins > 0 else f"{fw_secs}s"
        text += f"⏳ **FloodWaits:** `{fw_count}x` (last `{fw_fmt}`)\n"

    res = _server_resources()
    if res:
        text += f"\n╭─ **Host Resources**\n{res}\n"
    return text


def _build_settings_text() -> str:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        bw = BW_LIMIT or "Unlimited"
        conc = CONCURRENT_JOBS
        refr = BOARD_REFRESH_INTERVAL
        auto_res = AUTO_RESUME_JOBS
        def_mode = DEFAULT_UPLOAD_MODE

    db_note = "🟢 Synced to MongoDB" if db_mgr.connected else "🟡 Saved to Local Disk"
    return (
        f"⚙️ **Interactive Control Panel** · `{db_note}`\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Every change below is automatically persisted to MongoDB and restored on restart:\n\n"
        f"╭─ **Persisted Settings**\n"
        f"├ 🗑 **Auto-Delete Remote:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"├ ▶️ **Auto-Resume on Restart:** `{'ON 🟢' if auto_res else 'OFF 🔴'}`\n"
        f"├ 📏 **Max File Size Cap:** `{'ON 🟢' if size_on else 'OFF 🔴'}` (`{cap_mb} MB`)\n"
        f"├ ⚡ **Parallel Transfers:** `{conc}x`\n"
        f"├ 🚦 **Download BW Limit:** `{bw}`\n"
        f"├ ⏱ **Board Refresh Rate:** `{refr:.0f}s`\n"
        f"├ 🎬 **Default Mode:** `{def_mode.capitalize()}`\n"
        f"╰ ✂️ **Auto-Split Threshold:** `{fmt_size(SPLIT_SIZE)}`"
    )


async def _build_db_text() -> str:
    unfinished = await db_mgr.get_unfinished_job()
    rc_exists = RCLONE_CONF_PATH.exists()
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
        f"🗄 **MongoDB & Config Persistence**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"╭─ **Database Status**\n"
        f"├ 📡 **Backend:** `{'MongoDB (' + MONGO_DB_NAME + ')' if db_mgr.connected else 'Local JSON Fallback'}`\n"
        f"├ 🔗 **URI Configured:** `{'Yes ✅' if bool(MONGO_URI) else 'No ❌ (set MONGO_URI)'}`\n"
        f"├ ⏱ **Last Sync:** `{sync_ago}`\n"
        f"├ 🔧 **rclone.conf:** `{'Loaded (' + str(remotes_cnt) + ' remotes)' if rc_exists else 'Missing'}`\n"
        f"├ 👥 **Authorized Users:** `{len(AUTHORIZED_USERS)}`\n"
        f"╰ 🔄 **Saved Checkpoint:** {job_str}\n"
        + (f"\n⚠️ **Last DB Warning:** `{db_mgr.last_error}`\n" if db_mgr.last_error else "")
    )


def _build_dl_prompt_text(remote_path: str) -> str:
    with state_lock:
        del_on = delete_after_upload
        size_on = max_size_enabled
        cap_mb = max_size_bytes // (1024 * 1024)
        conc = CONCURRENT_JOBS
        bw = BW_LIMIT or "Unlimited"
    return (
        f"🚀 **Ready to Transfer**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 **Target:** `{remote_path}`\n\n"
        f"╭─ **Job Options**\n"
        f"├ 🗑 **Auto-Delete:** `{'ON 🟢' if del_on else 'OFF 🔴'}`\n"
        f"├ 📏 **Size Cap:** `{f'ON 🟢 ({cap_mb} MB)' if size_on else 'OFF 🔴'}`\n"
        f"├ ⚡ **Concurrency:** `{conc}x`\n"
        f"╰ 🚦 **BW Limit:** `{bw}`\n\n"
        f"Select **🎬 Start as Video** or **📄 Start as Document** below:"
    )


# ── 9. Non-Blocking Async Subprocess Engine ──────────────────────────────────
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


async def safe_edit(
    msg: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    key = (msg.chat.id, msg.id)
    markup_sig = str(reply_markup) if reply_markup else ""
    if _last_edits.get(key) == (text, markup_sig):
        return
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
        _last_edits[key] = (text, markup_sig)
        while len(_last_edits) > 200:
            _last_edits.popitem(last=False)
    except MessageNotModified:
        _last_edits[key] = (text, markup_sig)
    except FloodWait as e:
        await asyncio.sleep(e.value + 3)
        try:
            await msg.edit_text(text, reply_markup=reply_markup)
            _last_edits[key] = (text, markup_sig)
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
        active_procs[user_id].add(proc)


def _unregister_proc(user_id: int, proc: asyncio.subprocess.Process) -> None:
    if user_id and user_id in active_procs:
        active_procs[user_id].discard(proc)


def kill_user_procs(user_id: int) -> int:
    procs = list(active_procs.get(user_id, ()))
    killed = 0
    for p in procs:
        if p.returncode is None:
            try:
                p.kill()
                killed += 1
            except Exception:
                pass
    active_procs.pop(user_id, None)
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


async def rclone_list_dirs(remote_path: str) -> list[str]:
    cmd = ["rclone", "lsf", "--dirs-only", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    code, out, _ = await _run_cmd_async(cmd, timeout=30.0)
    if code != 0:
        return []
    return [d.strip().rstrip("/") for d in out.splitlines() if d.strip()]


async def rclone_list(remote_path: str, user_id: int = 0) -> list[str]:
    cmd = ["rclone", "lsf", "--files-only", "-R", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    code, out, err = await _run_cmd_async(cmd, user_id=user_id, timeout=180.0)
    if code != 0:
        raise RuntimeError(err.strip() or "rclone lsf failed")
    allowed = VIDEO_EXTS | AUDIO_EXTS
    return [
        f.strip().lstrip("/") for f in out.splitlines()
        if f.strip() and f.strip().rsplit(".", 1)[-1].lower() in allowed
    ]


async def rclone_size_map(remote_path: str, user_id: int = 0) -> dict[str, int]:
    cmd = ["rclone", "lsjson", "-R", "--files-only", "--no-modtime", "--no-mimetype", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
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
            rel = (entry.get("Path") or "").lstrip("/")
            size = entry.get("Size", -1)
            if rel and isinstance(size, (int, float)) and size >= 0:
                res[rel] = int(size)
        return res
    except Exception:
        return {}


async def rclone_size(remote_path: str, user_id: int = 0) -> int:
    if ":" in remote_path:
        remote_part, _, path_part = remote_path.partition(":")
        remote_part += ":"
    else:
        remote_part, path_part = "", remote_path
    parent = path_part.rsplit("/", 1)[0] if "/" in path_part else ""
    target_name = path_part.rsplit("/", 1)[-1]
    parent_remote = remote_part + parent if parent else remote_part

    cmd = ["rclone", "lsjson", "--no-modtime", "--no-mimetype", parent_remote]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    try:
        code, out, _ = await _run_cmd_async(cmd, user_id=user_id, timeout=45.0)
        if code != 0:
            return -1
        data = json.loads(out)
        if not isinstance(data, list):
            return -1
        for entry in data:
            if entry.get("Name") == target_name and not entry.get("IsDir"):
                size = entry.get("Size", -1)
                return int(size) if isinstance(size, (int, float)) and size >= 0 else -1
    except Exception:
        pass
    return -1


async def rclone_download(
    remote_path: str,
    dest_dir: str,
    progress_callback=None,
    user_id: int = 0,
) -> str:
    _filename = remote_path.split("/")[-1]
    if ":" in _filename:
        _filename = _filename.split(":", 1)[-1]
    local_dest = os.path.join(dest_dir, _filename)

    cmd = [
        "rclone", "copyto",
        "--stats=2s",
        "--stats-one-line",
        "--stats-log-level", "NOTICE",
        "--buffer-size", RCLONE_BUFFER_SIZE,
        "--multi-thread-streams", RCLONE_STREAMS,
        "--transfers", "1",
        "--checkers", "2",
    ] + _bwlimit_args() + [
        remote_path, local_dest,
    ]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stderr=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        env=_rclone_env(),
    )
    _register_proc(user_id, proc)

    last_line = ""
    last_progress_call = 0.0
    _ansi_re = re.compile(r"\x1b\[[0-9;]*[A-Za-z]|\r")

    try:
        assert proc.stderr is not None
        while True:
            if force_kill_flags.get(user_id):
                proc.kill()
                raise RuntimeError("Job aborted by user")
            raw_bytes = await proc.stderr.readline()
            if not raw_bytes:
                break
            line = _ansi_re.sub("", raw_bytes.decode("utf-8", errors="replace")).strip()
            if not line:
                continue
            last_line = line
            parsed = _parse_rclone_line(line)
            if not parsed:
                log.info(f"rclone: {line}")
            elif progress_callback:
                now = time.monotonic()
                if last_progress_call == 0.0 or now - last_progress_call >= 2.5:
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
    cmd = ["rclone", "deletefile", remote_path]
    if RCLONE_FLAGS:
        cmd += RCLONE_FLAGS.split()
    code, _, err = await _run_cmd_async(cmd, user_id=user_id, timeout=60.0)
    if code != 0:
        raise RuntimeError(err.strip() or "rclone deletefile failed")
    log.info(f"🗑 Deleted from remote: {remote_path}")


async def split_file_async(local_path: str, split_dir: str, user_id: int = 0) -> list[str]:
    size = os.path.getsize(local_path)
    if size <= SPLIT_SIZE:
        return [local_path]

    split_dir_path = Path(split_dir)
    split_dir_path.mkdir(parents=True, exist_ok=True)
    filename = os.path.basename(local_path)
    stem, extension = os.path.splitext(filename)

    code, out, _ = await _run_cmd_async(
        [
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-print_format", "json", "-show_format", local_path,
        ],
        user_id=user_id,
        timeout=30.0,
    )
    duration = 0
    if code == 0:
        try:
            fields = json.loads(out).get("format", {})
            duration = round(float(fields.get("duration", 0)))
        except Exception:
            duration = 0

    if duration == 0:
        return [local_path]

    split_size = SPLIT_SIZE - 3_000_000
    start_time = 0
    i = 1
    parts = (size // SPLIT_SIZE) + 1
    multi_streams = True
    out_paths: list[str] = []

    while i <= parts or start_time < duration - 4:
        if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
            return out_paths or [local_path]
        out_path = str(split_dir_path / f"{stem}.part{i:03}{extension}")

        prefix = ["taskset", "-c", f"{cores}"] if cores != "0" else []
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

        if rc != 0:
            try:
                os.remove(out_path)
            except OSError:
                pass
            if multi_streams:
                multi_streams = False
                continue
            return [local_path]

        out_size = os.path.getsize(out_path)
        if out_size > SPLIT_SIZE:
            new_split_size = split_size - (out_size - SPLIT_SIZE) - 5_000_000
            if new_split_size < 50_000_000:
                os.remove(out_path)
                return [local_path]
            split_size = new_split_size
            os.remove(out_path)
            continue

        rc2, out2, _ = await _run_cmd_async(
            [
                "ffprobe", "-hide_banner", "-loglevel", "error",
                "-print_format", "json", "-show_format", out_path,
            ],
            user_id=user_id,
            timeout=30.0,
        )
        lpd = 0
        if rc2 == 0:
            try:
                fields2 = json.loads(out2).get("format", {})
                lpd = round(float(fields2.get("duration", 0)))
            except Exception:
                lpd = 0

        if lpd == 0:
            break
        elif duration == lpd:
            out_paths.append(out_path)
            break
        elif lpd <= 3:
            os.remove(out_path)
            break

        out_paths.append(out_path)
        start_time += lpd - 3
        i += 1

    return out_paths or [local_path]


async def generate_thumbnail(local_path: str, user_id: int = 0) -> str | None:
    thumb_path = local_path + "_thumb.jpg"
    try:
        rc, stdout, _ = await _run_cmd_async(
            [
                "ffprobe", "-hide_banner", "-loglevel", "error",
                "-print_format", "json", "-show_format", local_path,
            ],
            user_id=user_id,
            timeout=15.0,
        )
        seek_time = 3
        if rc == 0:
            try:
                duration = float(json.loads(stdout).get("format", {}).get("duration", 0))
                if duration > 0:
                    seek_time = max(1, int(duration / 2))
            except Exception:
                pass

        await _run_cmd_async(
            [
                "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                "-ss", str(seek_time),
                "-i", local_path,
                "-vf", "thumbnail,scale=320:-1",
                "-q:v", "2",
                "-frames:v", "1",
                "-threads", "1",
                thumb_path,
            ],
            user_id=user_id,
            timeout=25.0,
        )
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0:
            return thumb_path
    except Exception:
        pass
    finally:
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) == 0:
            try:
                os.remove(thumb_path)
            except Exception:
                pass
    return None


async def _send_with_retry(coro_fn, *args, max_retries: int = 5, **kwargs):
    attempts = 0
    while True:
        try:
            return await coro_fn(*args, **kwargs)
        except FloodWait as e:
            attempts += 1
            wait = e.value + 5
            log.warning(f"FloodWait {e.value}s (attempt {attempts}/{max_retries})")
            with stats_lock:
                stats["floodwait_count"] += 1
                stats["floodwait_last_secs"] = e.value
                stats["floodwait_last_time"] = time.time()
            if attempts >= max_retries:
                raise
            await asyncio.sleep(wait)


# ── 10. Live Transfer Progress Board ─────────────────────────────────────────
def _render_board(total_files: int, concurrent: int, progress_map: dict, user_id: int = 0) -> str:
    is_cancelling = cancel_flags.get(user_id, False)
    state_tag = "🛑 **STOPPING AFTER CURRENT**" if is_cancelling else f"⚡ **{concurrent}x Parallel**"
    header = f"📡 **Live Transfer Board** · {state_tag}\n━━━━━━━━━━━━━━━━━━━━━━"

    done = sum(1 for v in progress_map.values() if v.startswith(("✅ ", "✅🗑", "✅⚠️")))
    failed = sum(1 for v in progress_map.values() if v.startswith("❌"))
    skipped = sum(1 for v in progress_map.values() if v.startswith("⏭"))

    active_entries = {
        k: v for k, v in progress_map.items()
        if not (v.startswith(("✅ ", "✅🗑", "✅⚠️")) or v.startswith("❌") or v.startswith("⏭"))
    }
    queued = max(0, total_files - done - failed - skipped - len(active_entries))

    if active_entries:
        body = "\n\n".join(active_entries[k] for k in sorted(active_entries))
    else:
        body = "⏳ _Preparing next file…_"

    overall_done = done + failed + skipped
    overall_bar = progress_bar(overall_done, total_files, 12)
    overall_pct = int(overall_done * 100 / total_files) if total_files > 0 else 0

    footer_lines = [
        f"╭─ **Progress:** `{overall_bar}` **{overall_pct}%** (`{overall_done}/{total_files}`)",
        f"├ ✅ `{done}` Done · 🔄 `{len(active_entries)}` Active · ⏳ `{queued}` Queued · ❌ `{failed}`"
        + (f" · ⏭ `{skipped}`" if skipped else ""),
    ]
    if psutil is not None:
        try:
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            footer_lines.append(f"├ 🖥 CPU `{cpu:.0f}%` · 🧠 RAM `{ram:.0f}%`")
        except Exception:
            pass

    try:
        du = shutil.disk_usage(DOWNLOAD_DIR)
        footer_lines.append(f"╰ 💾 Free Disk: `{fmt_size(du.free)} / {fmt_size(du.total)}`")
    except Exception:
        footer_lines[-1] = footer_lines[-1].replace("├", "╰", 1)

    text = f"{header}\n\n{body}\n\n━━━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(footer_lines)
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
            await asyncio.sleep(BOARD_REFRESH_INTERVAL)
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


# ── 11. Transfer Pipeline, Checkpointing & Auto-Resume Runner ────────────────
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
) -> Message | None:
    loop = asyncio.get_running_loop()
    fname_disp = fname.split("/")[-1]
    base_label = f"📤 `[{idx}/{total_files}]` `{fname_disp}`"
    part_str = f"{part_idx}/{total_parts}" if total_parts > 1 else ""
    last_cb_time = [0.0]

    async def progress_handler(current: int, total: int, *_) -> None:
        if force_kill_flags.get(user_id):
            raise RuntimeError("Upload aborted by user")
        now = loop.time()
        if current < total and (now - last_cb_time[0]) < 1.5:
            return
        last_cb_time[0] = now

        elapsed = now - upload_start[0]
        speed_bps = current / elapsed if elapsed > 0 else 0
        remaining = (total - current) / speed_bps if speed_bps > 0 else 0

        speed_str = fmt_size(int(speed_bps)) + "/s" if speed_bps > 0 else ""
        eta_str = fmt_eta(remaining)

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

    progress_map[idx] = make_progress_line(base_label, 0, file_size, part_info=part_str)
    if board_state is not None:
        board_state.mark_dirty()

    upload_start = [loop.time()]
    is_split = total_parts > 1
    ext = os.path.basename(part_path).rsplit(".", 1)[-1].lower()

    if mode == "video" and (ext in VIDEO_EXTS or is_split):
        return await _send_with_retry(
            app.send_video, DUMP_CHAT_ID, part_path,
            caption=caption, supports_streaming=True, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler,
        )
    elif ext in AUDIO_EXTS and mode != "document":
        return await _send_with_retry(
            app.send_audio, DUMP_CHAT_ID, part_path,
            caption=caption, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler,
        )
    elif ext in IMAGE_EXTS and mode != "document":
        return await _send_with_retry(
            app.send_photo, DUMP_CHAT_ID, part_path,
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler,
        )
    else:
        return await _send_with_retry(
            app.send_document, DUMP_CHAT_ID, part_path,
            caption=caption, thumb=thumb,
            reply_to_message_id=reply_to_message_id,
            progress=progress_handler,
        )


async def process_one_file(
    fname: str,
    full_remote: str,
    idx: int,
    total: int,
    mode: str,
    user_id: int,
    semaphore: asyncio.Semaphore,
    progress_map: dict,
    status_msg: Message,
    results: dict,
    job_concurrent: int,
    do_delete: bool = False,
    board_state: "_BoardState | None" = None,
    size_map: dict[str, int] | None = None,
    checkpoint_cb=None,
) -> None:
    async with semaphore:
        if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
            return

        file_dir = os.path.join(DOWNLOAD_DIR, f"job_{user_id}_{idx}")
        Path(file_dir).mkdir(parents=True, exist_ok=True)
        ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
        thumb_path = None
        split_dir_cleanup: str | None = None

        dl_start = time.monotonic()
        fname_display = fname.split("/")[-1]

        def dl_progress(done_bytes, total_bytes, pct, speed, eta):
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

        progress_map[idx] = f"╭─ 📥 `[{idx}/{total}]` `{fname_display}`\n╰ ⏳ _Connecting to remote…_"
        await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)

        with state_lock:
            size_cap_on = max_size_enabled
            size_cap = max_size_bytes
        if size_cap_on:
            remote_size = size_map.get(fname, -1) if size_map else -1
            if remote_size < 0:
                remote_size = await rclone_size(full_remote, user_id=user_id)
            if remote_size >= 0 and remote_size > size_cap:
                progress_map[idx] = (
                    f"⏭ `[{idx}/{total}]` Skipped: `{fname_display}` "
                    f"(`{fmt_size(remote_size)}` > `{fmt_size(size_cap)}`)"
                )
                results["skipped"].append(fname)
                with stats_lock:
                    stats["total_skipped"] = stats.get("total_skipped", 0) + 1
                shutil.rmtree(file_dir, ignore_errors=True)
                if checkpoint_cb:
                    await checkpoint_cb()
                return

        try:
            local_path = await rclone_download(full_remote, file_dir, dl_progress, user_id=user_id)
        except Exception as e:
            log.error(f"[{idx}/{total}] Download failed: {fname} — {e}")
            progress_map[idx] = f"❌ `[{idx}/{total}]` DL failed: `{fname_display}`\n    `{str(e)[:80]}`"
            results["failed"].append(fname)
            with stats_lock:
                stats["total_failed"] += 1
                stats["last_error"] = f"DL {fname}: {str(e)[:100]}"
            shutil.rmtree(file_dir, ignore_errors=True)
            if checkpoint_cb:
                await checkpoint_cb()
            return

        file_size = os.path.getsize(local_path)
        dl_elapsed = time.monotonic() - dl_start
        dl_speed = fmt_size(int(file_size / dl_elapsed)) + "/s" if dl_elapsed > 0 else ""

        progress_map[idx] = (
            f"╭─ 📥 Downloaded `[{idx}/{total}]` `{fname_display}`\n"
            f"╰ `{'▰' * 12}` **100%** · `{fmt_size(file_size)}`"
            + (f" · ⚡ `{dl_speed}`" if dl_speed else "")
        )
        await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)

        needs_split = file_size > SPLIT_SIZE

        if ext in VIDEO_EXTS:
            progress_map[idx] = f"╭─ 🖼 `[{idx}/{total}]` `{fname_display}`\n╰ 🎨 _Extracting video thumbnail…_"
            await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)
            thumb_path = await generate_thumbnail(local_path, user_id=user_id)

        if needs_split:
            progress_map[idx] = (
                f"╭─ ✂️ `[{idx}/{total}]` `{fname_display}`\n"
                f"╰ 🔄 _Splitting `{fmt_size(file_size)}` into <2GB parts…_"
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

        try:
            prev_msg_id: int | None = None
            all_parts_sent = True
            for part_idx, part_path in enumerate(parts, 1):
                if force_kill_flags.get(user_id):
                    raise RuntimeError("Job aborted by user")
                part_size = os.path.getsize(part_path)
                fname_only = fname.split("/")[-1]
                if ":" in fname_only:
                    fname_only = fname_only.split(":", 1)[-1]
                part_filename = os.path.basename(part_path)
                if ":" in part_filename:
                    part_filename = part_filename.split(":", 1)[-1]

                if total_parts > 1:
                    caption = (
                        f"🎬 `{part_filename}`\n"
                        f"🧩 **Part {part_idx}/{total_parts}** · `{fmt_size(part_size)}` "
                        f"(_Total: `{fmt_size(file_size)}`)"
                    )
                else:
                    caption = f"📁 `{fname_only}` · `{fmt_size(file_size)}`"

                sent = await upload_part_with_progress(
                    part_path=part_path,
                    caption=caption,
                    mode=mode,
                    thumb=thumb_path,
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
                )
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
                progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` Done (del skipped): `{fname_display}`"
                results["delete_failed"].append(fname)
            elif do_delete:
                progress_map[idx] = f"╭─ 🗑 `[{idx}/{total}]` `{fname_display}`\n╰ _Removing from remote…_"
                await _push_board(status_msg, total, job_concurrent, progress_map, board_state, user_id)
                try:
                    await rclone_delete(full_remote, user_id=user_id)
                    progress_map[idx] = f"✅🗑 `[{idx}/{total}]` `{fname_display}` · `{fmt_size(file_size)}`"
                    results["deleted"] += 1
                except Exception:
                    progress_map[idx] = f"✅⚠️ `[{idx}/{total}]` Done (del failed): `{fname_display}`"
                    results["delete_failed"].append(fname)
            else:
                progress_map[idx] = f"✅ `[{idx}/{total}]` `{fname_display}` · `{fmt_size(file_size)}`"

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
            log.error(f"[{idx}/{total}] Upload failed: {fname} — {e}")
            progress_map[idx] = f"❌ `[{idx}/{total}]` UL failed: `{fname_display}`\n    `{str(e)[:80]}`"
            results["failed"].append(fname)
            with stats_lock:
                stats["total_failed"] += 1
                stats["last_error"] = f"UL {fname}: {str(e)[:100]}"
            if checkpoint_cb:
                await checkpoint_cb()
        finally:
            if split_dir_cleanup:
                shutil.rmtree(split_dir_cleanup, ignore_errors=True)
            if thumb_path and os.path.exists(thumb_path):
                try:
                    os.remove(thumb_path)
                except OSError:
                    pass
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

    done_set = set(already_done or [])
    pending_files = [f for f in files if f not in done_set]
    total = len(files)
    mode_icon = "🎬 Video" if mode == "video" else "📄 Document"
    del_note = " · 🗑 Delete ON" if do_delete else ""
    job_concurrent = CONCURRENT_JOBS

    await safe_edit(
        status_msg,
        f"🚀 **Initializing Transfer Job**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 **Path:** `{remote_path}`\n"
        f"📦 **Files:** `{len(pending_files)}/{total}` · ⚡ **Parallel:** `{job_concurrent}x`\n"
        f"🎛 **Mode:** {mode_icon}{del_note}",
        reply_markup=active_job_keyboard(),
    )

    active_sessions.add(user_id)
    cancel_flags[user_id] = False
    force_kill_flags[user_id] = False
    upload_mode[user_id] = mode
    with stats_lock:
        stats["current_files"] = [f.split("/")[-1] for f in pending_files[:3]]

    semaphore = asyncio.Semaphore(job_concurrent)
    progress_map: dict[int, str] = {}
    for idx_pre, fname_pre in enumerate(files, 1):
        if fname_pre in done_set:
            progress_map[idx_pre] = f"✅ `[{idx_pre}/{total}]` `{fname_pre.split('/')[-1]}` · _resumed_"

    results = {
        "success": len(done_set),
        "done_files": list(done_set),
        "failed": [],
        "deleted": 0,
        "delete_failed": [],
        "skipped": [],
    }
    board_state = _BoardState(status_msg, total, job_concurrent, progress_map, user_id)
    job_start = time.monotonic()

    async def _checkpoint():
        await db_mgr.save_active_job(
            remote_path=remote_path,
            mode=mode,
            do_delete=do_delete,
            user_id=user_id,
            chat_id=status_msg.chat.id,
            status_msg_id=status_msg.id,
            all_files=files,
            done_files=results["done_files"],
            failed_files=results["failed"],
            skipped_files=results["skipped"],
        )

    await _checkpoint()

    try:
        tasks = []

        async def _guarded(fname, full_remote, idx):
            if cancel_flags.get(user_id) or force_kill_flags.get(user_id):
                return
            with stats_lock:
                if fname.split("/")[-1] not in stats["current_files"]:
                    stats["current_files"] = (stats["current_files"] + [fname.split("/")[-1]])[-3:]
            await process_one_file(
                fname, full_remote, idx, total,
                mode, user_id, semaphore,
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
            _rp = remote_path.rstrip("/")
            if ":" in _rp:
                _rpx, _rpp = _rp.split(":", 1)
                _rpp = _rpp.lstrip("/")
                _rp = f"{_rpx}:{_rpp}" if _rpp else f"{_rpx}:"
            if total == 1 and not remote_path.endswith("/"):
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
        active_sessions.discard(user_id)
        was_cancelled = cancel_flags.pop(user_id, False) or force_kill_flags.pop(user_id, False)
        with stats_lock:
            stats["current_files"] = []

    # Clear active job checkpoint if completed or user explicitly cancelled
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
    job_concurrent = results["job_concurrent"]
    do_delete = results["do_delete"]
    remote_path = results.get("remote_path", "")
    elapsed = results.get("elapsed", "—")
    was_cancelled = results.get("was_cancelled", False)

    title = "🛑 **Job Stopped**" if was_cancelled else "🎉 **Transfer Complete!**"
    summary = (
        f"{title}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"╭─ **Summary**\n"
        f"├ 📂 **Source:** `{remote_path}`\n"
        f"├ ✅ **Uploaded:** `{success}/{total}` files\n"
        f"├ 🎛 **Mode:** {mode_icon} (`{job_concurrent}x` parallel)\n"
        f"╰ ⏱ **Duration:** `{elapsed}`"
    )
    if do_delete:
        summary += f"\n🗑 **Remote Deleted:** `{deleted}/{success}`"
    if skipped:
        skip_list = "\n".join(f"• `{f.split('/')[-1]}`" for f in skipped[:8])
        if len(skipped) > 8:
            skip_list += f"\n_…and {len(skipped) - 8} more_"
        summary += f"\n\n⏭ **Skipped — Over Size Cap ({len(skipped)}):**\n{skip_list}"
    if failed:
        fail_list = "\n".join(f"• `{f.split('/')[-1]}`" for f in failed[:8])
        if len(failed) > 8:
            fail_list += f"\n_…and {len(failed) - 8} more_"
        summary += f"\n\n❌ **Failed ({len(failed)}):**\n{fail_list}"
    if delete_failed:
        df_list = "\n".join(f"• `{f.split('/')[-1]}`" for f in delete_failed[:5])
        summary += f"\n\n⚠️ **Delete Failed ({len(delete_failed)}):**\n{df_list}"

    rows = []
    if failed:
        rows.append([InlineKeyboardButton(f"🔁 Retry Failed ({len(failed)})", callback_data="action:retry")])
    if remote_path:
        key = _reg_path(remote_path)
        rows.append([InlineKeyboardButton("📂 Re-open Path Options", callback_data=f"dl:open:{key}")])
    rows.append([
        InlineKeyboardButton("🗂 Browse Remotes", callback_data="nav:remotes:0"),
        InlineKeyboardButton("📊 Status", callback_data="nav:status"),
    ])
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home")])

    await asyncio.sleep(1.0)
    await safe_edit(status_msg, summary, reply_markup=InlineKeyboardMarkup(rows))


async def _resume_saved_job(saved_job: dict[str, Any], notify_chat_id: int | None = None) -> None:
    """Resume an unfinished transfer job from MongoDB checkpoint."""
    remote_path = saved_job.get("remote_path", "")
    mode = saved_job.get("mode", DEFAULT_UPLOAD_MODE)
    do_delete = bool(saved_job.get("do_delete", False))
    user_id = int(saved_job.get("user_id") or OWNER_ID)
    chat_id = int(notify_chat_id or saved_job.get("chat_id") or OWNER_ID)
    all_files = list(saved_job.get("all_files") or [])
    done_files = list(saved_job.get("done_files") or [])

    if not remote_path or not all_files:
        await db_mgr.clear_active_job()
        return

    remaining = [f for f in all_files if f not in set(done_files)]
    if not remaining:
        await db_mgr.clear_active_job()
        return

    if user_id in active_sessions:
        return

    status_msg = await app.send_message(
        chat_id,
        f"🔄 **Auto-Resuming Unfinished Job**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 **Path:** `{remote_path}`\n"
        f"✅ **Already Completed:** `{len(done_files)}/{len(all_files)}`\n"
        f"⏳ **Remaining:** `{len(remaining)}` file(s)",
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


# ── Interactive Remote Browser Builder ────────────────────────────────────────
async def _build_remotes_browser(remote_path: str = "", page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    PAGE_SIZE = 8
    if not remote_path:
        remotes = await rclone_list_remotes()
        if not remotes:
            text = (
                "🗂 **Rclone Remote Browser**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ No configured remotes found!\n\n"
                "Tap **🔧 Upload rclone.conf** below (it will be saved in MongoDB automatically)."
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 Upload rclone.conf", callback_data="nav:setrclone")],
                [InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home")],
            ])
            return text, kb

        total_pages = max(1, (len(remotes) + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(0, min(page, total_pages - 1))
        chunk = remotes[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        rows = []
        for i in range(0, len(chunk), 2):
            row = []
            for r in chunk[i:i + 2]:
                k = _reg_path(r)
                row.append(InlineKeyboardButton(f"☁️ {r.rstrip(':')}", callback_data=f"br:dir:{k}:0"))
            rows.append(row)

        if total_pages > 1:
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"nav:remotes:{page - 1}"))
            nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"nav:remotes:{page + 1}"))
            rows.append(nav_row)

        rows.append([
            InlineKeyboardButton("🔧 Update rclone.conf", callback_data="nav:setrclone"),
            InlineKeyboardButton("🏠 Main Menu", callback_data="nav:home"),
        ])
        text = (
            f"🗂 **Select an Rclone Remote** (`{len(remotes)}` available)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Tap a remote below to browse its folders or start a transfer:"
        )
        return text, InlineKeyboardMarkup(rows)

    dirs = await rclone_list_dirs(remote_path)
    total_pages = max(1, (len(dirs) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    chunk = dirs[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    cur_key = _reg_path(remote_path)
    rows = [
        [InlineKeyboardButton("📥 Download All Media in This Folder", callback_data=f"dl:open:{cur_key}")]
    ]

    for d in chunk:
        sub_path = f"{remote_path.rstrip('/')}/{d}" if not remote_path.endswith(":") else f"{remote_path}{d}"
        sub_key = _reg_path(sub_path)
        label = d if len(d) <= 28 else d[:25] + "…"
        rows.append([InlineKeyboardButton(f"📁 {label}/", callback_data=f"br:dir:{sub_key}:0")])

    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"br:dir:{cur_key}:{page - 1}"))
        nav_row.append(InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"br:dir:{cur_key}:{page + 1}"))
        rows.append(nav_row)

    if ":" in remote_path:
        r_prefix, r_rest = remote_path.split(":", 1)
        r_rest = r_rest.strip("/")
        if not r_rest:
            rows.append([
                InlineKeyboardButton("⬆️ Back to Remotes", callback_data="nav:remotes:0"),
                InlineKeyboardButton("🏠 Home", callback_data="nav:home"),
            ])
        else:
            parent_rest = r_rest.rsplit("/", 1)[0] if "/" in r_rest else ""
            parent_path = f"{r_prefix}:{parent_rest}" if parent_rest else f"{r_prefix}:"
            p_key = _reg_path(parent_path)
            rows.append([
                InlineKeyboardButton("⬆️ Parent Folder", callback_data=f"br:dir:{p_key}:0"),
                InlineKeyboardButton("🗂 All Remotes", callback_data="nav:remotes:0"),
            ])

    text = (
        f"🗂 **Remote Folder Browser**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 **Path:** `{remote_path}`\n"
        f"📁 **Subfolders:** `{len(dirs)}`\n\n"
        f"Tap **📥 Download All Media** to transfer this folder, or open a subfolder:"
    )
    return text, InlineKeyboardMarkup(rows)


# ── 12. Telegram Command Handlers & Auto-Registered Commands ──────────────────
BOT_COMMANDS = [
    BotCommand("start", "🏠 Main menu & quick dashboard"),
    BotCommand("dl", "📥 Download & upload from rclone remote"),
    BotCommand("remotes", "🗂 Browse rclone remotes & folders"),
    BotCommand("status", "📊 Live bot stats & system resources"),
    BotCommand("settings", "⚙️ Interactive settings control panel"),
    BotCommand("db", "🗄 MongoDB status, sync & saved job manager"),
    BotCommand("resume", "▶️ Resume unfinished job from MongoDB"),
    BotCommand("queue", "🔄 View active transfer queue"),
    BotCommand("retry", "🔁 Retry failed files from last job"),
    BotCommand("setrclone", "🔧 Upload & save rclone.conf to MongoDB"),
    BotCommand("setdelete", "🗑 Toggle remote auto-delete (on/off)"),
    BotCommand("setmaxsize", "📏 Configure max file size cap (MB)"),
    BotCommand("concurrent", "⚡ Set parallel file transfers (1-5)"),
    BotCommand("setbwlimit", "🚦 Set bandwidth limit (e.g. 8M or off)"),
    BotCommand("setrefresh", "⏱ Set progress refresh rate (2-30s)"),
    BotCommand("auth", "👥 Manage authorized user IDs"),
    BotCommand("logs", "📜 View recent bot logs"),
    BotCommand("cancel", "🛑 Stop current job after active files"),
    BotCommand("forcestop", "⛔ Immediately kill active transfers"),
    BotCommand("restart", "🔄 Save state & restart bot"),
    BotCommand("stop", "🔌 Shut down the bot"),
]


async def register_bot_commands_auto() -> bool:
    """Automatically register all bot commands across Telegram scopes on startup."""
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


@app.on_message(filters.command(["start", "help"]) & auth_filter)
async def cmd_start(_, msg: Message):
    await msg.reply(_build_home_text(), reply_markup=home_keyboard())


@app.on_message(filters.command("status") & auth_filter)
async def cmd_status(_, msg: Message):
    await msg.reply(_build_status_text(), reply_markup=status_keyboard())


@app.on_message(filters.command("settings") & auth_filter)
async def cmd_settings(_, msg: Message):
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("db") & auth_filter)
async def cmd_db(_, msg: Message):
    await msg.reply(await _build_db_text(), reply_markup=db_keyboard())


@app.on_message(filters.command("resume") & auth_filter)
async def cmd_resume(_, msg: Message):
    if msg.from_user.id in active_sessions:
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
                await msg.reply(f"🗑 Removed `{uid}` from authorized users (synced to MongoDB).")
                return
            else:
                with state_lock:
                    AUTHORIZED_USERS.add(uid)
                await db_mgr.save_all()
                await msg.reply(f"✅ Added `{uid}` to authorized users (synced to MongoDB).")
                return
        except ValueError:
            pass
    with state_lock:
        ulist = "\n".join(f"• `{u}`" + (" _(Owner)_" if u == OWNER_ID else "") for u in sorted(AUTHORIZED_USERS))
    await msg.reply(
        f"👥 **Authorized Users** (Synced to MongoDB)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{ulist}\n\n"
        f"**Usage:**\n"
        f"• `/auth 123456789` — Add user ID\n"
        f"• `/auth -123456789` — Remove user ID",
        reply_markup=home_keyboard(),
    )


@app.on_message(filters.command("remotes") & auth_filter)
async def cmd_remotes(_, msg: Message):
    wait_msg = await msg.reply("🔍 _Loading rclone remotes…_")
    text, kb = await _build_remotes_browser("", 0)
    await safe_edit(wait_msg, text, reply_markup=kb)


@app.on_message(filters.command("queue") & auth_filter)
async def cmd_queue(_, msg: Message):
    with stats_lock:
        cur = list(stats["current_files"])
    if not active_sessions:
        await msg.reply(
            "📭 **No active transfers right now.**\nUse `/dl <remote:path>` or browse remotes below:",
            reply_markup=home_keyboard(),
        )
        return
    body = "\n".join(f"• `{f}`" for f in cur) if cur else "_Preparing files…_"
    await msg.reply(
        f"🔄 **Active Transfer Queue** (`{len(active_sessions)}` session)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{body}",
        reply_markup=active_job_keyboard(),
    )


@app.on_message(filters.command("logs") & auth_filter)
async def cmd_logs(_, msg: Message):
    lines = list(log_buffer)[-30:]
    body = "\n".join(lines) if lines else "No logs recorded yet."
    text = f"📜 **Recent Bot Logs**\n```\n{body[-3600:]}\n```"
    await msg.reply(text, reply_markup=logs_keyboard())


@app.on_message(filters.command("cancel") & auth_filter)
async def cmd_cancel(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in awaiting_rclone_conf:
        awaiting_rclone_conf.discard(user_id)
        await msg.reply("❌ `rclone.conf` upload cancelled.", reply_markup=home_keyboard())
        return
    if user_id not in active_sessions:
        await msg.reply("ℹ️ No active job to cancel.", reply_markup=home_keyboard())
        return
    cancel_flags[user_id] = True
    if user_id in active_boards:
        active_boards[user_id].mark_dirty()
    await msg.reply(
        "🛑 **Graceful stop requested.**\n"
        "In-flight files will finish uploading, then the job will stop.\n"
        "Want to abort immediately? Tap below:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⛔ Kill Active Transfers Now", callback_data="job:kill")]
        ]),
    )


@app.on_message(filters.command("forcestop") & auth_filter)
async def cmd_forcestop(_, msg: Message):
    if not active_sessions:
        await msg.reply("ℹ️ No active job is running.", reply_markup=home_keyboard())
        return
    total_killed = 0
    for uid in list(active_sessions):
        cancel_flags[uid] = True
        force_kill_flags[uid] = True
        total_killed += kill_user_procs(uid)
    await db_mgr.clear_active_job()
    await db_mgr.save_all()
    await msg.reply(
        f"⛔ **Job Force-Stopped Immediately**\n"
        f"Terminated `{total_killed}` active `rclone`/`ffmpeg` process(es) and cleared checkpoint.\n"
        f"The bot remains online and ready!",
        reply_markup=home_keyboard(),
    )


@app.on_message(filters.command("stop") & auth_filter)
async def cmd_stop(_, msg: Message):
    for uid in list(active_sessions):
        kill_user_procs(uid)
    await db_mgr.save_all()
    await msg.reply("🔌 **All settings saved to MongoDB. Shutting down…**")
    await asyncio.sleep(0.5)
    os._exit(0)


@app.on_message(filters.command("restart") & auth_filter)
async def cmd_restart(_, msg: Message):
    for uid in list(active_sessions):
        kill_user_procs(uid)
    await db_mgr.save_all()
    sent = await msg.reply("🔄 **Saving state to MongoDB & restarting bot…**")
    await db_mgr.save_restart_marker(sent.chat.id, sent.id)
    await asyncio.sleep(0.5)
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
                await msg.reply("❌ Usage: `/setmaxsize on`, `/setmaxsize off`, or `/setmaxsize 2000` (MB).")
                return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("concurrent") & auth_filter)
async def cmd_concurrent(_, msg: Message):
    global CONCURRENT_JOBS
    if len(msg.command) >= 2:
        try:
            val = int(msg.command[1].strip())
            if not 1 <= val <= 5:
                raise ValueError
            with state_lock:
                CONCURRENT_JOBS = val
            await db_mgr.save_all()
        except ValueError:
            await msg.reply("❌ Value must be between 1 and 5. Example: `/concurrent 2`")
            return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


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
            await msg.reply("❌ Invalid value. Examples: `512k`, `8M`, `1G`, or `off`.")
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
            await msg.reply("❌ Value must be between 2 and 30 seconds. Example: `/setrefresh 5`")
            return
    await msg.reply(_build_settings_text(), reply_markup=settings_keyboard())


@app.on_message(filters.command("dl") & auth_filter)
async def cmd_dl(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply(
            "⚠️ **A job is already running.**\nStop it first or wait for it to finish:",
            reply_markup=active_job_keyboard(),
        )
        return
    if len(msg.command) < 2:
        wait_msg = await msg.reply("🔍 _Opening remote folder browser…_")
        text, kb = await _build_remotes_browser("", 0)
        await safe_edit(wait_msg, text, reply_markup=kb)
        return
    remote_path = msg.text.split(None, 1)[1].strip()
    await msg.reply(
        _build_dl_prompt_text(remote_path),
        reply_markup=mode_keyboard(remote_path),
    )


@app.on_message(filters.command("retry") & auth_filter)
async def cmd_retry(_, msg: Message):
    user_id = msg.from_user.id
    if user_id in active_sessions:
        await msg.reply("⚠️ Job already running. Stop it first.", reply_markup=active_job_keyboard())
        return
    with state_lock:
        files = list(last_failed)
        remote_path = last_job_remote
        mode = last_job_mode
        do_delete = delete_after_upload
    if not files or not remote_path:
        await msg.reply("ℹ️ Nothing to retry — no failed files from the last job.", reply_markup=home_keyboard())
        return
    status_msg = await msg.reply(f"🔁 **Retrying `{len(files)}` failed file(s)…**")
    results = await _run_job(status_msg, files, remote_path, mode, user_id, do_delete)
    await _send_summary(status_msg, results)


def _validate_rclone_conf_text(text: str) -> list[str]:
    remotes = re.findall(r"^\[([^\]]+)\]", text, re.MULTILINE)
    if not remotes:
        raise ValueError("No `[remote]` sections found — this doesn't look like an rclone.conf file.")
    return remotes


@app.on_message(filters.command("setrclone") & auth_filter)
async def cmd_setrclone(_, msg: Message):
    if active_sessions:
        await msg.reply("⚠️ Stop the running job before swapping `rclone.conf`.", reply_markup=active_job_keyboard())
        return
    user_id = msg.from_user.id
    awaiting_rclone_conf.add(user_id)
    await msg.reply(
        "📤 **Upload `rclone.conf` (Persisted in MongoDB)**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Send your `rclone.conf` file now as a Telegram document.\n"
        "It will be validated, activated immediately, and saved to MongoDB so it survives restarts!",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel Upload", callback_data="action:cancel_rclone")]
        ]),
    )


@app.on_message(filters.document & auth_filter)
async def on_rclone_conf_document(_, msg: Message):
    user_id = msg.from_user.id
    if user_id not in awaiting_rclone_conf:
        return

    awaiting_rclone_conf.discard(user_id)
    doc = msg.document
    if doc.file_size and doc.file_size > 1024 * 1024:
        await msg.reply("❌ File is too large (>1 MB) to be an `rclone.conf`.", reply_markup=home_keyboard())
        return

    status = await msg.reply("⬇️ _Downloading, validating & saving `rclone.conf` to MongoDB…_")
    tmp_path = os.path.join(DOWNLOAD_DIR, f"rclone_upload_{user_id}_{int(time.time())}.conf")
    try:
        await msg.download(file_name=tmp_path)
        text = Path(tmp_path).read_text(encoding="utf-8", errors="strict")
        remotes = _validate_rclone_conf_text(text)

        RCLONE_CONF_PATH.parent.mkdir(parents=True, exist_ok=True)
        if RCLONE_CONF_PATH.exists():
            shutil.copy2(RCLONE_CONF_PATH, RCLONE_CONF_PATH.with_suffix(".conf.bak"))
        RCLONE_CONF_PATH.write_text(text, encoding="utf-8")
        os.environ["RCLONE_CONFIG"] = str(RCLONE_CONF_PATH)
        await db_mgr.save_rclone_conf(text, source="telegram_upload")
        log.info(f"rclone.conf updated & saved to MongoDB by {user_id} ({len(remotes)} remotes)")
    except Exception as e:
        await safe_edit(status, f"❌ **Config Update Failed:**\n`{e}`", reply_markup=home_keyboard())
        return
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    remote_list = "\n".join(f"• `{r}`" for r in remotes[:15])
    await safe_edit(
        status,
        f"✅ **`rclone.conf` Activated & Saved to MongoDB!** (`{len(remotes)}` remote(s))\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{remote_list}\n\n"
        f"Tap **🗂 Browse Remotes** to start transferring:",
        reply_markup=home_keyboard(),
    )


# ── 13. Unified Callback Query Router ────────────────────────────────────────
_SIZE_PRESETS_MB = [250, 500, 1024, 2000, 4000]
_BW_PRESETS = ["", "2M", "5M", "10M", "25M", "50M"]
_REFRESH_PRESETS = [3.0, 5.0, 8.0, 12.0, 15.0]


@app.on_callback_query(auth_filter)
async def on_callback(_, cq: CallbackQuery):
    global delete_after_upload, max_size_enabled, max_size_bytes
    global CONCURRENT_JOBS, BW_LIMIT, BOARD_REFRESH_INTERVAL
    global AUTO_RESUME_JOBS, DEFAULT_UPLOAD_MODE

    data = cq.data or ""
    user_id = cq.from_user.id

    if data == "noop":
        await cq.answer()
        return

    if data == "action:close":
        await cq.answer("Closed")
        try:
            await cq.message.delete()
        except Exception:
            pass
        return

    if data == "action:cancel_rclone":
        awaiting_rclone_conf.discard(user_id)
        await cq.answer("Upload cancelled")
        await safe_edit(cq.message, _build_home_text(), reply_markup=home_keyboard())
        return

    if data == "action:clear_logs":
        log_buffer.clear()
        await cq.answer("Logs cleared! 🧹")
        await safe_edit(
            cq.message,
            "📜 **Recent Bot Logs**\n```\nLog buffer cleared.\n```",
            reply_markup=logs_keyboard(),
        )
        return

    if data == "action:restart_bot":
        for uid in list(active_sessions):
            kill_user_procs(uid)
        await db_mgr.save_all()
        await cq.answer("🔄 Saving config to MongoDB & restarting…", show_alert=True)
        await safe_edit(cq.message, "🔄 **Saving config to MongoDB & restarting bot…**")
        await db_mgr.save_restart_marker(cq.message.chat.id, cq.message.id)
        await asyncio.sleep(0.5)
        os.execv(sys.executable, [sys.executable] + sys.argv)
        return

    if data == "action:resume_job":
        if user_id in active_sessions:
            await cq.answer("⚠️ A job is already running!", show_alert=True)
            return
        saved = await db_mgr.get_unfinished_job()
        if not saved:
            await cq.answer("ℹ️ No unfinished job saved in MongoDB.", show_alert=True)
            return
        await cq.answer("▶️ Resuming saved job…")
        asyncio.create_task(_resume_saved_job(saved, notify_chat_id=cq.message.chat.id))
        return

    # ── Navigation callbacks ──
    if data == "nav:home":
        awaiting_rclone_conf.discard(user_id)
        await cq.answer()
        await safe_edit(cq.message, _build_home_text(), reply_markup=home_keyboard())
        return

    if data == "nav:status":
        await cq.answer("Updated 📊")
        await safe_edit(cq.message, _build_status_text(), reply_markup=status_keyboard())
        return

    if data == "nav:settings":
        await cq.answer()
        await safe_edit(cq.message, _build_settings_text(), reply_markup=settings_keyboard())
        return

    if data == "nav:db":
        await cq.answer()
        await safe_edit(cq.message, await _build_db_text(), reply_markup=db_keyboard())
        return

    if data == "nav:queue":
        with stats_lock:
            cur = list(stats["current_files"])
        if not active_sessions:
            await cq.answer("No active job right now.", show_alert=False)
            await safe_edit(
                cq.message,
                "📭 **No active transfers right now.**\nUse **🗂 Browse Remotes** or `/dl <remote:path>` to start one.",
                reply_markup=home_keyboard(),
            )
            return
        await cq.answer()
        body = "\n".join(f"• `{f}`" for f in cur) if cur else "_Preparing files…_"
        await safe_edit(
            cq.message,
            f"🔄 **Active Transfer Queue** (`{len(active_sessions)}` session)\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{body}",
            reply_markup=active_job_keyboard(),
        )
        return

    if data == "nav:logs":
        await cq.answer("Logs refreshed 📜")
        lines = list(log_buffer)[-30:]
        body = "\n".join(lines) if lines else "No logs recorded yet."
        await safe_edit(
            cq.message,
            f"📜 **Recent Bot Logs**\n```\n{body[-3600:]}\n```",
            reply_markup=logs_keyboard(),
        )
        return

    if data == "nav:setrclone":
        if active_sessions:
            await cq.answer("⚠️ Stop the running job first!", show_alert=True)
            return
        awaiting_rclone_conf.add(user_id)
        await cq.answer()
        await safe_edit(
            cq.message,
            "📤 **Upload `rclone.conf` (Saved to MongoDB)**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n"
            "Send your `rclone.conf` file now as a Telegram document.\n"
            "It will be validated, activated, and backed up in MongoDB.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel Upload", callback_data="action:cancel_rclone")]
            ]),
        )
        return

    if data.startswith("nav:remotes:"):
        page = int(data.split(":")[2])
        await cq.answer("Loading remotes…")
        text, kb = await _build_remotes_browser("", page)
        await safe_edit(cq.message, text, reply_markup=kb)
        return

    # ── MongoDB Panel callbacks ──
    if data.startswith("db:"):
        sub = data.split(":", 1)[1]
        if sub == "save":
            await db_mgr.save_all()
            if RCLONE_CONF_PATH.exists():
                try:
                    await db_mgr.save_rclone_conf(RCLONE_CONF_PATH.read_text(encoding="utf-8"), source="manual_sync")
                except Exception:
                    pass
            await cq.answer("✅ All settings, stats & rclone.conf saved to MongoDB!", show_alert=True)
        elif sub == "reload":
            await db_mgr.load_all()
            await cq.answer("📥 Reloaded config & stats from MongoDB!", show_alert=True)
        elif sub == "clear_job":
            await db_mgr.clear_active_job()
            await cq.answer("🗑 Saved job checkpoint cleared.", show_alert=True)
        await safe_edit(cq.message, await _build_db_text(), reply_markup=db_keyboard())
        return

    # ── Folder Browser callbacks ──
    if data.startswith("br:dir:"):
        _, _, key, page_s = data.split(":", 3)
        rpath = _get_path(key)
        if not rpath:
            await cq.answer("Session expired — reloading remotes", show_alert=False)
            text, kb = await _build_remotes_browser("", 0)
            await safe_edit(cq.message, text, reply_markup=kb)
            return
        await cq.answer(f"Opening {rpath}…")
        text, kb = await _build_remotes_browser(rpath, int(page_s))
        await safe_edit(cq.message, text, reply_markup=kb)
        return

    # ── Settings Panel callbacks ──
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
            await cq.answer(f"▶️ Auto-Resume on Restart: {state_str}")
        elif action == "toggle_defmode":
            with state_lock:
                DEFAULT_UPLOAD_MODE = "document" if DEFAULT_UPLOAD_MODE == "video" else "video"
                m_str = DEFAULT_UPLOAD_MODE.capitalize()
            await db_mgr.save_all()
            await cq.answer(f"🎬 Default Mode: {m_str}")
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
            await cq.answer(f"📐 Size Cap set to {nxt} MB (ON)")
        elif action == "cycle_conc":
            with state_lock:
                CONCURRENT_JOBS = (CONCURRENT_JOBS % 5) + 1
                c_val = CONCURRENT_JOBS
            await db_mgr.save_all()
            await cq.answer(f"⚡ Parallel transfers: {c_val}x")
        elif action == "cycle_bw":
            with state_lock:
                idx_bw = _BW_PRESETS.index(BW_LIMIT) if BW_LIMIT in _BW_PRESETS else 0
                BW_LIMIT = _BW_PRESETS[(idx_bw + 1) % len(_BW_PRESETS)]
                bw_lbl = BW_LIMIT or "Unlimited"
            await db_mgr.save_all()
            await cq.answer(f"🚦 Bandwidth Limit: {bw_lbl}")
        elif action == "cycle_refresh":
            with state_lock:
                idx_r = _REFRESH_PRESETS.index(BOARD_REFRESH_INTERVAL) if BOARD_REFRESH_INTERVAL in _REFRESH_PRESETS else 1
                BOARD_REFRESH_INTERVAL = _REFRESH_PRESETS[(idx_r + 1) % len(_REFRESH_PRESETS)]
                r_val = int(BOARD_REFRESH_INTERVAL)
            await db_mgr.save_all()
            await cq.answer(f"⏱ Refresh Interval: {r_val}s")
        elif action == "save_db":
            await db_mgr.save_all()
            await cq.answer("💾 Synced all settings to MongoDB!", show_alert=True)
        elif action == "sync_cmds":
            ok = await register_bot_commands_auto()
            await cq.answer(
                "✅ Bot commands synced with Telegram!" if ok else "⚠️ Command sync failed",
                show_alert=True,
            )
            return

        await safe_edit(cq.message, _build_settings_text(), reply_markup=settings_keyboard())
        return

    # ── Live Job Control callbacks ──
    if data == "job:refresh":
        board = active_boards.get(user_id)
        if board and board.running:
            await cq.answer("Board refreshed 🔄")
            await board.force_refresh()
        else:
            await cq.answer("No active transfer board.", show_alert=False)
        return

    if data == "job:cancel":
        if not active_sessions:
            await cq.answer("No active job running.", show_alert=False)
            return
        for uid in list(active_sessions):
            cancel_flags[uid] = True
            if uid in active_boards:
                active_boards[uid].mark_dirty()
        await cq.answer("🛑 Stopping after current file(s) finish…", show_alert=True)
        return

    if data == "job:kill":
        if not active_sessions:
            await cq.answer("No active job running.", show_alert=False)
            return
        killed = 0
        for uid in list(active_sessions):
            cancel_flags[uid] = True
            force_kill_flags[uid] = True
            killed += kill_user_procs(uid)
        await db_mgr.clear_active_job()
        await cq.answer(f"⛔ Aborted! Killed {killed} process(es).", show_alert=True)
        return

    # ── Retry callback ──
    if data == "action:retry":
        if user_id in active_sessions:
            await cq.answer("⚠️ A job is already running!", show_alert=True)
            return
        with state_lock:
            files = list(last_failed)
            remote_path = last_job_remote
            mode = last_job_mode
            do_delete = delete_after_upload
        if not files or not remote_path:
            await cq.answer("ℹ️ No failed files to retry.", show_alert=True)
            return
        await cq.answer(f"🔁 Retrying {len(files)} file(s)…")
        results = await _run_job(cq.message, files, remote_path, mode, user_id, do_delete)
        await _send_summary(cq.message, results)
        return

    # ── /dl Launchpad callbacks ──
    if data.startswith("dl:"):
        parts = data.split(":")
        sub = parts[1]
        key = parts[-1]
        remote_path = _get_path(key)
        if not remote_path:
            await cq.answer("⚠️ Session expired — please run /dl again.", show_alert=True)
            return

        if sub == "open":
            await cq.answer()
            await safe_edit(cq.message, _build_dl_prompt_text(remote_path), reply_markup=mode_keyboard(remote_path))
            return

        if sub == "del":
            with state_lock:
                delete_after_upload = not delete_after_upload
                st = "ON 🟢" if delete_after_upload else "OFF 🔴"
            await db_mgr.save_all()
            await cq.answer(f"🗑 Auto-Delete: {st}")
            await safe_edit(cq.message, _build_dl_prompt_text(remote_path), reply_markup=mode_keyboard(remote_path))
            return

        if sub == "size":
            with state_lock:
                max_size_enabled = not max_size_enabled
                st = "ON 🟢" if max_size_enabled else "OFF 🔴"
            await db_mgr.save_all()
            await cq.answer(f"📏 Size Cap: {st}")
            await safe_edit(cq.message, _build_dl_prompt_text(remote_path), reply_markup=mode_keyboard(remote_path))
            return

        if sub == "conc":
            with state_lock:
                CONCURRENT_JOBS = (CONCURRENT_JOBS % 5) + 1
                c_val = CONCURRENT_JOBS
            await db_mgr.save_all()
            await cq.answer(f"⚡ Parallel: {c_val}x")
            await safe_edit(cq.message, _build_dl_prompt_text(remote_path), reply_markup=mode_keyboard(remote_path))
            return

        if sub == "bw":
            with state_lock:
                idx_bw = _BW_PRESETS.index(BW_LIMIT) if BW_LIMIT in _BW_PRESETS else 0
                BW_LIMIT = _BW_PRESETS[(idx_bw + 1) % len(_BW_PRESETS)]
                bw_lbl = BW_LIMIT or "Unlimited"
            await db_mgr.save_all()
            await cq.answer(f"🚦 BW Limit: {bw_lbl}")
            await safe_edit(cq.message, _build_dl_prompt_text(remote_path), reply_markup=mode_keyboard(remote_path))
            return

        if sub == "go":
            chosen = parts[2]
            mode = "video" if chosen == "video" else "document"
            mode_icon = "🎬 Video" if mode == "video" else "📄 Document"
            if user_id in active_sessions:
                await cq.answer("⚠️ A job is already running!", show_alert=True)
                return
            with state_lock:
                do_delete = delete_after_upload
                size_cap_on = max_size_enabled
                cap_mb = max_size_bytes // (1024 * 1024)
            del_note = " · 🗑 Delete ON" if do_delete else ""
            size_note = f" · 📏 Cap {cap_mb}MB" if size_cap_on else ""

            await cq.answer(f"Starting {mode_icon} transfer…")
            await safe_edit(
                cq.message,
                f"🔍 **Scanning Remote Path**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📂 `{remote_path}`\n"
                f"🎛 {mode_icon} mode{del_note}{size_note}\n\n"
                f"⏳ _Listing media files via rclone…_",
            )

            try:
                files = await rclone_list(remote_path, user_id=user_id)
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
                    f"📭 **No media files found at:**\n`{remote_path}`",
                    reply_markup=home_keyboard(),
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


# ── 14. Startup Lifecycle, Auto Command Registration & Auto-Resume Hook ───────
async def _async_main():
    if psutil is not None:
        try:
            psutil.cpu_percent(interval=None)
        except Exception:
            pass

    threading.Thread(target=_run_health, daemon=True).start()
    log.info(f"Health check server listening on port {HEALTH_PORT}")

    # 1. Connect to MongoDB & restore all settings, stats, and rclone.conf
    await db_mgr.connect()

    # 2. Start Telegram Bot Client
    await app.start()

    # 3. Auto-register all bot commands with Telegram (@BotFather menu)
    await register_bot_commands_auto()

    me = await app.get_me()
    log.info(f"🚀 Bot @{me.username} (ID: {me.id}) is online!")

    # 4. If restarted via /restart or button, update the restart message
    marker = await db_mgr.pop_restart_marker()
    if marker and marker.get("chat_id") and marker.get("message_id"):
        try:
            await app.edit_message_text(
                int(marker["chat_id"]),
                int(marker["message_id"]),
                "✅ **Bot Restarted & Online!**\n"
                "All settings, stats, and `rclone.conf` have been restored from MongoDB.\n\n"
                + _build_home_text(),
                reply_markup=home_keyboard(),
            )
        except Exception as e:
            log.debug(f"Could not edit restart marker message: {e}")

    # 5. Check MongoDB for any unfinished job and auto-start/resume if enabled
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
                    f"⏸ **Unfinished Job Found in MongoDB**\n"
                    f"📂 Path: `{unfinished.get('remote_path')}`\n"
                    f"Tap below to resume where it left off:",
                    reply_markup=InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("▶️ Resume Job Now", callback_data="action:resume_job"),
                            InlineKeyboardButton("🗑 Discard", callback_data="db:clear_job"),
                        ]
                    ]),
                )
            except Exception:
                pass

    await idle()
    await db_mgr.save_all()
    await app.stop()


if __name__ == "__main__":
    app.run(_async_main())