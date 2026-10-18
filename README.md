# Rclone ➔ Telegram Transfer Bot (MongoDB + Auto-Resume Edition)

A fast, lightweight, async Telegram bot that downloads media from any [rclone](https://rclone.org)-supported remote (Google Drive, Dropbox, OneDrive, S3, Mega, WebDAV, etc.) and uploads it straight to a Telegram chat or channel with **MongoDB persistence**, **automatic job resumption on restart**, **auto-registered Telegram commands**, **visual folder browsing**, and an **optimized multi-stage Docker image**.

## Highlights

- **🗄 Full MongoDB Persistence (`MONGO_URI`)** — Saves all runtime settings (`Auto-Delete`, `Size Cap`, `Concurrency`, `BW Limit`, `Refresh Rate`, `Default Mode`, `Authorized Users`), lifetime transfer statistics, and your uploaded **`rclone.conf`** directly in MongoDB (`motor` async driver). Even on ephemeral container hosts (Heroku, Render, Railway, Koyeb, Fly.io), your config and `rclone.conf` are automatically restored on boot.
- **▶️ Auto-Start / Auto-Resume on Bot Restart** — Active transfer jobs checkpoint their progress to MongoDB after every file. If the bot restarts or redeploys mid-job, it automatically resumes remaining files on startup without re-uploading completed ones.
- **📲 Automatic Bot Command Registration** — Calls `set_bot_commands` across Default, Private Chat, and Group scopes automatically on startup so the `/` command menu is always populated without `@BotFather`.
- **🗂 Interactive Remote & Folder Browser (`/remotes`)** — Navigate cloud remotes and subfolders with inline pagination buttons and start transfers with one tap.
- **🎛 Interactive Control Panels (`/settings` & `/db`)** — Configure every setting or trigger database syncs/restores using inline buttons.
- **⚡ Lightweight Non-Blocking Async Engine** — Built on `uvloop` and native `asyncio.create_subprocess_exec` for `rclone` and `ffmpeg`/`ffprobe`, with instant subprocess termination on abort.

## Commands (Auto-Registered on Startup)

| Command | Description |
|---|---|
| `/start` | 🏠 Main menu & overview dashboard |
| `/dl <remote:path>` | 📥 Start a transfer (or open the visual remote browser if no path is given) |
| `/remotes` | 🗂 Browse configured rclone remotes and folders interactively |
| `/status` | 📊 Live bot telemetry, MongoDB status, and CPU/RAM/Disk bars |
| `/settings` | ⚙️ Interactive control panel (auto-synced to MongoDB) |
| `/db` | 🗄 MongoDB connection status, manual sync/reload & saved job manager |
| `/resume` | ▶️ Manually resume an unfinished job from MongoDB |
| `/queue` | 🔄 View active transfer queue and live controls |
| `/retry` | 🔁 Re-run only the files that failed in the last job |
| `/setrclone` | 🔧 Upload a new `rclone.conf` file (validated & saved to MongoDB) |
| `/setdelete on\|off` | 🗑 Toggle auto-deleting files from the remote after upload |
| `/setmaxsize on\|off\|<MB>` | 📏 Skip files larger than `<MB>` before downloading |
| `/concurrent <1-5>` | ⚡ Set parallel file transfers |
| `/setbwlimit <rate>\|off` | 🚦 Limit rclone download bandwidth (e.g. `8M` or `off`) |
| `/setrefresh <2-30>` | ⏱ Set progress board refresh interval in seconds |
| `/auth <user_id>` | 👥 Add or remove (`-user_id`) authorized users (saved to MongoDB) |
| `/logs` | 📜 View or clear recent in-memory bot logs |
| `/cancel` | 🛑 Stop current job gracefully after active files finish |
| `/forcestop` | ⛔ Immediately kill active `rclone`/`ffmpeg` subprocesses |
| `/restart` | 🔄 Save state to MongoDB & restart the bot process cleanly |
| `/stop` | 🔌 Save state & shut down the bot process |

## Quick Start with Docker Compose

1. Copy `config.env.example` to `config.env` and set `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID`, `DUMP_CHAT_ID`, and `MONGO_URI` (or use the bundled `mongo` service in `docker-compose.yml`).
2. Start the stack:

```bash
docker compose up -d --build
```
