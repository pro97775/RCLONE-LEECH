# ☁️ Rclone ➔ Telegram Transfer Bot (WZGram + MongoDB)

High-speed, lightweight cloud-to-Telegram media transfer bot powered by **[WZGram](https://wzgram.com/)** (Rust `WarpCrypto`), **Rclone**, **FFmpeg**, and **Async Motor MongoDB**.

## ✨ Key Features
- **Pure WZGram Engine**: Built exclusively on `wzgram` with Rust-accelerated `WarpCrypto` for maximum MTProto upload throughput.
- **Full MongoDB Persistence & Auto-Resume**:
  - Automatically persists all settings, **Concurrency Setup** (`CONCURRENT_JOBS`, `RCLONE_STREAMS`, `RCLONE_BUFFER_SIZE`), `rclone.conf`, authorized users, and transfer statistics in MongoDB.
  - Checkpoints active transfer jobs after every file and automatically resumes unfinished jobs on container/bot restart.
- **Dedicated Concurrency & Streams Setup UI (`/concurrent`)**:
  - Interactive 1-tap buttons for Parallel Jobs (`1x`–`5x`), Rclone Multi-Thread Streams (`2`–`16`), and Rclone Buffer Size (`16M`–`128M`), synced live to WZGram's `max_concurrent_transmissions` and saved in MongoDB.
- **Midpoint Video Thumbnails & Metadata**:
  - Automatically probes video `(duration, width, height)` via `ffprobe` and extracts a `<= 320×320` JPEG thumbnail from the exact middle (`duration / 2`) of every video and split part.
- **Auto-Registered Bot Commands**:
  - Automatically registers all slash commands across Default, Private Chat, and Group Chat scopes on startup.

## 🚀 Quick Start (Docker)
1. Copy `config.env.example` to `config.env` and fill in `API_ID`, `API_HASH`, `BOT_TOKEN`, `OWNER_ID`, `DUMP_CHAT_ID`, and `MONGO_URI`.
2. Build and launch:
   ```bash
   docker compose up -d --build
   ```
3. Send `/start` to your bot in Telegram, upload your `rclone.conf` via **🔧 Upload rclone.conf**, and browse your remotes with **🗂 Browse Remotes**!
