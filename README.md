# Rclone ➔ Telegram Transfer Bot (WZGram / Kurigram + MongoDB Edition)

A fast, lightweight, async Telegram bot powered by **[WZGram](https://wzgram.com/)** (with Rust `WarpCrypto`) or **[Kurigram](https://docs.kurigram.icu/)** (`kurigram[fast]`) that downloads media from any [rclone](https://rclone.org)-supported remote and uploads it straight to Telegram with **MongoDB config & `rclone.conf` persistence**, **automatic job resumption on restart**, **auto-registered commands**, **visual folder browsing**, and an **optimized multi-stage Docker image**.

## Switching Between WZGram and Kurigram

`bot.py` features a **Dual MTProto Engine Loader** that automatically detects and runs whichever library is installed (`wzgram` or `kurigram`), including automatic `ReplyParameters` compatibility:

- **Default (`WZGram` + Rust `WarpCrypto`)**:
  ```bash
  pip install -r requirements.txt
  # or via Docker:
  docker compose build --build-arg MTPROTO_LIB="wzgram"
  ```
- **Alternative (`Kurigram` + `TgCrypto`)**:
  Uncomment `kurigram[fast]` (and comment out `wzgram`) in `requirements.txt`, or pass the Docker build argument:
  ```bash
  docker compose build --build-arg MTPROTO_LIB="kurigram[fast]"
  ```

## Highlights

- **🚀 WZGram & Kurigram Native Support** — Uses modern `ReplyParameters(message_id=...)` with automatic signature inspection and displays the active MTProto engine (`WZGram` or `Kurigram`) in `/start`, `/status`, and `/health`.
- **🗄 Full MongoDB Persistence (`MONGO_URI`)** — Saves all runtime settings (`Auto-Delete`, `Size Cap`, `Concurrency`, `BW Limit`, `Refresh Rate`, `Default Mode`, `Authorized Users`), lifetime transfer statistics, and your uploaded **`rclone.conf`** directly in MongoDB (`motor` async driver).
- **▶️ Auto-Start / Auto-Resume on Bot Restart** — Active transfer jobs checkpoint their progress to MongoDB after every file. If the bot restarts or redeploys mid-job, it automatically resumes remaining files on startup without re-uploading completed ones.
- **📲 Automatic Bot Command Registration** — Calls `set_bot_commands` across Default, Private Chat, and Group scopes automatically on startup so the `/` command menu is always populated without `@BotFather`.
- **🗂 Interactive Remote & Folder Browser (`/remotes`)** — Navigate cloud remotes and subfolders with inline pagination buttons and start transfers with one tap.
- **🎛 Interactive Control Panels (`/settings` & `/db`)** — Configure every setting or trigger database syncs/restores using inline buttons.
