# ☁️ Rclone ➔ Telegram Transfer Bot (WZGram + MongoDB Edition)

High-speed, pipelined Rclone-to-Telegram transfer bot powered by **WZGram** (Rust `WarpCrypto`) and **Motor MongoDB** where **100% of all configuration, remotes, file browsing, speed tuning, and job controls are managed directly inside the Telegram Bot UI**.

---

## ✨ Key Features (100% Controlled from Bot)

1. **🎯 File Types Filter (`Videos`, `Images`, `Documents`)**
   - Switch between **🎬 Videos Only**, **🖼 Images Only**, **📄 Documents Only**, **🎞 Video + Image**, or **📦 All Types** with 1 tap, or toggle individual categories on/off.
2. **🗂 Interactive Remote, Folder & Single-File Browser**
   - Browse all configured Rclone remotes (`☁️`), navigate into subfolders (`📁`), or tap **📄 View Files** to inspect individual files with sizes and transfer or delete any single file with 1 tap.
3. **⚡ Live Speed & Concurrency Tuning**
   - Switch between **🛡 Safe (`1x · 2w · 4s`)**, **⚡ Fast (`2x · 4w · 8s`)**, and **🚀 Turbo (`3x · 6w · 16s`)** presets, or tune **Parallel Jobs (`1x–5x`)**, **Upload Workers (`2w–8w`)**, **Rclone Streams (`4s–16s`)**, and **Buffer Size (`32M–256M`)** directly from inline buttons.
4. **📢 Interactive Dump Chat / Channel Manager**
   - Change the upload destination anytime from the bot: tap **📍 Use This Chat**, **👤 Use Owner DM**, **🔔 Test Upload Chat**, or **✏️ Enter ID / Forward Msg** (forward any message from your target channel to set it automatically).
5. **🔧 Full `rclone.conf` Manager inside Telegram**
   - Upload a `.conf` document, **paste `[remote]` config text directly in chat**, download your active `rclone.conf` for backup, or delete individual remotes with 1 tap.
6. **⚙️ Complete Control Panel in Telegram**
   - Toggle **Auto-Delete Remote**, **Auto-Resume Jobs**, **Default Upload Mode (`Media` vs `Document`)**, **Max File Size Cap** (presets + custom MB prompt), **Split Threshold (`500M–2000M`)**, **Bandwidth Limit**, **Rclone Extra Flags** (1-tap popular flags + custom input), and **Authorized Users** (`➕ Add` / `🗑 Remove` buttons).
7. **🗄 MongoDB Persistence + Auto-Resume**
   - All settings, `rclone.conf`, transfer statistics, and active job progress checkpoints are synced to MongoDB (with automatic local JSON fallback) so interrupted transfers resume seamlessly after a restart.
