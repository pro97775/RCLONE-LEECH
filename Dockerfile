# syntax=docker/dockerfile:1

# ── Stage 1: Fetch static rclone binary ───────────────────────────────
FROM rclone/rclone:latest AS rclone_bin

# ── Stage 2: Build Python packages (WZGram + WarpCrypto + Motor) ──────
FROM python:3.11-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install --no-warn-script-location -r requirements.txt

# ── Stage 3: Minimal, lightweight production runtime ──────────────────
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONOPTIMIZE=1 \
    MALLOC_ARENA_MAX=2 \
    RCLONE_CONFIG=/root/.config/rclone/rclone.conf \
    DOWNLOAD_DIR=/tmp/rclone_dl \
    PORT=8080

# Install only runtime essentials (ffmpeg + ca-certificates + taskset)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
        util-linux \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /usr/share/doc/* \
        /usr/share/man/* \
        /usr/share/locale/* \
        /tmp/* \
        /var/tmp/*

# Copy pre-built Python packages and official static rclone binary
COPY --from=builder /install /usr/local
COPY --from=rclone_bin /usr/local/bin/rclone /usr/local/bin/rclone

WORKDIR /app
COPY bot.py .

RUN mkdir -p /root/.config/rclone /tmp/rclone_dl

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; p = os.environ.get('PORT', '8080'); urllib.request.urlopen(f'http://127.0.0.1:{p}/health', timeout=3)"]

CMD ["python", "-O", "bot.py"]
