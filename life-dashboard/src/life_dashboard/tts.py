"""Text-to-Speech service for weather reports.

Supports:
  - Google gTTS (default, no API key required)
  - OpenAI-compatible REST API (for use with local TTS servers or OpenAI)

The TTS service takes the `tts_script` field from a MeteorologyReport and
generates spoken audio. Audio is cached keyed by the script text so repeated
requests are fast.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
from typing import Literal

import structlog

from gtts import gTTS

log = structlog.get_logger(__name__)

# ── Types ─────────────────────────────────────────────────────────────────────

TTSProvider = Literal["google", "openai"]


# ── Config ────────────────────────────────────────────────────────────────────

def _load_tts_config() -> dict:
    """Load TTS configuration from the dashboard config system."""
    try:
        from life_dashboard.config import load_config

        cfg = load_config()
        return cfg.get("tts", {})
    except Exception as exc:
        log.warn("tts.config_load_failed", error=str(exc))
        return {}


def _get_cache_dir() -> Path:
    """Resolve and ensure the TTS cache directory exists."""
    cfg = _load_tts_config()
    raw = cfg.get("cache_dir", "~/.cache/life-dashboard/tts")
    path = Path(os.path.expanduser(raw))
    path.mkdir(parents=True, exist_ok=True)
    return path


# ── gTTS generation ────────────────────────────────────────────────────────────

def _build_gtts(text: str, lang: str = "en-US", speed: float = 1.0) -> bytes:
    """Generate audio bytes using Google gTTS.

    speed is a multiplier (0.5–2.0) passed as the `speed` parameter to gTTS.
    gTTS normalises the rate; we clamp to the range gTTS accepts.
    """
    clamped_speed = max(0.5, min(2.0, speed))
    tts = gTTS(text=text, lang=lang, slow=(clamped_speed < 0.8))
    import io

    buf = io.BytesIO()
    tts.write_to_fp(buf)
    buf.seek(0)
    return buf.read()


# ── OpenAI-compatible API generation ─────────────────────────────────────────

async def _build_openai_tts(
    text: str,
    api_key: str,
    base_url: str,
    model: str,
    voice: str,
    speed: float = 1.0,
) -> bytes:
    """Generate audio bytes via an OpenAI-compatible REST API.

    Falls back to gTTS if the request fails.
    """
    import httpx

    payload = {
        "model": model,
        "input": text,
        "voice": voice,
        "speed": speed,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{base_url.rstrip('/')}/audio/speech",
                json=payload,
                headers=headers,
            )
            resp.raise_for_status()
            return resp.content
    except Exception as exc:
        log.warn("tts.openai_failed", error=str(exc), falling_back="gtts")
        # Fall back to gTTS silently
        return _build_gtts(text)


# ── Cache ─────────────────────────────────────────────────────────────────────

def _cache_key(text: str, provider: str, voice: str, speed: float) -> str:
    """Stable cache key for a TTS request."""
    raw = f"{provider}:{voice}:{speed}:{text}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ── Public API ─────────────────────────────────────────────────────────────────

async def synthesize_speech(
    text: str,
    provider: TTSProvider | None = None,
    voice: str | None = None,
    speed: float | None = None,
) -> bytes:
    """
    Synthesize speech from *text* and return raw MP3 audio bytes.

    Uses the configured TTS provider and caches results keyed by text hash
    so repeated syntheses are instant.

    Args:
        text: The script to speak.
        provider: Override the configured provider ("google" or "openai").
        voice: Override the configured voice/language.
        speed: Override the configured speed multiplier.

    Returns:
        Raw MP3 audio bytes.
    """
    if not text or not text.strip():
        raise ValueError("Cannot synthesize empty text")

    cfg = _load_tts_config()

    if not cfg.get("enabled", True):
        raise RuntimeError("TTS is disabled in config")

    resolved_provider: TTSProvider = provider or cfg.get("provider", "google")
    resolved_speed = speed or cfg.get("speed", 1.0)

    if resolved_provider == "openai":
        resolved_voice = voice or cfg.get("openai_voice", cfg.get("voice", "echo"))
    else:
        resolved_voice = voice or cfg.get("voice", "en-US")

    cache_dir = _get_cache_dir()
    cache_key = _cache_key(text, resolved_provider, resolved_voice, resolved_speed)
    cache_file = cache_dir / f"{cache_key}.mp3"

    # Serve from cache if available
    if cache_file.exists():
        log.debug("tts.cache_hit", key=cache_key)
        return cache_file.read_bytes()

    log.info("tts.synthesizing", provider=resolved_provider, voice=resolved_voice,
             speed=resolved_speed, char_count=len(text))

    if resolved_provider == "google":
        audio_bytes = _build_gtts(text, lang=resolved_voice, speed=resolved_speed)
    elif resolved_provider == "openai":
        api_key = os.environ.get("TTS_OPENAI_API_KEY", "")
        base_url = cfg.get("base_url", os.environ.get("TTS_OPENAI_BASE_URL", ""))
        model = cfg.get("model", "tts-1")
        openai_voice = cfg.get("openai_voice", "echo")
        if not api_key:
            log.warn("tts.openai_no_key", falling_back="gtts")
            audio_bytes = _build_gtts(text, lang=resolved_voice, speed=resolved_speed)
        else:
            audio_bytes = await _build_openai_tts(
                text, api_key=api_key, base_url=base_url,
                model=model, voice=openai_voice, speed=resolved_speed,
            )
    else:
        raise ValueError(f"Unknown TTS provider: {resolved_provider}")

    # Write to cache
    cache_file.write_bytes(audio_bytes)
    log.info("tts.cached", cache_file=str(cache_file), bytes=len(audio_bytes))

    return audio_bytes


def clear_cache() -> int:
    """Remove all cached TTS audio files. Returns count of files removed."""
    cache_dir = _get_cache_dir()
    files = list(cache_dir.glob("*.mp3"))
    for f in files:
        f.unlink()
    log.info("tts.cache_cleared", count=len(files))
    return len(files)
