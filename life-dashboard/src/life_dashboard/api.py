"""FastAPI REST API for Life Dashboard."""

from __future__ import annotations

import logging as stdlib_logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

stdlib_logging.getLogger("uvicorn").setLevel(stdlib_logging.INFO)
log = structlog.get_logger(__name__)

# ── State (lazy init to avoid import-time side effects) ────────────────────────

_api_db = None
_api_config = None

def _get_db():
    global _api_db
    if _api_db is None:
        from life_dashboard.config import load_config
        from life_dashboard.db import Database
        cfg = load_config()
        global _api_config
        _api_config = cfg
        db_path = cfg.get("database", {}).get("path", "~/.local/share/life-dashboard/life-dashboard.db")
        _api_db = Database(db_path)
    return _api_db

def _get_config():
    global _api_config
    if _api_config is None:
        from life_dashboard.config import load_config
        _api_config = load_config()
    return _api_config


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Life Dashboard API",
    version="0.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Static Files ───────────────────────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent / "static"

# Mount static files directory for CSS/JS
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
def root():
    """Serve the dashboard HTML UI."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text())
    return HTMLResponse(content="<h1>Life Dashboard API</h1><p>UI not found. Visit <a href='/docs'>/docs</a> for API documentation.</p>")

@app.get("/weather", response_class=HTMLResponse)
def weather_page():
    """Serve the weather report page."""
    weather_path = STATIC_DIR / "weather.html"
    if weather_path.exists():
        return HTMLResponse(content=weather_path.read_text())
    raise HTTPException(404, "Weather page not found")


# ── Request/Response Models ────────────────────────────────────────────────────

class TriggerIngestRequest(BaseModel):
    location: str | None = None


class TriggerResponse(BaseModel):
    status: str
    message: str
    report_id: str | None = None


class MeteorologyDetailResponse(BaseModel):
    report: dict[str, Any]
    glanceable: dict[str, Any] | None = None
    has_narrative: bool = False
    alert_count: int = 0
    tts_script: str = ""  # Polished on-air TTS script


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "life-dashboard", "version": "0.2.0"}


# ── Reports ───────────────────────────────────────────────────────────────────

@app.get("/api/reports")
def list_reports(
    type: str | None = Query(None, alias="type"),
    limit: int = Query(10, ge=1, le=100),
    since: str | None = None,
) -> dict[str, Any]:
    since_dt = None
    if since:
        since_dt = datetime.fromisoformat(since)
    reports = _get_db().get_reports(rtype=type, limit=limit, since=since_dt)
    return {"reports": reports, "count": len(reports)}


@app.get("/api/reports/{report_id}")
def get_report(report_id: str) -> dict[str, Any]:
    r = _get_db().get_report(report_id)
    if not r:
        raise HTTPException(404, f"Report not found: {report_id}")
    return r


# ── Meteorology Reports ───────────────────────────────────────────────────────

@app.get("/api/meteorology/latest")
def get_latest_meteorology() -> MeteorologyDetailResponse:
    """Get the latest meteorology report with full details."""
    db = _get_db()
    report = db.get_latest_report("meteorology")
    if not report:
        raise HTTPException(404, "No meteorology report found")
    
    content = report.get("content", {})
    alerts = content.get("alerts", [])
    
    # Extract glanceable data
    glanceable = None
    if content.get("current"):
        glanceable = {
            "temp_f": content["current"].get("temp_f"),
            "condition": content["current"].get("condition"),
            "condition_icon": _condition_to_icon(content["current"].get("condition", "")),
            "high_f": content.get("forecast", [{}])[0].get("high_f") if content.get("forecast") else None,
            "low_f": content.get("forecast", [{}])[0].get("low_f") if content.get("forecast") else None,
            "precip_chance": content.get("forecast", [{}])[0].get("precip_pct") if content.get("forecast") else None,
            "alert_count": len(alerts),
            "alert_severity": _max_alert_severity(alerts),
        }
    
    return MeteorologyDetailResponse(
        report=report,
        glanceable=glanceable,
        has_narrative=bool(content.get("narrative")),
        alert_count=len(alerts),
        tts_script=content.get("tts_script", ""),
    )


@app.get("/api/meteorology/history")
def get_meteorology_history(
    limit: int = Query(24, ge=1, le=168),  # Default 24 hours, max 1 week
) -> dict[str, Any]:
    """Get meteorology report history for trend analysis."""
    since = datetime.now(timezone.utc) - timedelta(hours=limit)
    
    reports = _get_db().get_reports(rtype="meteorology", limit=limit, since=since)
    
    # Extract trend data
    trend_data = []
    for r in reports:
        content = r.get("content", {})
        if content.get("current"):
            trend_data.append({
                "timestamp": r.get("generated_at"),
                "temp_f": content["current"].get("temp_f"),
                "humidity_pct": content["current"].get("humidity_pct"),
                "condition": content["current"].get("condition"),
            })
    
    return {
        "reports": reports,
        "trend_data": trend_data,
        "count": len(reports),
    }


@app.get("/api/meteorology/tts", response_class=StreamingResponse)
async def get_meteorology_tts() -> StreamingResponse:
    """
    Synthesize and stream the on-air TTS script for the latest weather report.

    Returns MP3 audio streaming from the configured TTS provider (gTTS by default).
    Falls back gracefully if TTS is disabled or synthesis fails.
    """
    from fastapi.responses import StreamingResponse

    db = _get_db()
    report = db.get_latest_report("meteorology")
    if not report:
        raise HTTPException(404, "No meteorology report found")

    content = report.get("content", {})
    tts_script = content.get("tts_script", "")
    if not tts_script:
        raise HTTPException(422, "No TTS script available for this report")

    try:
        from life_dashboard.tts import synthesize_speech

        audio_bytes = await synthesize_speech(tts_script)
    except RuntimeError as exc:
        if "disabled" in str(exc).lower():
            raise HTTPException(503, "TTS is disabled in configuration")
        raise HTTPException(500, str(exc))
    except Exception as exc:
        log.error("tts.synthesis_failed", error=str(exc))
        raise HTTPException(500, f"TTS synthesis failed: {exc}")

    import io

    return StreamingResponse(
        io.BytesIO(audio_bytes),
        media_type="audio/mpeg",
        headers={
            "Content-Disposition": "inline; filename=\"weather-report.mp3\"",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


def _condition_to_icon(condition: str) -> str:
    """Map condition to emoji icon."""
    c = condition.lower()
    if any(w in c for w in ["sun", "clear", "fair"]):
        return "☀️"
    if "mostly" in c or "mainly" in c:
        return "🌤️"
    if "partly" in c:
        return "⛅"
    if "cloud" in c or "overcast" in c:
        return "☁️"
    if "rain" in c or "drizzle" in c or "shower" in c:
        return "🌧️"
    if "storm" in c or "thunder" in c:
        return "⛈️"
    if "snow" in c or "sleet" in c or "ice" in c:
        return "❄️"
    if "fog" in c or "mist" in c:
        return "🌫️"
    if "wind" in c:
        return "💨"
    return "🌤️"


def _max_alert_severity(alerts: list[dict]) -> str:
    """Get maximum severity from alerts."""
    severity_order = {"none": 0, "minor": 1, "moderate": 2, "severe": 3, "extreme": 4}
    max_sev = "none"
    for alert in alerts:
        sev = alert.get("severity", "minor")
        if severity_order.get(sev, 0) > severity_order.get(max_sev, 0):
            max_sev = sev
    return max_sev


# ── Dashboard State ───────────────────────────────────────────────────────────

@app.get("/api/dashboard")
def get_dashboard() -> dict[str, Any]:
    return _get_db().get_all_state()


@app.get("/api/dashboard/{key}")
def get_dashboard_key(key: str) -> dict[str, Any]:
    val = _get_db().get_state(key)
    if val is None:
        raise HTTPException(404, f"Dashboard key not found: {key}")
    return {"key": key, "value": val}


# ── Ingestion ─────────────────────────────────────────────────────────────────

@app.post("/api/ingest/{pipeline}", response_model=TriggerResponse)
async def trigger_ingest(pipeline: str, body: TriggerIngestRequest | None = None) -> TriggerResponse:
    db = _get_db()
    cfg = _get_config()

    if pipeline == "weather":
        location = body.location if body and body.location else cfg.get("weather", {}).get("default_location", "Austin,TX")
        try:
            report = await run_weather_pipeline(location=location, db=db)
            report_id = db.get_latest_report("meteorology")
            return TriggerResponse(
                status="success",
                message=report.get_summary(),
                report_id=report_id["id"] if report_id else None,
            )
        except Exception as exc:
            raise HTTPException(500, str(exc))
    elif pipeline == "daily":
        try:
            report = await run_daily_pipeline(db=db)
            report_id = db.get_latest_report("daily")
            return TriggerResponse(
                status="success",
                message=report.summary,
                report_id=report_id["id"] if report_id else None,
            )
        except Exception as exc:
            raise HTTPException(500, str(exc))
    elif pipeline == "site":
        try:
            report = await run_site_pipeline(db=db)
            report_id = db.get_latest_report("site")
            return TriggerResponse(
                status="success",
                message=report.summary,
                report_id=report_id["id"] if report_id else None,
            )
        except Exception as exc:
            raise HTTPException(500, str(exc))
    else:
        raise HTTPException(400, f"Unknown pipeline: {pipeline}")


# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
def get_logs(
    pipeline: str | None = None,
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    runs = _get_db().get_recent_runs(pipeline=pipeline, limit=limit)
    return {"runs": runs, "count": len(runs)}


# ── Late imports for pipelines ─────────────────────────────────────────────────

async def run_weather_pipeline(location: str, db):
    from life_dashboard.ingestion.weather import run_weather_pipeline as _run
    return await _run(location=location, db=db)

async def run_daily_pipeline(db):
    from life_dashboard.ingestion.daily import run_daily_pipeline as _run
    return await _run(db=db)

async def run_site_pipeline(db):
    from life_dashboard.ingestion.site import run_site_pipeline as _run
    return await _run(db=db)
