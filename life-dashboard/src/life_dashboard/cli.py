"""Life Dashboard CLI — Typer-based command interface."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import structlog
import typer
from typing import Optional
from typer import echo as typer_echo, secho as typer_secho

from life_dashboard import __version__
from life_dashboard.config import load_config
from life_dashboard.db import Database
from life_dashboard.ingestion.weather import run_weather_pipeline
from life_dashboard.ingestion.daily import run_daily_pipeline
from life_dashboard.ingestion.site import run_site_pipeline
from life_dashboard.logging_ import setup_logging

log = structlog.get_logger(__name__)

# ── Helpers ─────────────────────────────────────────────────────────────────────

def _get_db(config: dict) -> Database:
    return Database(config.get("database", {}).get(
        "path", "~/.local/share/life-dashboard/life-dashboard.db"
    ))


def _load_cfg(ctx: typer.Context) -> dict:
    if "config" not in ctx.obj:
        cfg_path = ctx.obj.get("config_path")
        ctx.obj["config"] = load_config(cfg_path)
    return ctx.obj["config"]


# ── App ───────────────────────────────────────────────────────────────────────

app = typer.Typer(add_completion=False, invoke_without_command=True)
ingest_app = typer.Typer()
report_app = typer.Typer()
dashboard_app = typer.Typer()

app.add_typer(ingest_app, name="ingest")
app.add_typer(report_app, name="report")
app.add_typer(dashboard_app, name="dashboard")


# ── Root Callback ─────────────────────────────────────────────────────────────

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Config file path"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Max's Life Dashboard CLI."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config
    cfg = _load_cfg(ctx)
    log_level = "DEBUG" if verbose else cfg.get("logging", {}).get("level", "INFO")
    log_path = cfg.get("logging", {}).get("path")
    setup_logging(level=log_level, log_path=Path(log_path) if log_path else None)


# ── Root Commands ─────────────────────────────────────────────────────────────

@app.command("db:init")
def db_init(ctx: typer.Context) -> None:
    """Initialize the SQLite database schema."""
    _get_db(_load_cfg(ctx)).init_schema()
    typer_echo("Database initialized.")


@app.command("serve")
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind to"),
    port: int = typer.Option(8080, "--port", help="Port to bind to"),
) -> None:
    """Start the Life Dashboard REST API server."""
    import uvicorn
    from life_dashboard.api import app as fastapi_app
    typer_echo(f"Starting Life Dashboard API on {host}:{port} ...")
    uvicorn.run(fastapi_app, host=host, port=port, log_level="info")


@app.command("mcp")
def mcp_cmd(
    action: str = typer.Argument("run", help="Action (only 'run' supported)"),
) -> None:
    """Run the MCP server (for OpenClaw integration)."""
    if action == "run":
        from life_dashboard.mcp import run_mcp_server
        typer_echo("Starting Life Dashboard MCP server...")
        run_mcp_server()
    else:
        typer_secho(f"Unknown MCP action: {action}", fg="red")
        raise typer.Exit(1)


@app.command("config")
def config_cmd(ctx: typer.Context) -> None:
    """Show current config (secrets redacted)."""
    cfg = _load_cfg(ctx)

    def redact(obj: dict) -> dict:
        result = {}
        for k, v in obj.items():
            if any(s in k.lower() for s in ("password", "token", "key", "secret")):
                result[k] = "***REDACTED***"
            elif isinstance(v, dict):
                result[k] = redact(v)
            elif isinstance(v, list):
                result[k] = [redact(i) if isinstance(i, dict) else i for i in v]
            else:
                result[k] = v
        return result

    typer_echo(json.dumps(redact(cfg), indent=2))


@app.command("version")
def version_cmd() -> None:
    """Show version."""
    typer_echo(f"life-dashboard {__version__}")


# ── Ingest Subcommands ────────────────────────────────────────────────────────

@ingest_app.command("weather")
def ingest_weather(
    ctx: typer.Context,
    location: Optional[str] = typer.Option(None, "--location", "-l", help="Location for weather data"),
) -> None:
    """Fetch weather data and generate a meteorology report."""
    cfg = _load_cfg(ctx)
    db = _get_db(cfg)
    loc = location or cfg.get("weather", {}).get("default_location", "Austin,TX")
    try:
        report = asyncio.run(run_weather_pipeline(location=loc, db=db))
        typer_echo(f"[OK] Weather report generated.")
        typer_echo(f"     {report.summary}")
    except Exception as exc:
        typer_secho(f"[ERROR] {exc}", fg="red", err=True)
        raise typer.Exit(1)


@ingest_app.command("daily")
def ingest_daily(
    ctx: typer.Context,
    since_hours: int = typer.Option(24, "--since", help="Hours to look back"),
) -> None:
    """Ingest emails + GitHub issues to generate a daily report."""
    cfg = _load_cfg(ctx)
    db = _get_db(cfg)
    try:
        report = asyncio.run(run_daily_pipeline(db=db, since_hours=since_hours))
        typer_echo(f"[OK] Daily report generated — {report.date}")
        typer_echo(f"     {report.summary}")
    except Exception as exc:
        typer_secho(f"[ERROR] {exc}", fg="red", err=True)
        raise typer.Exit(1)


@ingest_app.command("site")
def ingest_site(ctx: typer.Context) -> None:
    """Ingest MaxsComputers.com analytics and call data."""
    cfg = _load_cfg(ctx)
    db = _get_db(cfg)
    try:
        report = asyncio.run(run_site_pipeline(db=db))
        typer_echo(f"[OK] Site report generated — {report.site} — {report.date}")
        typer_echo(f"     {report.summary}")
    except Exception as exc:
        typer_secho(f"[ERROR] {exc}", fg="red", err=True)
        raise typer.Exit(1)


# ── Report Subcommands ────────────────────────────────────────────────────────

@report_app.command("list")
def report_list(
    ctx: typer.Context,
    rtype: Optional[str] = typer.Option(None, "--type", help="Report type filter"),
    limit: int = typer.Option(10, "--limit", help="Max reports to show"),
) -> None:
    """List stored reports."""
    reports = _get_db(_load_cfg(ctx)).get_reports(rtype=rtype, limit=limit)
    if not reports:
        typer_echo("No reports found.")
        return
    for r in reports:
        typer_echo(f"[{r['type']:15}] {r['generated_at']}  {r['title']}")
        if r.get("summary"):
            typer_echo(f"  {r['summary'][:80]}")


@report_app.command("show")
def report_show(
    ctx: typer.Context,
    report_id: str = typer.Argument(..., help="Report ID"),
) -> None:
    """Display a single report by ID."""
    r = _get_db(_load_cfg(ctx)).get_report(report_id)
    if not r:
        typer_secho(f"Report not found: {report_id}", fg="red")
        raise typer.Exit(1)
    typer_echo(json.dumps(r, indent=2, default=str))


@report_app.command("latest")
def report_latest(
    ctx: typer.Context,
    rtype: str = typer.Option(..., "--type", help="Report type"),
) -> None:
    """Show the most recent report of a given type."""
    r = _get_db(_load_cfg(ctx)).get_latest_report(rtype)
    if not r:
        typer_secho(f"No {rtype} report found.", fg="yellow")
        raise typer.Exit(1)
    typer_echo(json.dumps(r, indent=2, default=str))


# ── Dashboard Subcommands ─────────────────────────────────────────────────────

@dashboard_app.command("state")
def dashboard_state(
    ctx: typer.Context,
    key: Optional[str] = typer.Option(None, "--key", help="Specific state key"),
) -> None:
    """Show current dashboard state."""
    db = _get_db(_load_cfg(ctx))
    if key:
        val = db.get_state(key)
        typer_echo(json.dumps(val, indent=2, default=str) if val else f"Key not found: {key}")
    else:
        typer_echo(json.dumps(db.get_all_state(), indent=2, default=str))


def main() -> None:
    app()
