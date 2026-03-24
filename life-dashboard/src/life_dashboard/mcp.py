"""MCP server exposing Life Dashboard tools to the LLM assistant (astra)."""

from __future__ import annotations

import json
import os
import uuid
from typing import Any

import structlog
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from life_dashboard.config import load_config
from life_dashboard.db import Database

log = structlog.get_logger(__name__)

# ── Server Bootstrap ────────────────────────────────────────────────────────────

def _make_server() -> tuple[Server, Database]:
    config = load_config(os.environ.get("DASHBOARD_CONFIG_PATH"))
    db_path = config.get("database", {}).get("path", "~/.local/share/life-dashboard/life-dashboard.db")
    db = Database(db_path)
    server = Server("life-dashboard")
    return server, db


# ── Tool Definitions ────────────────────────────────────────────────────────────

TOOLS: list[Tool] = [
    Tool(
        name="get_dashboard",
        description="Returns the full current dashboard state as a JSON object.",
        inputSchema={
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_latest_report",
        description="Returns the most recent report of a given type (meteorology, daily, or site).",
        inputSchema={
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["meteorology", "daily", "site"],
                    "description": "Report type",
                }
            },
            "required": ["type"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_reports",
        description="Returns a list of stored reports with optional filters.",
        inputSchema={
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["meteorology", "daily", "site"],
                    "description": "Filter by report type",
                },
                "limit": {
                    "type": "integer",
                    "default": 10,
                    "description": "Maximum number of reports to return",
                },
            },
            "additionalProperties": False,
        },
    ),
    Tool(
        name="trigger_ingest",
        description="Triggers a named ingestion pipeline (weather, daily, or site). Returns confirmation.",
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline": {
                    "type": "string",
                    "enum": ["weather", "daily", "site"],
                    "description": "Pipeline name to trigger",
                },
                "location": {
                    "type": "string",
                    "description": "For weather pipeline: location string (e.g. 'Austin,TX')",
                },
            },
            "required": ["pipeline"],
            "additionalProperties": False,
        },
    ),
    Tool(
        name="get_ingestion_status",
        description="Returns the status of recent ingestion runs.",
        inputSchema={
            "type": "object",
            "properties": {
                "pipeline": {
                    "type": "string",
                    "enum": ["meteorology", "daily", "site"],
                    "description": "Filter by pipeline",
                },
                "limit": {
                    "type": "integer",
                    "default": 5,
                },
            },
            "additionalProperties": False,
        },
    ),
]


# ── Tool Handlers ─────────────────────────────────────────────────────────────

async def handle_get_dashboard(db: Database) -> TextContent:
    state = db.get_all_state()
    return TextContent(type="text", text=json.dumps(state, indent=2, default=str))


async def handle_get_latest_report(db: Database, rtype: str) -> TextContent:
    r = db.get_latest_report(rtype)
    if not r:
        return TextContent(type="text", text=f"No {rtype} report found.")
    return TextContent(type="text", text=json.dumps(r, indent=2, default=str))


async def handle_get_reports(db: Database, rtype: str | None = None, limit: int = 10) -> TextContent:
    reports = db.get_reports(rtype=rtype, limit=limit)
    return TextContent(type="text", text=json.dumps({"reports": reports, "count": len(reports)}, indent=2, default=str))


async def handle_trigger_ingest(db: Database, pipeline: str, location: str | None = None) -> TextContent:
    if pipeline == "weather":
        from life_dashboard.ingestion.weather import run_weather_pipeline
        loc = location or "Austin,TX"
        try:
            report = await run_weather_pipeline(location=loc, db=db)
            return TextContent(type="text", text=f"Weather ingestion complete. {report.summary}")
        except Exception as exc:
            return TextContent(type="text", text=f"Weather ingestion failed: {exc}")
    elif pipeline in ("daily", "site"):
        return TextContent(type="text", text=f"[TODO] {pipeline} ingestion not yet implemented.")
    else:
        return TextContent(type="text", text=f"Unknown pipeline: {pipeline}")


async def handle_get_ingestion_status(db: Database, pipeline: str | None = None, limit: int = 5) -> TextContent:
    runs = db.get_recent_runs(pipeline=pipeline, limit=limit)
    return TextContent(type="text", text=json.dumps({"runs": runs, "count": len(runs)}, indent=2))


# ── Server Setup ───────────────────────────────────────────────────────────────

def run_mcp_server() -> None:
    """Run the MCP server using stdio transport."""
    import asyncio
    server, db = _make_server()

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return TOOLS

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            match name:
                case "get_dashboard":
                    return [await handle_get_dashboard(db)]
                case "get_latest_report":
                    return [await handle_get_latest_report(db, arguments["type"])]
                case "get_reports":
                    return [await handle_get_reports(db, arguments.get("type"), arguments.get("limit", 10))]
                case "trigger_ingest":
                    return [await handle_trigger_ingest(db, arguments["pipeline"], arguments.get("location"))]
                case "get_ingestion_status":
                    return [await handle_get_ingestion_status(db, arguments.get("pipeline"), arguments.get("limit", 5))]
                case _:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]
        except Exception as exc:
            log.error("mcp.tool_error", tool=name, error=str(exc))
            return [TextContent(type="text", text=f"Error: {exc}")]

    async def main() -> None:
        options = server.create_initialization_options()
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, options, raise_exceptions=True)

    asyncio.run(main())
