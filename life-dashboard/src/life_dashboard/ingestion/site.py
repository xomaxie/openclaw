"""Site report ingestion pipeline — MaxsComputers.com analytics + iflow → SiteReport."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import structlog

from life_dashboard.reports import SiteReport, SiteAnalytics, SiteCalls

log = structlog.get_logger(__name__)


# ── Plausible Analytics ───────────────────────────────────────────────────────

async def fetch_plausible_analytics(
    analytics_url: str,
    api_key: str,
    site_domain: str,
    since_days: int = 1,
) -> SiteAnalytics:
    """
    Fetch site analytics from Plausible API.
    
    API endpoint: GET /api/stats/main
    Docs: https://plausible.io/docs/stats-api
    
    Returns visitors, pageviews, and top pages.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    params = {
        "site_id": site_domain,
        "period": "custom",
        "date": f"{since_days}d",  # last N days
        "metrics": "visitors,pageviews",
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Main stats
            resp = await client.get(
                f"{analytics_url}/api/stats/main",
                headers=headers,
                params=params,
            )
            if resp.status_code != 200:
                log.warn("plausible.api_error", status=resp.status_code, body=resp.text[:200])
                return SiteAnalytics()

            data = resp.json()
            visitors = data.get("results", [{}])[0].get("visitors", 0) if data.get("results") else 0
            pageviews = data.get("results", [{}])[0].get("pageviews", 0) if data.get("results") else 0

            # Top pages
            page_params = {
                "site_id": site_domain,
                "period": "custom",
                "date": f"{since_days}d",
                "metrics": "pageviews",
                "dimension": "event:page",
                "limit": 5,
            }
            page_resp = await client.get(
                f"{analytics_url}/api/stats/main",
                headers=headers,
                params=page_params,
            )
            top_pages: list[str] = []
            if page_resp.status_code == 200:
                page_data = page_resp.json()
                for item in page_data.get("results", []):
                    top_pages.append(item.get("page", ""))

            log.info("plausible.fetched", visitors=visitors, pageviews=pageviews, top_pages=top_pages)
            return SiteAnalytics(
                visitors=visitors,
                pageviews=pageviews,
                top_pages=top_pages,
            )
    except Exception as exc:
        log.error("plausible.fetch_error", error=str(exc))
        return SiteAnalytics()


# ── iflow Integration ──────────────────────────────────────────────────────────
#
# INTERFACE NEEDED FOR IFFLOW:
# 
# The iflow integration is TBD — Max needs to specify the API.
# Based on the ARCHITECTURE.md, iflow provides call log data.
# 
# Expected interface once specified:
# 
# ```python
# async def fetch_iflow_calls(
#     api_url: str,
#     api_key: str,
#     since_days: int = 1,
# ) -> SiteCalls:
#     """
#     Fetch call logs from iflow API.
#     
#     PUT /v1/calls/list
#     Headers: Authorization: Bearer <api_key>
#     Body: {
#       "from": "<YYYY-MM-DD>",
#       "to": "<YYYY-MM-DD>",
#       "filters": {}
#     }
#     
#     Expected response:
#     {
#       "total": 10,
#       "missed": 2,
#       "calls": [
#         {
#           "id": "...",
#           "duration": 240,
#           "status": "completed|missed|voicemail",
#           "timestamp": "2026-03-23T10:30:00Z"
#         }
#       ]
#     }
#     """
# 
# To implement when API spec is available:
# 1. Add `iflow_url`, `iflow_api_key_env` to config
# 2. Uncomment and implement `fetch_iflow_calls` below
# 3. Add iflow data to SiteReport in run_site_pipeline
# ```
#
# Placeholder until iflow API is specified:
async def fetch_iflow_calls_placeholder(since_days: int = 1) -> SiteCalls:
    """
    Placeholder for iflow call data.
    
    TODO: Replace with actual iflow API integration once Max specifies the API.
    
    Expected iflow integration:
    - API endpoint for fetching call logs
    - Authentication via API key (Authorization: Bearer header)
    - Filter by date range (last N days)
    - Return: total calls, missed calls, average duration
    
    Will implement when iflow API documentation/spec is provided.
    """
    log.info("iflow.placeholder_called", message="iflow integration not yet implemented")
    return SiteCalls(total=0, missed=0, avg_duration_sec=0.0)


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def run_site_pipeline(
    db=None,
) -> SiteReport:
    """
    Full site report ingestion pipeline.
    1. Fetch analytics from Plausible (MaxsComputers.com)
    2. Fetch call data from iflow (placeholder — awaiting API spec)
    3. Persist to DB (if db provided)
    4. Update dashboard_state (if db provided)
    Returns the report.
    """
    import os
    log.info("site.pipeline.start")

    from life_dashboard.config import load_config
    cfg = load_config()

    site_domain = "maxscomputers.com"
    analytics_url = cfg.get("site", {}).get("analytics_url", "https://plausible.maxscomputers.com")
    analytics_key_env = cfg.get("site", {}).get("analytics_api_key_env", "PLAUSIBLE_API_KEY")
    analytics_key = os.environ.get(analytics_key_env, "")

    # iflow config (placeholder)
    iflow_url = cfg.get("site", {}).get("iflow_url", "")
    iflow_key_env = "IFLOW_API_KEY"  # TODO: Add to config when specified

    # Fetch analytics and calls concurrently
    analytics_task = fetch_plausible_analytics(
        analytics_url=analytics_url,
        api_key=analytics_key,
        site_domain=site_domain,
        since_days=1,
    )
    calls_task = fetch_iflow_calls_placeholder(since_days=1)

    analytics, calls = await analytics_task, await calls_task

    report_date = datetime.now(timezone.utc).date()
    report = SiteReport(
        type="site",
        site=site_domain,
        date=report_date,
        analytics=analytics,
        calls=calls,
        summary=_build_site_summary(analytics, calls),
    )

    if db:
        run_id = db.start_run("site")
        try:
            report_id = db.insert_report(
                rtype="site",
                title=f"Site Report — {site_domain} — {report_date}",
                content=report.model_dump(),
                generated_at=datetime.now(timezone.utc),
                summary=report.summary,
                source_tag="site",
                raw_inputs=[
                    f"plausible:{site_domain}",
                    "iflow:placeholder",
                ],
            )
            db.set_state("current_site", report.model_dump())
            db.set_state("latest_site_report_id", report_id)
            db.finish_run(run_id, "success", report_id)
            log.info("site.pipeline.done", report_id=report_id)
        except Exception as exc:
            db.finish_run(run_id, "error", error=str(exc))
            raise
    else:
        log.info("site.pipeline.done", db=False)

    return report


def _build_site_summary(analytics: SiteAnalytics, calls: SiteCalls) -> str:
    """Build a human-readable summary from analytics + calls."""
    parts = []
    if analytics.visitors > 0:
        parts.append(f"{analytics.visitors} visitors and {analytics.pageviews} pageviews")
    if calls.total > 0:
        missed_str = f", {calls.missed} missed" if calls.missed > 0 else ", none missed"
        parts.append(f"{calls.total} calls{missed_str}")

    if not parts:
        return "No site data available yet."

    return f"Strong traffic day. " + ". ".join(parts) + "."
