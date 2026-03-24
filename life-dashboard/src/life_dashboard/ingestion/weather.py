"""Enhanced weather ingestion pipeline — fetches from multiple sources: wttr.in, Open-Meteo, NWS/weather.gov, and PWS (Synoptic)."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from life_dashboard.reports import (
    CurrentWeather,
    DayForecast,
    HourlyForecast,
    MeteorologyReport,
    PWSLayerSummary,
    WeatherAlert,
    WeatherSourceData,
    RegionalSignal,
    METARLayerSummary,
    METARStationObservation,
    AFDSummary,
    SPCOutlookSummary,
    SPCRiskArea,
)
from life_dashboard.ingestion.pws import (
    fetch_pws_layer,
    blend_pws_into_current,
)

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_LAT = 30.2672  # Austin, TX
DEFAULT_LON = -97.7431
DEFAULT_LOCATION = "Austin,TX"

# NWS grid for Austin (approximate)
NWS_OFFICE = "EWX"  # Austin/San Antonio NWS office
NWS_GRID_X = 156
NWS_GRID_Y = 90


# ── Source Fetchers ───────────────────────────────────────────────────────────

async def fetch_wttr(location: str) -> dict[str, Any]:
    """Fetch current conditions + 3-day forecast from wttr.in."""
    url = f"https://wttr.in/{location}?format=j1"
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()


async def fetch_open_meteo(
    lat: float = DEFAULT_LAT,
    lon: float = DEFAULT_LON,
    forecast_days: int = 3,
) -> dict[str, Any]:
    """Fetch extended weather data from Open-Meteo (free, no key)."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,uv_index,wind_speed_10m,wind_direction_10m,weather_code,pressure_msl,visibility",
        "hourly": "temperature_2m,precipitation_probability,weather_code,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,weather_code",
        "timezone": "America/Chicago",
        "forecast_days": forecast_days,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def fetch_nws_alerts(
    lat: float = DEFAULT_LAT,
    lon: float = DEFAULT_LON,
) -> list[WeatherAlert]:
    """Fetch active weather alerts from NWS API for given coordinates."""
    try:
        url = f"https://api.weather.gov/alerts/active"
        params = {
            "point": f"{lat},{lon}",
        }
        headers = {
            "User-Agent": "LifeDashboard/0.2.0 (personal weather dashboard)",
            "Accept": "application/geo+json",
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 404:
                # No alerts for this location
                return []
            resp.raise_for_status()
            data = resp.json()
            
        alerts = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            severity = props.get("severity", "Unknown").lower()
            # Map NWS severity to our enum
            severity_map = {
                "minor": "minor",
                "moderate": "moderate",
                "severe": "severe",
                "extreme": "extreme",
            }
            mapped_severity = severity_map.get(severity, "minor")
            
            alerts.append(WeatherAlert(
                title=props.get("event", "Weather Alert"),
                severity=mapped_severity,  # type: ignore
                description=props.get("headline", props.get("description", "")[:200]),
                effective=props.get("effective"),
                expires=props.get("expires"),
                instruction=props.get("instruction", ""),
                source="NWS",
                url=props.get("id"),
            ))
        
        log.info("nws.alerts_fetched", count=len(alerts))
        return alerts
    except Exception as exc:
        log.warn("nws.alerts_failed", error=str(exc))
        return []


async def fetch_nws_forecast(
    lat: float = DEFAULT_LAT,
    lon: float = DEFAULT_LON,
) -> dict[str, Any] | None:
    """Fetch forecast from NWS API (US only)."""
    try:
        # First get the grid point
        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        headers = {
            "User-Agent": "LifeDashboard/0.2.0 (personal weather dashboard)",
            "Accept": "application/geo+json",
        }
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            points_resp = await client.get(points_url, headers=headers)
            points_resp.raise_for_status()
            points_data = points_resp.json()
            
            # Get forecast URL from points response
            props = points_data.get("properties", {})
            forecast_url = props.get("forecast")
            hourly_forecast_url = props.get("forecastHourly")
            
            if not forecast_url:
                return None
            
            # Fetch forecast
            forecast_resp = await client.get(forecast_url, headers=headers)
            forecast_resp.raise_for_status()
            forecast_data = forecast_resp.json()
            
            # Fetch hourly forecast if available
            hourly_data = None
            if hourly_forecast_url:
                hourly_resp = await client.get(hourly_forecast_url, headers=headers)
                if hourly_resp.status_code == 200:
                    hourly_data = hourly_resp.json()
            
            return {
                "forecast": forecast_data,
                "hourly": hourly_data,
                "grid": props,
            }
    except Exception as exc:
        log.warn("nws.forecast_failed", error=str(exc))
        return None


# ── METAR / Aviation Observations ─────────────────────────────────────────────

# Pre-defined nearby station sets keyed by WFO (NWS office)
# Covers Philadelphia, MS (32.77, -89.12) area
# JAN = Jackson, MS WFO covers most of central/eastern Mississippi
_METAR_STATION_SETS = {
    "JAN": ["KJAN", "KMEI", "KHEZ", "KMCB"],  # Jackson, Meridian, Natchez, McComb
    "EWX": ["KATT", "KAUS", "KHDO", "KSSF"],  # Austin area fallback
    "MEG": ["KMEM", "KTUP", "KNQA", "KHAB"],  # Memphis area
}
# Default stations if WFO not in map
_DEFAULT_METAR_STATIONS = ["KJAN", "KMEI"]


async def fetch_metar_layer(
    lat: float,
    lon: float,
    wfo_office: str | None = None,
) -> METARLayerSummary:
    """Fetch latest METAR/ASOS observations from nearby NWS stations.

    Uses the NWS stations API (api.weather.gov) which returns official
    aviation observations for ASOS/ATCT stations.
    """
    summary = METARLayerSummary(
        enabled=True,
        target_latitude=lat,
        target_longitude=lon,
    )

    # Determine which stations to query
    station_list = _METAR_STATION_SETS.get(wfo_office or "", _DEFAULT_METAR_STATIONS)
    headers = {
        "User-Agent": "LifeDashboard/0.2.0 (personal weather dashboard)",
        "Accept": "application/geo+json",
    }

    import math

    def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 3956.0
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlam = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
        return R * 2 * math.asin(math.sqrt(a))

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            tasks = [
                client.get(
                    f"https://api.weather.gov/stations/{stn}/observations/latest",
                    headers=headers,
                )
                for stn in station_list
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        observations: list[METARStationObservation] = []
        temps: list[float] = []
        humidities: list[float] = []
        winds: list[float] = []
        visibilities: list[float] = []

        for stn, resp in zip(station_list, responses):
            if isinstance(resp, Exception):
                log.warn("metar.station_failed", station=stn, error=str(resp))
                continue
            if resp.status_code != 200:
                log.warn("metar.station_status", station=stn, status=resp.status_code)
                continue

            summary.stations_responded += 1
            data = resp.json()
            props = data.get("properties", {})

            # Parse temperature
            temp_c = props.get("temperature", {}).get("value")
            temp_f = round(temp_c * 9 / 5 + 32, 1) if temp_c is not None else None

            # Parse dewpoint
            dp_c = props.get("dewpoint", {}).get("value")
            dewpoint_f = round(dp_c * 9 / 5 + 32, 1) if dp_c is not None else None

            # Parse humidity
            humidity = props.get("relativeHumidity", {}).get("value")
            humidity_pct = round(humidity, 0) if humidity is not None else None

            # Parse wind
            wind_speed = props.get("windSpeed", {}).get("value")
            wind_speed_mph = round(wind_speed * 0.621371, 1) if wind_speed is not None else None
            wind_dir_deg = props.get("windDirection", {}).get("value")
            wind_dir = _degrees_to_direction(wind_dir_deg) if wind_dir_deg is not None else None
            wind_gust = props.get("windGust", {}).get("value")
            wind_gust_mph = round(wind_gust * 0.621371, 1) if wind_gust is not None else None

            # Parse pressure (hPa -> inHg)
            pressure_hpa = props.get("pressure", {}).get("value")
            pressure_in = round(pressure_hpa * 0.02953, 2) if pressure_hpa is not None else None

            # Parse visibility
            vis_m = props.get("visibility", {}).get("value")
            visibility_mi = round(vis_m / 1609.34, 1) if vis_m is not None else None

            # Parse ceiling (cloud base)
            ceiling_ft = None
            layers = props.get("ceiling", {})
            if layers and layers.get("value") is not None:
                ceiling_ft = layers.get("value")

            # Parse station location
            station_lat = data.get("geometry", {}).get("coordinates", [None, None])[1]
            station_lon = data.get("geometry", {}).get("coordinates", [None, None])[0]
            distance = haversine_miles(lat, lon, station_lat or lat, station_lon or lon)

            # Parse observation time and age
            obs_time_str = props.get("timestamp") or props.get("observationTime")
            freshness = "stale"
            age_minutes: float | None = None
            if obs_time_str:
                try:
                    obs_time = datetime.fromisoformat(obs_time_str.replace("Z", "+00:00"))
                    age_minutes = (datetime.now(timezone.utc) - obs_time).total_seconds() / 60
                    if age_minutes < 15:
                        freshness = "fresh"
                    elif age_minutes < 45:
                        freshness = "recent"
                except Exception:
                    age_minutes = None

            obs = METARStationObservation(
                station_id=stn,
                station_name=props.get("stationIdentifier", stn),
                distance_miles=round(distance, 1),
                latitude=station_lat or 0.0,
                longitude=station_lon or 0.0,
                text_description=props.get("textDescription"),
                temp_f=temp_f,
                dewpoint_f=dewpoint_f,
                humidity_pct=humidity_pct,
                wind_speed_mph=wind_speed_mph,
                wind_direction=wind_dir,
                wind_gust_mph=wind_gust_mph,
                pressure_in=pressure_in,
                visibility_mi=visibility_mi,
                ceiling_ft=ceiling_ft,
                raw_metar=props.get("rawObservation"),
                observation_time=obs_time_str,
                observation_age_minutes=round(age_minutes, 1) if age_minutes is not None else None,
                freshness=freshness,  # type: ignore
            )
            observations.append(obs)

            if temp_f is not None:
                temps.append(temp_f)
            if humidity_pct is not None:
                humidities.append(humidity_pct)
            if wind_speed_mph is not None:
                winds.append(wind_speed_mph)
            if visibility_mi is not None:
                visibilities.append(visibility_mi)

        summary.stations = observations
        summary.fetch_success = True

        # Compute consensus
        if temps:
            summary.consensus_temp_f = round(sum(temps) / len(temps), 1)
        if humidities:
            summary.consensus_humidity_pct = round(sum(humidities) / len(humidities), 0)
        if winds:
            summary.consensus_wind_mph = round(sum(winds) / len(winds), 1)
        if visibilities:
            summary.consensus_visibility_mi = round(sum(visibilities) / len(visibilities), 1)

        # Confidence based on response rate and agreement
        responded_pct = len(observations) / len(station_list)
        if len(observations) >= 3 and responded_pct >= 0.75:
            summary.confidence = "high"
            summary.confidence_reason = f"{len(observations)} stations responded"
        elif len(observations) >= 2:
            summary.confidence = "medium"
            summary.confidence_reason = f"{len(observations)} stations responded"
        elif len(observations) >= 1:
            summary.confidence = "low"
            summary.confidence_reason = f"Only {len(observations)} station responded"
        else:
            summary.confidence = "none"
            summary.confidence_reason = "No stations responded"

        log.info("metar.layer_complete",
                 stations=len(observations),
                 responded=summary.stations_responded,
                 confidence=summary.confidence)

    except Exception as exc:
        summary.fetch_success = False
        summary.fetch_error = str(exc)
        log.warn("metar.layer_error", error=str(exc))

    return summary


# ── NWS Area Forecast Discussion ─────────────────────────────────────────────

async def fetch_afd(
    lat: float,
    lon: float,
) -> AFDSummary:
    """Fetch the latest NWS Area Forecast Discussion (AFD) for the forecast office.

    Uses the NWS public products API (api.weather.gov) which returns JSON-LD
    with the AFD text content.
    """
    summary = AFDSummary(enabled=True)

    try:
        # First, resolve the grid point to get the WFO
        points_url = f"https://api.weather.gov/points/{lat},{lon}"
        headers = {
            "User-Agent": "LifeDashboard/0.2.0 (personal weather dashboard)",
            "Accept": "application/geo+json",
        }
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            points_resp = await client.get(points_url, headers=headers)
            if points_resp.status_code != 200:
                summary.fetch_error = f"NWS points API returned {points_resp.status_code}"
                log.warn("afd.points_failed", status=points_resp.status_code)
                return summary

            points_data = points_resp.json()
            wfo = points_data.get("properties", {}).get("gridId")
            if not wfo:
                summary.fetch_error = "Could not determine WFO from grid point"
                return summary

            summary.wfo_office = wfo

            # Fetch the AFD products list
            afd_url = f"https://api.weather.gov/products/types/AFD/locations/{wfo}"
            afd_resp = await client.get(afd_url, headers=headers)
            if afd_resp.status_code != 200:
                summary.fetch_error = f"AFD products API returned {afd_resp.status_code}"
                log.warn("afd.products_failed", status=afd_resp.status_code)
                return summary

            afd_data = afd_resp.json()
            graph = afd_data.get("@graph", [])

            if not graph:
                summary.fetch_error = "No AFD products returned"
                return summary

            # Get the most recent AFD
            latest = graph[0]
            product_id = latest.get("id", "")
            issuance_time = latest.get("issuanceTime", "")
            forecaster = latest.get(" forecaster", latest.get("issuedBy", {}).get("name", ""))
            summary.forecaster = forecaster
            summary.product_id = product_id
            summary.issuance_time = issuance_time

            # Parse times
            if latest.get("validTime"):
                summary.valid_time = latest["validTime"]
            if latest.get("expireTime"):
                summary.expire_time = latest["expireTime"]

            # Fetch the full AFD text
            # The AFD product has a @id that can be used to get the full text
            afd_id = latest.get("@id", "")
            if afd_id:
                detail_resp = await client.get(afd_id, headers=headers)
                if detail_resp.status_code == 200:
                    detail_data = detail_resp.json()
                    # The text content is in the productText field
                    full_text = detail_data.get("productText", "") or detail_data.get("text", "")
                    if full_text:
                        # Take first ~800 chars as excerpt for the raw field
                        summary.raw_excerpt = full_text[:800].strip()
                        # Try to extract sections from the AFD text
                        _parse_afd_text(full_text, summary)
                        summary.fetch_success = True
                        summary.parse_confidence = "high"
                        log.info("afd.fetched", wfo=wfo, forecaster=forecaster,
                                synopsis_len=len(summary.synopsis))
                        return summary

            # If we couldn't get full text, try raw content from the graph entry
            raw_text = latest.get("productText", latest.get("text", ""))
            if raw_text:
                summary.raw_excerpt = raw_text[:800].strip()
                _parse_afd_text(raw_text, summary)
                summary.fetch_success = True
                summary.parse_confidence = "medium"
                log.info("afd.fetched_partial", wfo=wfo)
                return summary

            summary.fetch_error = "Could not extract AFD text content"
            summary.parse_confidence = "none"

    except Exception as exc:
        summary.fetch_error = str(exc)
        summary.parse_confidence = "none"
        log.warn("afd.fetch_error", error=str(exc))

    return summary


def _parse_afd_text(text: str, summary: AFDSummary) -> None:
    """Parse AFD text into structured sections.

    AFDs follow a loose format. We do lightweight extraction:
    - Skip WMO bulletin header (first few lines like "000", "FXUS64 KJAN...", "AFDJAN")
    - Look for KEY MESSAGES / SYNOPSIS section
    - .SHORT TERM / .LONG TERM / .EXTENDED sections if present
    - Concerns mentioned anywhere in text
    """
    if not text:
        return

    import re

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Split into lines
    lines = text.strip().split("\n")

    # Skip WMO header lines (lines that look like "000", "FXUS64 KJAN...", "AFDJAN", blank lines)
    # Start collecting content after the header block
    content_start = 0
    header_pattern = re.compile(r"^\d{3}\s*$|^FXUS\d{2}\s+\w{3}\s+\d{6}\s+\w{4}$|^AFD\w{3}\s*$|^\s*$", re.IGNORECASE)
    for i, line in enumerate(lines):
        if not header_pattern.match(line.strip()):
            content_start = i
            break

    # Get remaining lines for parsing
    content_lines = lines[content_start:]

    # Collect first non-empty paragraph as synopsis (after header)
    para_lines: list[str] = []
    for line in content_lines:
        stripped = line.strip()
        if stripped:
            para_lines.append(stripped)
        elif para_lines:
            break  # End of first paragraph

    if para_lines:
        summary.synopsis = " ".join(para_lines[:4])[:300]

    # Look for KEY MESSAGES / SYNOPSIS section
    key_msg_lines: list[str] = []
    in_key_msg = False
    in_short = False
    in_extended = False
    short_term_parts: list[str] = []
    extended_parts: list[str] = []

    for line in content_lines:
        upper = line.upper()
        if re.search(r"\bKEY\s+MESSAGES\b|\bSYNOPSIS\b", upper):
            in_key_msg = True
            in_short = False
            in_extended = False
        elif re.search(r"\bSHORT\s+TERM\b", upper):
            in_short = True
            in_extended = False
            in_key_msg = False
        elif re.search(r"\bLONG\s+TERM\b|\bEXTENDED\b", upper):
            in_short = False
            in_extended = True
            in_key_msg = False
        elif re.search(r"\.\.\.", upper) or re.search(r"^\s*&&", line):
            in_key_msg = False
            in_short = False
            in_extended = False
        elif in_key_msg:
            stripped = line.strip()
            if stripped:
                key_msg_lines.append(stripped)
        elif in_short:
            stripped = line.strip()
            if stripped:
                short_term_parts.append(stripped)
        elif in_extended:
            stripped = line.strip()
            if stripped:
                extended_parts.append(stripped)

    if key_msg_lines:
        # Use KEY MESSAGES as the synopsis if available
        summary.synopsis = " ".join(key_msg_lines[:4])[:300]

    if short_term_parts:
        summary.short_term_summary = " ".join(short_term_parts[:8])[:400]
    if extended_parts:
        summary.extended_summary = " ".join(extended_parts[:8])[:400]

    # Look for any "concerns" keywords throughout
    concern_keywords = ["concern", "watch", "warning", "advisory", "heavy rain",
                        "severe", "flash flood", "tornado", "hail", "wind damage",
                        "fire danger", "elevated risk"]
    concern_lines = [
        line.strip() for line in content_lines
        if any(kw in line.lower() for kw in concern_keywords)
        and not line.strip().startswith('&&')
    ]
    if concern_lines:
        summary.concerns = " ".join(concern_lines[:3])[:300]


# ── SPC Convective Outlook ─────────────────────────────────────────────────────

# SPC DN values: 0=TSTM, 1=MRGL, 2=SLGT, 3=ENH, 4=MDT, 5=HIGH
_DN_TO_RISK = {
    0: "TSTM",
    1: "MRGL",
    2: "SLGT",
    3: "ENH",
    4: "MDT",
    5: "HIGH",
}
_RISK_TO_DN = {v: k for k, v in _DN_TO_RISK.items()}


async def fetch_spc_outlook(
    lat: float,
    lon: float,
    day: int = 1,
) -> SPCOutlookSummary:
    """Fetch SPC convective outlook and check if target location is in risk area.

    Uses the SPC's GeoJSON archive zip file to get risk polygon geometries
    and attributes. The GeoJSON files contain MultiPolygon/Polygon geometries
    for each risk category.
    """
    import zipfile
    import io
    import json
    import re

    summary = SPCOutlookSummary(enabled=True, day=day)

    # Determine date for URL construction
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")
    # Use 00z or 12z cycle — determine current cycle
    hour_utc = now.hour
    cycle = "0100" if hour_utc < 12 else "1300"

    zip_url = f"https://www.spc.noaa.gov/products/outlook/archive/{now.year}/day{day}otlk_{date_str}_{cycle}-geojson.zip"

    headers = {"User-Agent": "LifeDashboard/0.2.0 (personal weather dashboard)"}

    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(zip_url, headers=headers)
            if resp.status_code != 200:
                summary.fetch_error = f"SPC archive returned {resp.status_code}"
                log.warn("spc.zip_failed", status=resp.status_code, url=zip_url)
                return summary

            z = zipfile.ZipFile(io.BytesIO(resp.content))
            namelist = z.namelist()

            # Parse the categorical outlook
            cat_file = next((n for n in namelist if "_cat." in n and "lyr" in n), None)
            torn_file = next((n for n in namelist if "_torn." in n and "lyr" in n), None)
            hail_file = next((n for n in namelist if "_hail." in n and "lyr" in n), None)
            wind_file = next((n for n in namelist if "_wind." in n and "lyr" in n), None)

            def parse_risk_area(name: str | None, risk_key: str) -> SPCRiskArea | None:
                if not name:
                    return None
                try:
                    content = json.loads(z.read(name).decode("utf-8"))
                    features = content.get("features", [])
                    if not features:
                        return None
                    feat = features[0]
                    props = feat.get("properties", {})
                    dn = int(props.get("DN", 0))
                    label = str(props.get("LABEL", ""))
                    label2 = str(props.get("LABEL2", ""))
                    fill = props.get("fill", "")
                    geom_type = feat.get("geometry", {}).get("type", "")

                    # Determine if target point is within this polygon
                    in_area = False
                    if geom_type in ("Polygon", "MultiPolygon"):
                        coords = feat.get("geometry", {}).get("coordinates", [])
                        in_area = _point_in_polygon(lat, lon, coords, geom_type)

                    return SPCRiskArea(
                        risk_type=_DN_TO_RISK.get(dn, "TSTM"),  # type: ignore
                        label=label,
                        label2=label2,
                        dn_value=dn,
                        fill_color=fill,
                        has_geometry=geom_type in ("Polygon", "MultiPolygon"),
                    )
                except Exception as exc:
                    log.warn("spc.parse_error", file=name, error=str(exc))
                    return None

            # Parse categorical outlook first (overall risk)
            if cat_file:
                try:
                    cat_data = json.loads(z.read(cat_file).decode("utf-8"))
                    cat_feat = cat_data.get("features", [{}])[0]
                    cat_props = cat_feat.get("properties", {})
                    summary.categorical_dn = int(cat_props.get("DN", 0))
                    summary.categorical_risk = _DN_TO_RISK.get(summary.categorical_dn, "TSTM")  # type: ignore
                    summary.categorical_label = str(cat_props.get("LABEL2", cat_props.get("LABEL", "")))
                    summary.issuance_time = cat_props.get("ISSUE_ISO", "")
                    summary.valid_time = cat_props.get("VALID_ISO", "")
                    summary.expire_time = cat_props.get("EXPIRE_ISO", "")
                    summary.forecaster = str(cat_props.get("FORECASTER", ""))
                    summary.confidence = "high"
                except Exception as exc:
                    log.warn("spc.cat_parse_failed", error=str(exc))

            # Parse per-hazard risk areas
            summary.tornado = parse_risk_area(torn_file, "tornado")
            summary.hail = parse_risk_area(hail_file, "hail")
            summary.wind = parse_risk_area(wind_file, "wind")

            # Check if target is in any risk area
            for area in [summary.tornado, summary.hail, summary.wind]:
                if area and area.has_geometry:
                    # Check if in this risk polygon
                    in_polygon = _check_target_in_risk(lat, lon, area, cat_file, z if cat_file else None)
                    if in_polygon:
                        summary.target_in_risk_area = True
                        summary.target_risk_description = f"{area.risk_type} Risk: {area.label}"
                        break

            summary.fetch_success = True
            log.info("spc.outlook_fetched",
                     day=day,
                     risk=summary.categorical_risk,
                     target_in_area=summary.target_in_risk_area,
                     forecaster=summary.forecaster)

    except Exception as exc:
        summary.fetch_error = str(exc)
        summary.fetch_success = False
        log.warn("spc.outlook_error", error=str(exc))

    return summary


def _point_in_polygon(lat: float, lon: float, coords: Any, geom_type: str) -> bool:
    """Ray-casting point-in-polygon for a single polygon or multi-polygon."""
    import math

    def point_in_single_poly(pt_lat: float, pt_lon: float, poly_coords: list) -> bool:
        """Check if point is inside a polygon given as list of [lon, lat] rings."""
        inside = False
        for ring in poly_coords:
            # ring is list of [lon, lat] points
            j = len(ring) - 1
            for i in range(len(ring)):
                xi, yi = ring[i][0], ring[i][1]
                xj, yj = ring[j][0], ring[j][1]
                if ((yi > pt_lat) != (yj > pt_lat)) and \
                        (pt_lon < (xj - xi) * (pt_lat - yi) / (yj - yi) + xi):
                    inside = not inside
                j = i
        return inside

    if geom_type == "Polygon":
        return point_in_single_poly(lat, lon, coords[0] if coords else [])
    elif geom_type == "MultiPolygon":
        for poly in coords:
            if point_in_single_poly(lat, lon, poly[0] if poly else []):
                return True
        return False
    return False


def _check_target_in_risk(
    lat: float,
    lon: float,
    area: SPCRiskArea,
    cat_file: str | None,
    z: zipfile.ZipFile | None,
) -> bool:
    """Check if target lat/lon is within a given risk area's polygon."""
    if not area.has_geometry or not cat_file or z is None:
        return False
    try:
        cat_data = json.loads(z.read(cat_file).decode("utf-8"))
        feat = cat_data.get("features", [{}])[0]
        geom_type = feat.get("geometry", {}).get("type", "")
        coords = feat.get("geometry", {}).get("coordinates", [])
        return _point_in_polygon(lat, lon, coords, geom_type)
    except Exception:
        return False


# ── Data Transformation ───────────────────────────────────────────────────────

def _wmo_code_to_condition(code: int) -> str:
    """Map WMO weather code to human-readable condition."""
    mapping = {
        0: "Clear",
        1: "Mainly Clear",
        2: "Partly Cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Depositing Rime Fog",
        51: "Light Drizzle",
        53: "Drizzle",
        55: "Heavy Drizzle",
        61: "Light Rain",
        63: "Rain",
        65: "Heavy Rain",
        71: "Light Snow",
        73: "Snow",
        75: "Heavy Snow",
        77: "Snow Grains",
        80: "Light Showers",
        81: "Showers",
        82: "Heavy Showers",
        85: "Snow Showers",
        86: "Snow Showers",
        95: "Thunderstorm",
        96: "Thunderstorm with Hail",
        99: "Thunderstorm with Heavy Hail",
    }
    return mapping.get(code, f"Code {code}")


def _wttr_code_to_condition(code: int) -> str:
    """Map wttr.in weather code to human-readable condition."""
    mapping = {
        113: "Clear",
        116: "Partly Cloudy",
        119: "Cloudy",
        122: "Overcast",
        143: "Mist",
        176: "Patchy Rain",
        179: "Patchy Snow",
        182: "Patchy Sleet",
        185: "Patchy Freezing Drizzle",
        200: "Thundery Outbreaks",
        227: "Blowing Snow",
        230: "Blizzard",
        248: "Fog",
        260: "Freezing Fog",
        263: "Patchy Light Drizzle",
        266: "Light Drizzle",
        281: "Freezing Drizzle",
        284: "Heavy Freezing Drizzle",
        293: "Patchy Light Rain",
        296: "Light Rain",
        299: "Moderate Rain at Times",
        302: "Moderate Rain",
        305: "Heavy Rain at Times",
        308: "Heavy Rain",
        311: "Light Freezing Rain",
        314: "Moderate/Heavy Freezing Rain",
        317: "Light Sleet",
        320: "Moderate/Heavy Sleet",
        323: "Patchy Light Snow",
        326: "Light Snow",
        329: "Patchy Moderate Snow",
        332: "Moderate Snow",
        335: "Patchy Heavy Snow",
        338: "Heavy Snow",
        350: "Ice Pellets",
        353: "Light Rain Shower",
        356: "Moderate/Heavy Rain Shower",
        359: "Torrential Rain Shower",
        362: "Light Sleet Showers",
        365: "Moderate/Heavy Sleet Showers",
        368: "Light Snow Showers",
        371: "Moderate/Heavy Snow Showers",
        374: "Light Showers of Ice Pellets",
        377: "Moderate/Heavy Showers of Ice Pellets",
        386: "Patchy Light Rain with Thunder",
        389: "Moderate/Heavy Rain with Thunder",
        392: "Patchy Light Snow with Thunder",
        395: "Moderate/Heavy Snow with Thunder",
    }
    return mapping.get(code, f"Condition {code}")


def _degrees_to_direction(degrees: float) -> str:
    """Convert wind degrees to cardinal direction."""
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    index = round(degrees / 22.5) % 16
    return directions[index]


def _build_current_weather(
    wttr_data: dict[str, Any],
    meteo_data: dict[str, Any] | None,
) -> CurrentWeather:
    """Build CurrentWeather from multiple sources, preferring best data."""
    # Start with wttr.in data
    current = wttr_data.get("current_condition", [{}])[0]
    wttr_temp = float(current.get("temp_F", 70))
    wttr_humidity = int(current.get("humidity", 50))
    wttr_wind = float(current.get("windspeedMiles", 0))
    wttr_pressure = float(current.get("pressure", 29.92))
    
    # Overlay with Open-Meteo for better precision
    uv_index = 0.0
    feels_like = None
    wind_dir = "N"
    dewpoint = None
    visibility = None
    
    if meteo_data:
        curr = meteo_data.get("current", {})
        uv_index = float(curr.get("uv_index", 0.0))
        
        # Use Open-Meteo temp if available (often more accurate)
        if "temperature_2m" in curr:
            # Convert C to F
            wttr_temp = float(curr["temperature_2m"]) * 9/5 + 32
        
        if "apparent_temperature" in curr:
            feels_like = float(curr["apparent_temperature"]) * 9/5 + 32
        
        if "wind_direction_10m" in curr:
            wind_dir = _degrees_to_direction(float(curr["wind_direction_10m"]))
        
        if "relative_humidity_2m" in curr:
            wttr_humidity = int(curr["relative_humidity_2m"])
        
        if "pressure_msl" in curr:
            # Convert hPa to inches
            wttr_pressure = float(curr["pressure_msl"]) * 0.02953
        
        if "visibility" in curr:
            # Convert meters to miles
            visibility_m = float(curr["visibility"])
            visibility = visibility_m / 1609.34
    
    # Get condition from wttr.in
    wttr_code = int(current.get("weatherCode", 116))
    condition = _wttr_code_to_condition(wttr_code)
    
    return CurrentWeather(
        temp_f=round(wttr_temp, 1),
        condition=condition,
        humidity_pct=wttr_humidity,
        wind_mph=round(wttr_wind, 1),
        wind_direction=wind_dir,
        uv_index=round(uv_index, 1),
        pressure_in=round(wttr_pressure, 2) if wttr_pressure else None,
        visibility_mi=round(visibility, 1) if visibility else None,
        feels_like_f=round(feels_like, 1) if feels_like else None,
    )


def _build_forecast(
    wttr_data: dict[str, Any],
    meteo_data: dict[str, Any] | None,
    nws_data: dict[str, Any] | None,
) -> list[DayForecast]:
    """Build forecast from multiple sources."""
    forecast: list[DayForecast] = []
    
    # Primary: wttr.in 3-day forecast
    wttr_days = wttr_data.get("weather", [])[:3]
    for day in wttr_days:
        max_temp_c = float(day.get("maxtempC", 25))
        min_temp_c = float(day.get("mintempC", 15))
        max_f = max_temp_c * 9/5 + 32
        min_f = min_temp_c * 9/5 + 32
        
        # Get weather code from midday hourly
        hourly = day.get("hourly", [])
        mid_hour = hourly[len(hourly)//2] if hourly else {}
        code = int(mid_hour.get("weatherCode", 116))
        precip = int(mid_hour.get("chanceofrain", 0))
        
        forecast.append(DayForecast(
            day=day.get("date", "Unknown"),
            date_iso=day.get("date"),
            high_f=round(max_f, 1),
            low_f=round(min_f, 1),
            condition=_wttr_code_to_condition(code),
            precip_pct=precip,
        ))
    
    # Overlay Open-Meteo forecast data if available
    if meteo_data and "daily" in meteo_data:
        daily = meteo_data["daily"]
        dates = daily.get("time", [])
        max_temps = daily.get("temperature_2m_max", [])
        min_temps = daily.get("temperature_2m_min", [])
        precip_probs = daily.get("precipitation_probability_max", [])
        codes = daily.get("weather_code", [])
        
        for i, date_str in enumerate(dates[:3]):
            if i < len(forecast):
                # Update with more precise data
                if i < len(max_temps):
                    forecast[i].high_f = round(float(max_temps[i]) * 9/5 + 32, 1)
                if i < len(min_temps):
                    forecast[i].low_f = round(float(min_temps[i]) * 9/5 + 32, 1)
                if i < len(precip_probs):
                    forecast[i].precip_pct = int(precip_probs[i])
                if i < len(codes):
                    forecast[i].condition = _wmo_code_to_condition(int(codes[i]))
    
    # Overlay NWS forecast if available (US only)
    if nws_data and "forecast" in nws_data:
        nws_forecast = nws_data["forecast"]
        periods = nws_forecast.get("properties", {}).get("periods", [])
        
        # Group periods by day
        day_periods: dict[str, list[dict]] = {}
        for period in periods[:6]:  # Take first 6 periods (3 days x 2 periods)
            day_name = period.get("name", "").split()[0]  # "Monday Night" -> "Monday"
            if day_name not in day_periods:
                day_periods[day_name] = []
            day_periods[day_name].append(period)
        
        # Update forecast with NWS data
        for i, fc in enumerate(forecast):
            day_name = fc.day
            if day_name in day_periods:
                periods = day_periods[day_name]
                if periods:
                    # Use the day period (not night) for condition
                    day_period = next((p for p in periods if "night" not in p.get("name", "").lower()), periods[0])
                    fc.condition = day_period.get("shortForecast", fc.condition)
    
    return forecast


def _build_hourly_forecast(meteo_data: dict[str, Any] | None) -> list[HourlyForecast]:
    """Build hourly forecast from Open-Meteo."""
    hourly: list[HourlyForecast] = []
    
    if not meteo_data or "hourly" not in meteo_data:
        return hourly
    
    h = meteo_data["hourly"]
    times = h.get("time", [])
    temps = h.get("temperature_2m", [])
    precip = h.get("precipitation_probability", [])
    codes = h.get("weather_code", [])
    winds = h.get("wind_speed_10m", [])
    
    # Take next 24 hours
    for i in range(min(24, len(times))):
        try:
            temp_c = float(temps[i]) if i < len(temps) else 0
            temp_f = temp_c * 9/5 + 32
            code = int(codes[i]) if i < len(codes) else 0
            precip_prob = int(precip[i]) if i < len(precip) else 0
            wind = float(winds[i]) if i < len(winds) else 0
            
            hourly.append(HourlyForecast(
                hour=times[i],
                temp_f=round(temp_f, 1),
                condition=_wmo_code_to_condition(code),
                precip_chance=precip_prob,
                wind_mph=round(wind * 0.621371, 1),  # km/h to mph
            ))
        except (ValueError, TypeError):
            continue
    
    return hourly


# ── LLM Synthesis ─────────────────────────────────────────────────────────────

async def synthesize_meteorology_narrative(
    report: MeteorologyReport,
    api_key: str | None = None,
    provider: str = "openrouter",
) -> dict[str, Any]:
    """Generate LLM narrative for meteorology report using MiniMax M2.7."""
    import time
    
    start_time = time.time()
    
    # Build prompt
    prompt = _build_meteorology_prompt(report)
    
    # Try the preferred provider first, then fall back sanely.
    result = None
    
    if provider == "minimax" and api_key:
        result = await _call_minimax(prompt, api_key)
        if not result and os.environ.get("OPENROUTER_API_KEY"):
            result = await _call_openrouter(prompt, os.environ.get("OPENROUTER_API_KEY"))
    else:
        result = await _call_openrouter(prompt, api_key or os.environ.get("OPENROUTER_API_KEY"))
        if not result and os.environ.get("MINIMAX_API_KEY"):
            result = await _call_minimax(prompt, os.environ.get("MINIMAX_API_KEY"))
    
    duration_ms = int((time.time() - start_time) * 1000)
    
    if result:
        sections = result.get("sections", {})
        summary = result.get("summary", "")
        tts_script = (result.get("tts_script", "") or "").strip()

        if not tts_script and provider == "minimax" and api_key:
            tts_script = await _call_minimax_tts_script(report, summary, sections, api_key)

        return {
            "narrative": result.get("narrative", ""),
            "narrative_summary": summary,
            "tts_script": tts_script,
            "sections": sections,
            "model": result.get("model", "unknown"),
            "reasoning": result.get("reasoning", ""),
            "duration_ms": duration_ms,
            "success": True,
        }
    
    # Fallback deterministic summary
    fallback = _generate_fallback_narrative(report)
    return {
        "narrative": fallback["narrative"],
        "narrative_summary": fallback["summary"],
        "tts_script": "",
        "sections": fallback["sections"],
        "model": "fallback",
        "reasoning": "none",
        "duration_ms": duration_ms,
        "success": False,
    }


_US_STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia",
}


def _spoken_location(location: str) -> str:
    """Expand short state abbreviations for more natural TTS speech."""
    parts = [part.strip() for part in location.split(",") if part.strip()]
    if len(parts) >= 2:
        maybe_state = parts[-1].upper()
        if maybe_state in _US_STATE_NAMES:
            parts[-1] = _US_STATE_NAMES[maybe_state]
    return ", ".join(parts) if parts else location


def _build_weather_tts_brief(report: MeteorologyReport, summary: str = "") -> str:
    """Build a richer spoken weather brief suitable for TTS playback."""
    current = report.current
    forecast = report.forecast
    alerts = report.alerts
    afd = report.afd_layer
    spc = report.spc_outlook

    now = datetime.now(timezone.utc)
    daypart = "morning" if now.hour < 12 else ("afternoon" if now.hour < 18 else "evening")
    location = _spoken_location(report.location)

    intro = (
        f"Good {daypart} from Compass. Right now in {location}, "
        f"we have {current.condition.lower()} skies with temperatures near {round(current.temp_f)} degrees"
    )
    if current.feels_like_f is not None and round(current.feels_like_f) != round(current.temp_f):
        intro += f", feeling closer to {round(current.feels_like_f)}"
    if current.humidity_pct is not None:
        intro += f", with humidity near {round(current.humidity_pct)} percent"
    if current.wind_mph:
        intro += f", and winds out of the {current.wind_direction or 'north'} around {round(current.wind_mph)} miles per hour"
    intro += "."

    outlook = ""
    if forecast:
        today = forecast[0]
        precip_text = (
            f" Rain chances are around {round(today.precip_pct)} percent."
            if today.precip_pct is not None
            else ""
        )
        outlook = (
            f" Through the next day, expect a high near {round(today.high_f)} and a low near {round(today.low_f)}, "
            f"with {today.condition.lower()} conditions.{precip_text}"
        )
        if len(forecast) > 1:
            tomorrow = forecast[1]
            outlook += (
                f" Looking a little farther ahead, the following day trends {tomorrow.condition.lower()} "
                f"with highs around {round(tomorrow.high_f)} and lows near {round(tomorrow.low_f)}."
            )

    main_thing = ""
    concerns = (afd.concerns if afd and afd.concerns else "").lower()
    summary_lower = (summary or "").lower()
    if alerts:
        main_thing = (
            f" There {'is' if len(alerts) == 1 else 'are'} {len(alerts)} active weather alert"
            f"{'' if len(alerts) == 1 else 's'} to keep in mind."
        )
    elif "fire" in concerns or "fire" in summary_lower:
        main_thing = " The main thing to watch is the warm, dry pattern and a growing fire weather concern later this week."
    elif spc and spc.target_in_risk_area and spc.categorical_risk and spc.categorical_risk not in ("NONE", "TSTM"):
        main_thing = f" The main thing to watch is a {spc.categorical_risk.lower()} severe weather risk in the area."
    elif spc and spc.fetch_success:
        main_thing = f" Severe weather is not expected around {location} right now."

    wrap = " Plan on generally manageable weather overall, and check back for updates if you have outdoor plans."
    return f"{intro}{main_thing}{outlook}{wrap}".strip()


def _build_tts_script(
    report: MeteorologyReport,
    summary: str,
    sections: dict[str, str],
) -> str:
    """Build a spoken, TTS-friendly weather hit when the model does not provide one."""
    return _build_weather_tts_brief(report, summary)


def _build_meteorology_tts_prompt(
    report: MeteorologyReport,
    summary: str,
    sections: dict[str, str],
) -> str:
    """Build a focused prompt for an LLM-authored spoken weather script."""
    location = _spoken_location(report.location)
    current = report.current
    forecast = report.forecast
    executive = sections.get("executive_summary", "")
    short_term = sections.get("short_term_forecast", "")
    notable = sections.get("notable_patterns", "")
    recommendations = sections.get("recommendations", "")

    forecast_lines = []
    for idx, day in enumerate(forecast[:2], start=1):
        forecast_lines.append(
            f"Day {idx}: high {round(day.high_f)}°F, low {round(day.low_f)}°F, {day.condition}, precip {round(day.precip_pct or 0)}%"
        )

    return f"""Write ONLY valid JSON for a spoken weather briefing.

Location: {location}
Current conditions: {current.condition}, {round(current.temp_f)}°F, feels like {round(current.feels_like_f or current.temp_f)}°F, humidity {round(current.humidity_pct or 0)}%, wind {round(current.wind_mph or 0)} mph from {current.wind_direction or 'north'}
Dashboard summary: {summary}
Executive summary: {executive}
Short-term forecast: {short_term}
Notable patterns: {notable}
Recommendations: {recommendations}
Forecast snapshot:
- {'; '.join(forecast_lines) if forecast_lines else 'No forecast data'}

Rules:
- Return ONLY JSON in this shape: {{"tts_script":"..."}}
- Write for audio playback, not for reading on screen
- 60 to 100 seconds when spoken aloud
- Sound natural, polished, and specific
- Fully spell state names for speech, for example Mississippi instead of MS
- Do not mention being an AI or reading sections
- Do not include markdown or stage directions
"""


def _build_meteorology_prompt(report: MeteorologyReport) -> str:
    """Build a detailed prompt for meteorology synthesis.

    Sources in priority order for current conditions:
      1. METAR / ASOS (official aviation observations — most reliable for temp/wind)
      2. PWS consensus (community stations — good for local nuance)
      3. NWS forecast (official forecast guidance)
      4. Model data (wttr.in / Open-Meteo — best for forecast trends)
    """
    current = report.current
    forecast = report.forecast
    alerts = report.alerts
    pws = report.pws_layer
    metar = report.metar_layer
    afd = report.afd_layer
    spc = report.spc_outlook

    # ── Current conditions from models ──────────────────────────────────────────
    current_text = f"""
Current Model Conditions ({report.location}):
- Temperature: {current.temp_f}°F (feels like {current.feels_like_f or current.temp_f}°F)
- Condition: {current.condition}
- Humidity: {current.humidity_pct}%
- Wind: {current.wind_mph} mph from {current.wind_direction}
- UV Index: {current.uv_index}
- Pressure: {current.pressure_in or 'N/A'} inches
"""

    # ── Official METAR observations ───────────────────────────────────────────
    metar_text = ""
    if metar and metar.fetch_success and metar.stations_responded > 0:
        metar_text = "\nOfficial METAR / ASOS Aviation Observations (NWS):\n"
        metar_text += f"- Confidence: {metar.confidence} ({metar.confidence_reason})\n"
        for obs in metar.stations:
            freshness_note = f" ({obs.freshness}, {obs.observation_age_minutes:.0f} min ago)" if obs.observation_age_minutes else ""
            metar_text += f"- {obs.station_id} ({obs.text_description or 'N/A'}){freshness_note}: "
            parts = []
            if obs.temp_f is not None:
                parts.append(f"{obs.temp_f}°F")
            if obs.humidity_pct is not None:
                parts.append(f"{obs.humidity_pct:.0f}% RH")
            if obs.wind_speed_mph is not None:
                parts.append(f"wind {obs.wind_speed_mph} mph {obs.wind_direction or ''}".strip())
            if obs.visibility_mi is not None:
                parts.append(f"vis {obs.visibility_mi} mi")
            metar_text += ", ".join(parts) + "\n"
        if metar.consensus_temp_f:
            metar_text += f"- Consensus observed temp: {metar.consensus_temp_f}°F\n"
    elif metar and not metar.fetch_success:
        metar_text = f"\nMETAR Layer: Fetch failed — {metar.fetch_error or 'unknown'}\n"
    else:
        metar_text = "\nMETAR Layer: Not available (out of range or no stations responded)\n"

    # ── AFD (NWS Forecast Discussion) ─────────────────────────────────────────
    afd_text = ""
    if afd and afd.fetch_success:
        afd_text = "\nNWS Area Forecast Discussion (AFD) — " + (afd.wfo_office or "NWS") + ":\n"
        afd_text += f"- Forecaster: {afd.forecaster or 'Unknown'}\n"
        afd_text += f"- Issued: {afd.issuance_time or 'N/A'}\n"
        if afd.synopsis:
            afd_text += f"- Synopsis: {afd.synopsis[:300]}\n"
        if afd.short_term_summary:
            afd_text += f"- Short-term: {afd.short_term_summary[:300]}\n"
        if afd.concerns:
            afd_text += f"- Concerns: {afd.concerns[:300]}\n"
    elif afd and afd.fetch_error:
        afd_text = f"\nAFD Layer: Could not retrieve — {afd.fetch_error}\n"
    else:
        afd_text = "\nAFD Layer: Not available\n"

    # ── SPC convective outlook ─────────────────────────────────────────────────
    spc_text = ""
    if spc and spc.fetch_success:
        spc_text = "\nSPC Convective Outlook (Day 1):\n"
        spc_text += f"- Issuing forecaster: {spc.forecaster or 'N/A'}\n"
        spc_text += f"- Valid: {spc.valid_time or 'N/A'} to {spc.expire_time or 'N/A'}\n"
        spc_text += f"- Categorical risk: {spc.categorical_risk} — {spc.categorical_label}\n"
        if spc.target_in_risk_area:
            spc_text += f"- Target area ({report.location}): IN a risk zone — {spc.target_risk_description}\n"
        else:
            spc_text += f"- Target area ({report.location}): NOT in a risk zone\n"
        if spc.tornado and spc.tornado.dn_value > 0:
            spc_text += f"- Tornado risk: {spc.tornado.risk_type} — {spc.tornado.label}\n"
        if spc.hail and spc.hail.dn_value > 0:
            spc_text += f"- Hail risk: {spc.hail.risk_type} — {spc.hail.label}\n"
        if spc.wind and spc.wind.dn_value > 0:
            spc_text += f"- Wind risk: {spc.wind.risk_type} — {spc.wind.label}\n"
    elif spc and spc.fetch_error:
        spc_text = f"\nSPC Layer: Could not retrieve — {spc.fetch_error}\n"
    else:
        spc_text = "\nSPC Layer: Not available (off-season or fetch failed)\n"

    # ── Forecast ───────────────────────────────────────────────────────────────
    forecast_text = "\n3-Day Forecast:\n"
    for day in forecast:
        forecast_text += f"- {day.day}: High {day.high_f}°F, Low {day.low_f}°F, {day.condition}, {day.precip_pct}% rain chance\n"

    # ── Alerts ─────────────────────────────────────────────────────────────────
    alerts_text = ""
    if alerts:
        alerts_text = "\nActive NWS Alerts:\n"
        for alert in alerts:
            alerts_text += f"- [{alert.severity.upper()}] {alert.title}: {alert.description[:150]}...\n"

    # ── PWS community observations ─────────────────────────────────────────────
    pws_text = ""
    if pws and pws.fetch_success and pws.stations_with_data > 0:
        pws_text = "\nNearby Community PWS Observations (Synoptic):\n"
        if pws.consensus:
            pws_text += f"- {pws.stations_with_data} local stations within {pws.search_radius_miles} miles — {pws.consensus.confidence} confidence\n"
            if pws.consensus.consensus_temp_f:
                pws_text += f"- Observed temp (PWS consensus): {pws.consensus.consensus_temp_f}°F\n"
            if pws.consensus.consensus_humidity_pct:
                pws_text += f"- Observed humidity: {int(pws.consensus.consensus_humidity_pct)}%\n"
            if pws.consensus.consensus_wind_mph:
                pws_text += f"- Observed wind: {pws.consensus.consensus_wind_mph} mph\n"
            if pws.consensus.disagreement_pct > 30:
                pws_text += f"- ⚠ High variance between stations ({pws.consensus.disagreement_pct:.0f}% disagreement — treat consensus with caution)\n"
        if pws.stations:
            pws_text += "\nIndividual stations (freshest 3):\n"
            for station in pws.stations[:3]:
                fresh = "" if station.freshness == "fresh" else f" [{station.freshness}]"
                pws_text += f"- {station.name} ({station.distance_miles:.1f} mi){fresh}: {station.temp_f}°F\n"
        if pws.blended_into_current:
            pws_text += f"\n→ PWS data blended into current conditions at {pws.blend_influence_pct:.0f}% weight.\n"
    elif pws and not pws.api_token_configured:
        pws_text = "\nPWS Layer: Not configured (no API token)\n"
    elif pws and not pws.fetch_success:
        pws_text = f"\nPWS Layer: Fetch failed — {pws.fetch_error or 'unknown error'}\n"

    # ── Source comparison / conflict detection ──────────────────────────────────
    source_notes: list[str] = []
    if metar and metar.consensus_temp_f and pws and pws.consensus and pws.consensus.consensus_temp_f:
        diff = abs(metar.consensus_temp_f - pws.consensus.consensus_temp_f)
        if diff > 3:
            source_notes.append(
                f"⚠ METAR ({metar.consensus_temp_f}°F) and PWS ({pws.consensus.consensus_temp_f}°F) "
                f"disagree by {diff:.1f}°F — likely sensor placement or local microclimate effects."
            )
    if metar and metar.consensus_temp_f:
        model_temp = current.temp_f
        obs_temp = metar.consensus_temp_f
        diff = abs(model_temp - obs_temp)
        if diff > 4:
            source_notes.append(
                f"⚠ Model ({model_temp}°F) and official METAR ({obs_temp}°F) disagree significantly "
                f"by {diff:.1f}°F. Trust METAR for current conditions; model may be resolving boundary layer poorly."
            )
        elif diff <= 2:
            source_notes.append(f"✓ Model and METAR are in strong agreement ({diff:.1f}°F difference).")

    source_comparison = ""
    if source_notes:
        source_comparison = "\nSource Conflict Detection:\n" + "\n".join(f"- {n}" for n in source_notes) + "\n"

    prompt = f"""You are a meteorologist providing a detailed, source-aware weather analysis for {report.location}.

SOURCE PRIORITY FOR CURRENT CONDITIONS:
  1. METAR / ASOS (NWS official) → most reliable for observed temp, wind, visibility
  2. PWS consensus (community stations) → good local nuance, slightly lower confidence
  3. NWS forecast discussion (AFD) → expert reasoning on patterns and concerns
  4. Model guidance (wttr.in / Open-Meteo) → best for forecast trends

IMPORTANT SOURCE-AWARENESS RULES:
- Always distinguish OBSERVED conditions (METAR, PWS) from FORECAST conditions (model, NWS forecast)
- When official observations and models disagree, trust observations for current conditions
- Call out source agreement and disagreement explicitly
- Avoid false certainty — use hedged language when sources conflict
- PWS data is community-reported; trust consensus but note if variance is high
- SPC convective outlook provides severe weather context; factor in when risk > MRGL

{current_text}
{metar_text}
{afd_text}
{spc_text}
{forecast_text}
{alerts_text}
{pws_text}
{source_comparison}
Please provide a comprehensive meteorology report with these sections:

1. EXECUTIVE_SUMMARY: 2-3 sentence overview — current conditions, main concern, brief outlook
2. CURRENT_ANALYSIS: Detailed analysis. Compare model data vs METAR vs PWS. Note discrepancies and which source is most reliable right now. Cite specific values from sources.
3. SHORT_TERM_FORECAST: Next 24 hours — tonight and tomorrow. Use forecast models as primary guide.
4. EXTENDED_OUTLOOK: Days 2-3 outlook.
5. RECOMMENDATIONS: Practical advice — clothing, activities, precautions, especially if severe weather risk exists.
6. NOTABLE_PATTERNS: Interesting meteorological patterns, trends, or changes worth noting.
7. DATA_CONFIDENCE: Assess confidence. Note if sources agree, if there's a conflict to watch, and the main source of uncertainty.

If SPC outlook shows any risk above TSTM for the area, explicitly discuss the severe weather potential.

Respond ONLY as valid JSON:
{{
  "summary": "Brief 1-2 sentence teaser for dashboard",
  "narrative": "Full multi-section narrative in natural language, incorporating all sections",
  "sections": {{
    "executive_summary": "...",
    "current_analysis": "...",
    "short_term_forecast": "...",
    "extended_outlook": "...",
    "recommendations": "...",
    "notable_patterns": "...",
    "data_confidence": "..."
  }}
}}

Be factual and grounded. Cite specific temperatures and source disagreements where relevant. Use Fahrenheit."""

    return prompt


async def _call_openrouter(
    prompt: str,
    api_key: str | None = None,
) -> dict[str, Any] | None:
    """Call MiniMax M2.7 via OpenRouter."""
    if not api_key:
        api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        return None
    
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://life-dashboard.local",
        "X-Title": "Life Dashboard Meteorology",
    }
    payload = {
        "model": "minimax/minimax-m2.7",
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 2000,
        "reasoning": {"effort": "high"},
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # Extract JSON from response
            result = _extract_json_from_response(content)
            if result:
                return {
                    "narrative": result.get("narrative", ""),
                    "summary": result.get("summary", ""),
                    "sections": result.get("sections", {}),
                    "model": "minimax/minimax-m2.7",
                    "reasoning": "high",
                }
    except Exception as exc:
        log.warn("openrouter.call_failed", error=str(exc))
    
    return None


async def _call_minimax(
    prompt: str,
    api_key: str,
) -> dict[str, Any] | None:
    """Call MiniMax API directly."""
    url = "https://api.minimax.io/v1/text/chatcompletion_v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "MiniMax-M2.7",
        "messages": [
            {"role": "system", "name": "MiniMax AI", "content": "You are a precise meteorology report generator. Return valid JSON only."},
            {"role": "user", "name": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 4000,
        "response_format": {"type": "json_object"},
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            for attempt in range(2):
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()

                choices = data.get("choices") or []
                first_choice = choices[0] if choices and isinstance(choices[0], dict) else {}
                message = first_choice.get("message") or {}
                content = message.get("content") or ""

                result = _extract_json_from_response(content)
                if result:
                    return {
                        "narrative": result.get("narrative", ""),
                        "summary": result.get("summary", ""),
                        "sections": result.get("sections", {}),
                        "tts_script": result.get("tts_script", ""),
                        "model": "MiniMax-M2.7",
                        "reasoning": "high",
                    }

                log.warn(
                    "minimax.parse_failed",
                    attempt=attempt + 1,
                    response_keys=sorted(data.keys()),
                    content_preview=str(content)[:500],
                )
    except Exception as exc:
        log.warn("minimax.call_failed", error=str(exc))
    
    return None


async def _call_minimax_tts_script(
    report: MeteorologyReport,
    summary: str,
    sections: dict[str, str],
    api_key: str,
) -> str:
    """Request an LLM-authored spoken weather script from MiniMax."""
    prompt = _build_meteorology_tts_prompt(report, summary, sections)
    url = "https://api.minimax.io/v1/text/chatcompletion_v2"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "MiniMax-M2.7",
        "messages": [
            {"role": "system", "name": "MiniMax AI", "content": "You write polished spoken weather audio copy. Return valid JSON only."},
            {"role": "user", "name": "user", "content": prompt}
        ],
        "temperature": 0.4,
        "max_tokens": 1200,
        "response_format": {"type": "json_object"},
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
            choices = data.get("choices") or []
            first_choice = choices[0] if choices and isinstance(choices[0], dict) else {}
            message = first_choice.get("message") or {}
            content = message.get("content") or ""
            result = _extract_json_from_response(content) or {}
            return (result.get("tts_script", "") or "").strip()
    except Exception as exc:
        log.warn("minimax.tts_script_failed", error=str(exc))
        return ""


def _extract_json_from_response(content: str) -> dict[str, Any] | None:
    """Extract and parse JSON from LLM response."""
    import re
    import json
    
    # Try to find JSON block
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Try to find raw JSON object
    json_match = re.search(r'(\{[\s\S]*"summary"[\s\S]*\})', content)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Try to parse entire content as JSON
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    
    return None


def _generate_fallback_narrative(report: MeteorologyReport) -> dict[str, Any]:
    """Generate deterministic fallback narrative when LLM fails."""
    current = report.current
    forecast = report.forecast
    alerts = report.alerts
    
    # Summary
    summary_parts = [f"Currently {current.condition.lower()} at {round(current.temp_f)}°F"]
    if forecast:
        summary_parts.append(f"with a high of {round(forecast[0].high_f)}°F today")
    if alerts:
        summary_parts.append(f"({len(alerts)} active alert{'s' if len(alerts) > 1 else ''})")
    summary = ". ".join(summary_parts) + "."
    
    # Full narrative
    narrative_parts = [
        f"## Current Conditions\n\n"
        f"It is currently {current.condition.lower()} in {report.location} with a temperature of {round(current.temp_f)}°F. "
        f"Humidity is at {current.humidity_pct}% with winds of {current.wind_mph} mph from the {current.wind_direction}."
    ]
    
    if current.feels_like_f and abs(current.feels_like_f - current.temp_f) > 3:
        narrative_parts.append(f" It feels like {round(current.feels_like_f)}°F.")
    
    if forecast:
        narrative_parts.append(f"\n\n## Forecast\n\n")
        for day in forecast:
            narrative_parts.append(
                f"**{day.day}**: High of {round(day.high_f)}°F, low of {round(day.low_f)}°F. "
                f"{day.condition} with {day.precip_pct}% chance of precipitation.\n\n"
            )
    
    if alerts:
        narrative_parts.append("## Alerts\n\n")
        for alert in alerts:
            narrative_parts.append(f"- **{alert.title}** ({alert.severity}): {alert.description[:150]}...\n\n")
    
    narrative = "".join(narrative_parts)
    
    tts_script = _build_weather_tts_brief(report, summary)

    return {
        "narrative": narrative,
        "summary": summary,
        "tts_script": tts_script,
        "sections": {
            "executive_summary": summary,
            "current_analysis": f"{current.condition} conditions with {current.temp_f}°F temperature.",
            "short_term_forecast": forecast[0].condition if forecast else "See forecast details.",
            "extended_outlook": "See forecast details.",
            "recommendations": "Check current conditions before outdoor activities.",
            "notable_patterns": "No significant patterns detected.",
        }
    }


# ── Main Pipeline ─────────────────────────────────────────────────────────────

async def run_weather_pipeline(
    location: str = DEFAULT_LOCATION,
    lat: float | None = None,
    lon: float | None = None,
    db=None,
    enable_llm: bool = True,
) -> MeteorologyReport:
    """
    Full weather ingestion pipeline with multiple sources and LLM synthesis.
    
    1. Fetch from wttr.in (global, reliable)
    2. Fetch from Open-Meteo (free, detailed)
    3. Fetch from NWS/weather.gov (US only, official alerts)
    4. Synthesize with LLM (MiniMax M2.7)
    5. Persist to database
    """
    log.info("weather.pipeline.start", location=location)

    # Resolve coordinates from explicit args, then config, then hardcoded defaults.
    config_weather = {}
    if lat is None or lon is None:
        try:
            from life_dashboard.config import load_config
            config_weather = load_config().get("weather", {})
        except Exception:
            config_weather = {}

    resolved_lat = lat if lat is not None else config_weather.get("latitude", DEFAULT_LAT)
    resolved_lon = lon if lon is not None else config_weather.get("longitude", DEFAULT_LON)
    
    # Fetch all sources concurrently
    sources: list[WeatherSourceData] = []
    
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        tasks = [
            client.get(f"https://wttr.in/{location}?format=j1"),
            client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": resolved_lat,
                    "longitude": resolved_lon,
                    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,uv_index,wind_speed_10m,wind_direction_10m,weather_code,pressure_msl,visibility",
                    "hourly": "temperature_2m,precipitation_probability,weather_code,wind_speed_10m",
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_probability_max,weather_code",
                    "timezone": "America/Chicago",
                    "forecast_days": 3,
                },
            ),
        ]
        
        wttr_resp, meteo_resp = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process responses
    wttr_data = {}
    meteo_data = None
    
    if not isinstance(wttr_resp, Exception) and wttr_resp.status_code == 200:
        wttr_data = wttr_resp.json()
        sources.append(WeatherSourceData(
            source="wttr.in",
            fetched_at=datetime.now(timezone.utc).isoformat(),
            confidence="high",
        ))
        log.info("weather.source.success", source="wttr.in")
    else:
        log.warn("weather.source.failed", source="wttr.in", error=str(wttr_resp) if isinstance(wttr_resp, Exception) else f"status {wttr_resp.status_code}")
    
    if not isinstance(meteo_resp, Exception) and meteo_resp.status_code == 200:
        meteo_data = meteo_resp.json()
        sources.append(WeatherSourceData(
            source="open-meteo",
            fetched_at=datetime.now(timezone.utc).isoformat(),
            confidence="high",
        ))
        log.info("weather.source.success", source="open-meteo")
    else:
        log.warn("weather.source.failed", source="open-meteo", error=str(meteo_resp) if isinstance(meteo_resp, Exception) else f"status {meteo_resp.status_code}")
    
    # Fetch NWS data (US only, for alerts and official forecast)
    alerts: list[WeatherAlert] = []
    nws_data = None
    
    # Check if location is likely in US (simple heuristic)
    is_us_location = any(x in location.lower() for x in [",tx", ",ny", ",ca", ",fl", ",wa", "usa", "united states"]) or \
                     (30 < resolved_lat < 50 and -125 < resolved_lon < -65)
    
    if is_us_location:
        try:
            alerts = await fetch_nws_alerts(resolved_lat, resolved_lon)
            if alerts:
                sources.append(WeatherSourceData(
                    source="nws-alerts",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    confidence="high",
                ))
            
            nws_data = await fetch_nws_forecast(resolved_lat, resolved_lon)
            if nws_data:
                sources.append(WeatherSourceData(
                    source="nws-forecast",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    confidence="high",
                ))
        except Exception as exc:
            log.warn("weather.nws.failed", error=str(exc))
    
    # Fetch PWS data from Synoptic (optional layer - gracefully skips if no token)
    pws_layer: PWSLayerSummary | None = None
    try:
        # Load PWS config
        pws_config = {}
        try:
            from life_dashboard.config import load_config
            pws_config = load_config().get("pws", {})
        except Exception:
            pass
        
        # Get token from env or config
        token_env = pws_config.get("synoptic_token_env", "SYNOPTIC_API_TOKEN")
        api_token = os.environ.get(token_env)
        
        radius = pws_config.get("search_radius_miles", 15.0)
        max_age = pws_config.get("max_station_age_minutes", 60.0)
        min_consensus = pws_config.get("min_consensus_stations", 2)
        
        pws_layer = await fetch_pws_layer(
            lat=resolved_lat,
            lon=resolved_lon,
            api_token=api_token,
            radius_miles=radius,
            max_age_minutes=max_age,
            min_consensus_stations=min_consensus,
        )
        
        if pws_layer.fetch_success and pws_layer.stations_with_data > 0:
            sources.append(WeatherSourceData(
                source="synoptic-pws",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                confidence=pws_layer.consensus.confidence if pws_layer.consensus else "medium",
            ))
            log.info("weather.pws.success", 
                    stations=pws_layer.stations_with_data,
                    confidence=pws_layer.consensus.confidence if pws_layer.consensus else "none")
        elif not pws_layer.api_token_configured:
            log.debug("weather.pws.skipped", reason="no_api_token")
        else:
            log.warn("weather.pws.failed", error=pws_layer.fetch_error)
            
    except Exception as exc:
        log.warn("weather.pws.error", error=str(exc))
        pws_layer = PWSLayerSummary(
            enabled=False,
            fetch_success=False,
            fetch_error=str(exc),
        )

    # ── New official source layers ────────────────────────────────────────────

    # METAR / ASOS official aviation observations
    metar_layer: METARLayerSummary | None = None
    try:
        # Resolve WFO from config or via NWS points API
        wfo_office: str | None = None
        try:
            from life_dashboard.config import load_config
            cfg = load_config()
            wfo_office = cfg.get("weather", {}).get("nws_wfo")
        except Exception:
            pass

        metar_layer = await fetch_metar_layer(
            lat=resolved_lat,
            lon=resolved_lon,
            wfo_office=wfo_office,
        )
        if metar_layer.fetch_success and metar_layer.stations_responded > 0:
            sources.append(WeatherSourceData(
                source="nws-metar",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                confidence=metar_layer.confidence,
            ))
            log.info("weather.metar.success",
                     stations=metar_layer.stations_responded,
                     confidence=metar_layer.confidence)
        else:
            log.debug("weather.metar.skipped",
                      success=metar_layer.fetch_success,
                      error=metar_layer.fetch_error)
    except Exception as exc:
        log.warn("weather.metar.error", error=str(exc))
        metar_layer = METARLayerSummary(enabled=False, fetch_success=False, fetch_error=str(exc))

    # NWS Area Forecast Discussion
    afd_layer: AFDSummary | None = None
    try:
        afd_layer = await fetch_afd(lat=resolved_lat, lon=resolved_lon)
        if afd_layer.fetch_success:
            sources.append(WeatherSourceData(
                source="nws-afd",
                fetched_at=datetime.now(timezone.utc).isoformat(),
                confidence=afd_layer.parse_confidence,
            ))
            log.info("weather.afd.success",
                     wfo=afd_layer.wfo_office,
                     forecaster=afd_layer.forecaster,
                     confidence=afd_layer.parse_confidence)
        else:
            log.debug("weather.afd.skipped", error=afd_layer.fetch_error)
    except Exception as exc:
        log.warn("weather.afd.error", error=str(exc))
        afd_layer = AFDSummary(enabled=False, fetch_success=False, fetch_error=str(exc))

    # SPC convective outlook (only during active severe weather season — March-October)
    spc_outlook: SPCOutlookSummary | None = None
    try:
        now_month = datetime.now().month
        if 3 <= now_month <= 10:
            spc_outlook = await fetch_spc_outlook(lat=resolved_lat, lon=resolved_lon, day=1)
            if spc_outlook.fetch_success:
                sources.append(WeatherSourceData(
                    source="spc-outlook",
                    fetched_at=datetime.now(timezone.utc).isoformat(),
                    confidence=spc_outlook.confidence,
                ))
                log.info("weather.spc.success",
                         day=spc_outlook.day,
                         risk=spc_outlook.categorical_risk,
                         target_in_area=spc_outlook.target_in_risk_area)
            else:
                log.debug("weather.spc.skipped", error=spc_outlook.fetch_error)
        else:
            log.debug("weather.spc.off_season", month=now_month)
    except Exception as exc:
        log.warn("weather.spc.error", error=str(exc))
        spc_outlook = SPCOutlookSummary(enabled=False, fetch_success=False, fetch_error=str(exc))

    # Build the report
    current = _build_current_weather(wttr_data, meteo_data)
    forecast = _build_forecast(wttr_data, meteo_data, nws_data)
    hourly = _build_hourly_forecast(meteo_data)
    
    # Add hourly to today's forecast
    if forecast and hourly:
        today = datetime.now().strftime("%Y-%m-%d")
        # Filter hourly for today only
        today_hourly = [h for h in hourly if h.hour.startswith(today)]
        forecast[0].hourly = today_hourly[:12]  # Next 12 hours
    
    # Blend PWS data into current conditions conservatively
    blend_influence_pct = 0.0
    if pws_layer and pws_layer.fetch_success:
        current_dict = current.model_dump()
        blended_dict, blend_influence_pct = blend_pws_into_current(current_dict, pws_layer)
        current = CurrentWeather(**blended_dict)
        
        # Update PWS layer with blend info
        pws_layer.blended_into_current = blend_influence_pct > 0
        pws_layer.blend_influence_pct = blend_influence_pct

    report = MeteorologyReport(
        location=location,
        location_coords={"lat": resolved_lat, "lon": resolved_lon},
        generated_at=datetime.now(timezone.utc).isoformat(),
        current=current,
        forecast=forecast,
        hourly_forecast=hourly[:12],  # First 12 hours for summary
        alerts=alerts,
        regional_signals=[],  # Placeholder for AQ/pollen data
        pws_layer=pws_layer,
        metar_layer=metar_layer,
        afd_layer=afd_layer,
        spc_outlook=spc_outlook,
        sources=sources,
    )
    
    # LLM synthesis
    if enable_llm:
        config = {}
        if db:
            try:
                from life_dashboard.config import load_config
                config = load_config()
            except Exception:
                pass
        
        # Check for API keys
        minimax_key = os.environ.get("MINIMAX_API_KEY")
        openrouter_key = os.environ.get("OPENROUTER_API_KEY")
        llm_api_key = os.environ.get("LLM_API_KEY")
        
        # Determine provider
        provider = config.get("meteorology", {}).get("llm_provider") or ("minimax" if minimax_key else "openrouter")
        api_key = minimax_key if provider == "minimax" else (openrouter_key or llm_api_key)
        
        if api_key:
            try:
                synthesis = await synthesize_meteorology_narrative(report, api_key, provider)
                report.narrative = synthesis.get("narrative")
                report.narrative_summary = synthesis.get("narrative_summary", "")
                report.tts_script = synthesis.get("tts_script", "")
                report.narrative_sections = synthesis.get("sections", {})
                report.llm_synthesis_enabled = True
                report.llm_model = synthesis.get("model")
                report.llm_reasoning = synthesis.get("reasoning")
                report.synthesis_duration_ms = synthesis.get("duration_ms")
                log.info("weather.llm.synthesis_complete", model=report.llm_model, duration_ms=report.synthesis_duration_ms)
            except Exception as exc:
                log.warn("weather.llm.synthesis_failed", error=str(exc))
                # Fallback to deterministic
                fallback = _generate_fallback_narrative(report)
                report.narrative = fallback["narrative"]
                report.narrative_summary = fallback["summary"]
                report.tts_script = fallback.get("tts_script", fallback["summary"])
                report.narrative_sections = fallback["sections"]
        else:
            log.info("weather.llm.no_api_key", message="No LLM API key found, using fallback narrative")
            fallback = _generate_fallback_narrative(report)
            report.narrative = fallback["narrative"]
            report.narrative_summary = fallback["summary"]
            report.tts_script = ""
            report.narrative_sections = fallback["sections"]
    
    # Legacy summary for backwards compatibility
    report.summary = report.get_summary()
    
    # Persist to database
    if db:
        run_id = db.start_run("weather")
        try:
            report_id = db.insert_report(
                rtype="meteorology",
                title=f"Meteorology Report — {location}",
                content=report.model_dump(),
                generated_at=datetime.now(timezone.utc),
                summary=report.get_summary(),
                source_tag="weather",
                raw_inputs=[s.source for s in sources],
            )
            db.set_state("current_weather", report.to_glanceable().model_dump())
            db.set_state("latest_meteorology_report", report.model_dump())
            db.set_state("latest_meteorology_report_id", report_id)
            db.finish_run(run_id, "success", report_id)
            log.info("weather.pipeline.done", report_id=report_id)
        except Exception as exc:
            db.finish_run(run_id, "error", error=str(exc))
            raise
    else:
        log.info("weather.pipeline.done", db=False)
    
    return report
