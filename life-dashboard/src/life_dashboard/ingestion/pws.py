"""Personal Weather Station (PWS) ingestion layer using Synoptic Data API.

This module provides a fourth weather data source layer alongside wttr.in,
Open-Meteo, and NWS. It fetches observations from nearby public/community
weather stations and provides consensus-based blending.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from life_dashboard.reports import (
    PWSConsensusSummary,
    PWSStationObservation,
    PWSLayerSummary,
    WeatherSourceData,
)

log = structlog.get_logger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

SYNOPTIC_API_BASE = "https://api.synopticdata.com/v2"
DEFAULT_RADIUS_MILES = 15.0
DEFAULT_MAX_AGE_MINUTES = 60.0
DEFAULT_MIN_CONSENSUS_STATIONS = 2

# Conversion constants
MILES_TO_KM = 1.60934
MPS_TO_MPH = 2.23694
HPA_TO_INHG = 0.02953


# ── API Client ────────────────────────────────────────────────────────────────

class SynopticClient:
    """Client for Synoptic Data API."""
    
    def __init__(self, api_token: str | None = None):
        self.api_token = api_token or os.environ.get("SYNOPTIC_API_TOKEN")
        self.client = httpx.AsyncClient(timeout=15.0)
    
    def is_configured(self) -> bool:
        """Check if API token is available."""
        return bool(self.api_token)
    
    async def fetch_nearby_stations(
        self,
        lat: float,
        lon: float,
        radius_miles: float = DEFAULT_RADIUS_MILES,
        max_age_minutes: float = DEFAULT_MAX_AGE_MINUTES,
    ) -> dict[str, Any] | None:
        """Fetch nearby weather stations with recent observations.
        
        Uses the stations/nearesttime endpoint to get the most recent
        observations from stations within the specified radius.
        """
        if not self.api_token:
            log.debug("synoptic.no_token")
            return None
        
        url = f"{SYNOPTIC_API_BASE}/stations/nearesttime"
        params = {
            "token": self.api_token,
            "radius": f"{lat},{lon},{radius_miles}",
            "limit": 20,  # Max stations to return
            "vars": "air_temp,relative_humidity,wind_speed,wind_direction,pressure",
            "within": int(max_age_minutes),  # Only stations with obs within this many minutes
            "units": "english",
            "status": "active",
        }
        
        try:
            resp = await self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            # Check for API errors
            if data.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
                error_msg = data.get("SUMMARY", {}).get("RESPONSE_MESSAGE", "Unknown error")
                log.warn("synoptic.api_error", error=error_msg)
                return None
            
            log.info("synoptic.stations_fetched", 
                    station_count=len(data.get("STATION", [])),
                    lat=lat, lon=lon, radius_miles=radius_miles)
            return data
            
        except httpx.HTTPStatusError as e:
            log.warn("synoptic.http_error", status=e.response.status_code, error=str(e))
            return None
        except Exception as e:
            log.warn("synoptic.fetch_failed", error=str(e))
            return None
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()


# ── Data Transformation ───────────────────────────────────────────────────────

def _calculate_freshness(age_minutes: float) -> str:
    """Determine freshness category based on observation age."""
    if age_minutes < 15:
        return "fresh"
    elif age_minutes < 45:
        return "recent"
    return "stale"


def _calculate_quality_score(age_minutes: float, distance_miles: float) -> float:
    """Calculate a quality score (0-1) based on age and distance.
    
    Closer and fresher stations get higher scores.
    """
    # Age factor: 1.0 at 0 min, 0.5 at 60 min, 0.0 at 120 min
    age_factor = max(0, 1 - (age_minutes / 120))
    
    # Distance factor: 1.0 at 0 miles, 0.5 at 15 miles, 0.0 at 30 miles
    distance_factor = max(0, 1 - (distance_miles / 30))
    
    # Weight age more heavily (fresh data from farther away is better than stale close data)
    return (age_factor * 0.6) + (distance_factor * 0.4)


def _parse_wind_direction(degrees: float | None) -> str | None:
    """Convert wind direction degrees to cardinal direction."""
    if degrees is None:
        return None
    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    index = round(degrees / 22.5) % 16
    return directions[index]


def _extract_sensor_value(
    station_data: dict,
    var_name: str,
    sensor_set: int = 1,
) -> float | None:
    """Extract a sensor value from station data.
    
    Synoptic returns sensors as lists with the sensor number as index.
    """
    sensors = station_data.get("SENSOR_VARIABLES", {})
    if var_name not in sensors:
        return None
    
    # Get the observation for this variable
    obs = station_data.get("OBSERVATIONS", {})
    if var_name not in obs:
        return None
    
    var_data = obs[var_name]
    
    # Handle both list and dict formats
    if isinstance(var_data, list) and len(var_data) >= sensor_set:
        value_data = var_data[sensor_set - 1]
        if isinstance(value_data, dict):
            return value_data.get("value")
        return value_data
    elif isinstance(var_data, dict):
        return var_data.get("value")
    
    return None


def _extract_observation_time(station_data: dict) -> str | None:
    """Extract the observation timestamp from station data."""
    obs = station_data.get("OBSERVATIONS", {})
    
    # Try to get time from air_temp observation
    if "air_temp" in obs:
        temp_data = obs["air_temp"]
        if isinstance(temp_data, list) and temp_data:
            return temp_data[0].get("date_time")
        elif isinstance(temp_data, dict):
            return temp_data.get("date_time")
    
    # Fallback to any available observation time
    for var_name, var_data in obs.items():
        if isinstance(var_data, list) and var_data:
            return var_data[0].get("date_time")
        elif isinstance(var_data, dict):
            return var_data.get("date_time")
    
    return None


def _transform_station_data(
    station_data: dict,
    target_lat: float,
    target_lon: float,
) -> PWSStationObservation | None:
    """Transform raw Synoptic station data into our model."""
    try:
        station_id = station_data.get("STID", "UNKNOWN")
        name = station_data.get("NAME", "Unknown Station")
        
        # Get location
        lat_raw = station_data.get("LATITUDE")
        lon_raw = station_data.get("LONGITUDE")
        
        if lat_raw is None or lon_raw is None:
            log.debug("synoptic.station_no_location", station_id=station_id)
            return None
        
        try:
            lat = float(lat_raw)
            lon = float(lon_raw)
        except (TypeError, ValueError):
            log.debug("synoptic.station_bad_location", station_id=station_id, lat=lat_raw, lon=lon_raw)
            return None
        
        if lat is None or lon is None:
            log.debug("synoptic.station_no_location", station_id=station_id)
            return None
        
        # Calculate distance (simplified haversine would be better, but this is approximate)
        import math
        
        # Convert to radians
        lat1, lon1 = math.radians(target_lat), math.radians(target_lon)
        lat2, lon2 = math.radians(lat), math.radians(lon)
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 3956  # Earth's radius in miles
        distance_miles = c * r
        
        # Get observation time
        obs_time_str = _extract_observation_time(station_data)
        if not obs_time_str:
            log.debug("synoptic.station_no_time", station_id=station_id)
            return None
        
        # Parse observation time and calculate age
        try:
            # Synoptic returns ISO format like "2025-03-24T01:00:00Z"
            obs_time = datetime.fromisoformat(obs_time_str.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            age_minutes = (now - obs_time).total_seconds() / 60
        except Exception:
            log.debug("synoptic.time_parse_error", station_id=station_id, time_str=obs_time_str)
            age_minutes = 999  # Mark as very old if we can't parse
        
        # Extract sensor values
        temp_f = _extract_sensor_value(station_data, "air_temp")
        humidity = _extract_sensor_value(station_data, "relative_humidity")
        wind_speed = _extract_sensor_value(station_data, "wind_speed")
        wind_dir_degrees = _extract_sensor_value(station_data, "wind_direction")
        pressure = _extract_sensor_value(station_data, "pressure")
        
        # Calculate derived values
        wind_direction = _parse_wind_direction(wind_dir_degrees)
        
        # Convert pressure from hPa to inHg if present (Synoptic returns mb/hPa by default, 
        # but we're requesting english units)
        pressure_in = None
        if pressure:
            # If pressure is > 100, it's likely in hPa/mb, convert to inHg
            if pressure > 100:
                pressure_in = pressure * HPA_TO_INHG
            else:
                pressure_in = pressure  # Already in inches
        
        # Calculate dewpoint if we have temp and humidity
        dewpoint_f = None
        if temp_f is not None and humidity is not None:
            # Simple dewpoint approximation
            # Td = T - ((100 - RH) / 5)
            dewpoint_f = temp_f - ((100 - humidity) / 5)
        
        freshness = _calculate_freshness(age_minutes)
        quality_score = _calculate_quality_score(age_minutes, distance_miles)
        
        return PWSStationObservation(
            station_id=station_id,
            provider="synoptic",
            name=name,
            distance_miles=round(distance_miles, 2),
            latitude=lat,
            longitude=lon,
            temp_f=round(temp_f, 1) if temp_f else None,
            humidity_pct=round(humidity, 0) if humidity else None,
            wind_mph=round(wind_speed, 1) if wind_speed else None,
            wind_direction=wind_direction,
            pressure_in=round(pressure_in, 2) if pressure_in else None,
            dewpoint_f=round(dewpoint_f, 1) if dewpoint_f else None,
            observation_time=obs_time_str,
            observation_age_minutes=round(age_minutes, 1),
            freshness=freshness,  # type: ignore
            quality_score=round(quality_score, 2),
            raw_sensor_data={
                "wind_direction_degrees": wind_dir_degrees,
                "source_vars": list(station_data.get("SENSOR_VARIABLES", {}).keys()),
            },
        )
        
    except Exception as e:
        log.warn("synoptic.transform_error", station_id=station_data.get("STID"), error=str(e))
        return None


# ── Consensus Calculation ─────────────────────────────────────────────────────

def _calculate_consensus(
    stations: list[PWSStationObservation],
    min_consensus_stations: int = DEFAULT_MIN_CONSENSUS_STATIONS,
) -> PWSConsensusSummary:
    """Calculate consensus values across multiple PWS stations.
    
    Uses weighted averaging based on quality scores and identifies
    outlier stations that may be reporting erroneous data.
    """
    if len(stations) < min_consensus_stations:
        return PWSConsensusSummary(
            station_count=len(stations),
            agreeing_stations=len(stations),
            confidence="none",
            confidence_reason=f"Need at least {min_consensus_stations} stations for consensus",
        )
    
    # Filter to fresh/recent stations only
    usable_stations = [
        s for s in stations 
        if s.freshness in ("fresh", "recent") and s.quality_score > 0.3
    ]
    
    if len(usable_stations) < min_consensus_stations:
        return PWSConsensusSummary(
            station_count=len(stations),
            agreeing_stations=len(usable_stations),
            confidence="low",
            confidence_reason="Insufficient fresh station data",
        )
    
    def _weighted_average(values: list[tuple[float, float]]) -> float | None:
        """Calculate weighted average. Values are (value, weight) tuples."""
        if not values:
            return None
        total_weight = sum(w for _, w in values)
        if total_weight == 0:
            return None
        return sum(v * w for v, w in values) / total_weight
    
    def _calculate_std_dev(values: list[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    # Collect weighted values
    temps = [(s.temp_f, s.quality_score) for s in usable_stations if s.temp_f is not None]
    humidities = [(s.humidity_pct, s.quality_score) for s in usable_stations if s.humidity_pct is not None]
    winds = [(s.wind_mph, s.quality_score) for s in usable_stations if s.wind_mph is not None]
    pressures = [(s.pressure_in, s.quality_score) for s in usable_stations if s.pressure_in is not None]
    
    # Calculate consensus values
    consensus_temp = _weighted_average(temps)
    consensus_humidity = _weighted_average(humidities)
    consensus_wind = _weighted_average(winds)
    consensus_pressure = _weighted_average(pressures)
    
    # Calculate agreement metrics
    temp_values = [s.temp_f for s in usable_stations if s.temp_f is not None]
    temp_std = _calculate_std_dev(temp_values) if temp_values else 0
    
    # A 3°F std dev is considered good agreement for outdoor sensors
    good_agreement_threshold = 3.0
    agreeing_count = sum(
        1 for s in usable_stations
        if s.temp_f is not None and consensus_temp is not None
        and abs(s.temp_f - consensus_temp) <= good_agreement_threshold
    )
    
    # Calculate overall disagreement percentage (0-100)
    # Higher when std dev is high or few stations agree
    if consensus_temp:
        disagreement_pct = min(100, (temp_std / 5) * 100)  # 5°F std dev = 100% disagreement
    else:
        disagreement_pct = 100.0
    
    # Determine confidence level
    confidence: Literal["high", "medium", "low", "none"] = "none"
    confidence_reason = ""
    
    fresh_count = sum(1 for s in usable_stations if s.freshness == "fresh")
    
    if len(usable_stations) >= 4 and fresh_count >= 2 and disagreement_pct < 20:
        confidence = "high"
        confidence_reason = f"{len(usable_stations)} stations in good agreement (σ={temp_std:.1f}°F)"
    elif len(usable_stations) >= 3 and disagreement_pct < 40:
        confidence = "medium"
        confidence_reason = f"{len(usable_stations)} stations with moderate agreement"
    elif len(usable_stations) >= 2:
        confidence = "low"
        confidence_reason = f"Only {len(usable_stations)} stations or limited agreement"
    else:
        confidence = "none"
        confidence_reason = "Insufficient station data for reliable consensus"
    
    return PWSConsensusSummary(
        consensus_temp_f=round(consensus_temp, 1) if consensus_temp else None,
        consensus_humidity_pct=round(consensus_humidity, 0) if consensus_humidity else None,
        consensus_wind_mph=round(consensus_wind, 1) if consensus_wind else None,
        consensus_pressure_in=round(consensus_pressure, 2) if consensus_pressure else None,
        station_count=len(stations),
        agreeing_stations=agreeing_count,
        disagreement_pct=round(disagreement_pct, 1),
        confidence=confidence,
        confidence_reason=confidence_reason,
    )


# ── Main Ingestion Function ───────────────────────────────────────────────────

async def fetch_pws_layer(
    lat: float,
    lon: float,
    api_token: str | None = None,
    radius_miles: float = DEFAULT_RADIUS_MILES,
    max_age_minutes: float = DEFAULT_MAX_AGE_MINUTES,
    min_consensus_stations: int = DEFAULT_MIN_CONSENSUS_STATIONS,
) -> PWSLayerSummary:
    """Fetch and process PWS data from Synoptic Data API.
    
    This is the main entry point for PWS ingestion. It returns a complete
    PWSLayerSummary that can be added to the MeteorologyReport.
    
    If no API token is configured, returns a disabled summary gracefully.
    """
    client = SynopticClient(api_token)
    
    # Check if PWS layer is enabled
    summary = PWSLayerSummary(
        enabled=True,
        api_token_configured=client.is_configured(),
        search_radius_miles=radius_miles,
        target_latitude=lat,
        target_longitude=lon,
    )
    
    if not client.is_configured():
        summary.enabled = False
        summary.fetch_error = "Synoptic API token not configured"
        log.info("pws.layer_skipped", reason="no_api_token")
        return summary
    
    try:
        # Fetch stations from Synoptic
        raw_data = await client.fetch_nearby_stations(
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            max_age_minutes=max_age_minutes,
        )
        
        if raw_data is None:
            summary.fetch_success = False
            summary.fetch_error = "Failed to fetch from Synoptic API"
            log.warn("pws.fetch_failed")
            return summary
        
        # Transform station data
        raw_stations = raw_data.get("STATION", [])
        summary.stations_found = len(raw_stations)
        
        stations: list[PWSStationObservation] = []
        for station_data in raw_stations:
            station = _transform_station_data(station_data, lat, lon)
            if station:
                stations.append(station)
        
        # Sort by quality score (best first)
        stations.sort(key=lambda s: s.quality_score, reverse=True)
        
        summary.stations = stations
        summary.stations_with_data = len(stations)
        summary.fetch_success = True
        
        # Calculate consensus
        summary.consensus = _calculate_consensus(stations, min_consensus_stations)
        
        log.info("pws.layer_complete",
                stations_found=summary.stations_found,
                stations_valid=summary.stations_with_data,
                consensus_confidence=summary.consensus.confidence,
                consensus_stations=summary.consensus.agreeing_stations)
        
        return summary
        
    except Exception as e:
        summary.fetch_success = False
        summary.fetch_error = str(e)
        log.error("pws.layer_error", error=str(e))
        return summary
    finally:
        await client.close()


# ── Blending ──────────────────────────────────────────────────────────────────

def blend_pws_into_current(
    current: dict[str, Any],
    pws_layer: PWSLayerSummary,
    blend_threshold: float = 0.5,  # Minimum confidence to blend
) -> tuple[dict[str, Any], float]:
    """Blend PWS consensus data into current conditions.
    
    Returns the updated current conditions dict and the blend influence percentage.
    The influence percentage indicates how much weight was given to PWS vs other sources.
    
    Blending is conservative:
    - Only blends if consensus confidence is medium or high
    - Only blends if at least 2 stations agree
    - Temperature gets the most weight (PWS sensors are typically accurate for temp)
    - Other fields get less weight or are not blended
    """
    if not pws_layer or not pws_layer.consensus:
        return current, 0.0
    
    consensus = pws_layer.consensus
    
    # Check if we should blend at all
    if not consensus.is_usable_for_blend():
        log.debug("pws.blend_skipped", 
                 confidence=consensus.confidence,
                 agreeing=consensus.agreeing_stations)
        return current, 0.0
    
    # Calculate blend weight based on confidence
    # High confidence = 30% PWS, 70% other sources
    # Medium confidence = 15% PWS, 85% other sources
    if consensus.confidence == "high":
        pws_weight = 0.30
    elif consensus.confidence == "medium":
        pws_weight = 0.15
    else:
        pws_weight = 0.0
    
    other_weight = 1 - pws_weight
    
    updated = dict(current)  # Copy to avoid modifying original
    
    # Blend temperature (most reliable from PWS)
    if consensus.consensus_temp_f is not None and "temp_f" in current:
        current_temp = current["temp_f"]
        if isinstance(current_temp, (int, float)):
            updated["temp_f"] = round(
                current_temp * other_weight + consensus.consensus_temp_f * pws_weight,
                1
            )
            # Also update feels_like if it exists
            if "feels_like_f" in current and current["feels_like_f"]:
                # Adjust feels_like proportionally to temp change
                temp_diff = updated["temp_f"] - current_temp
                updated["feels_like_f"] = round(current["feels_like_f"] + temp_diff, 1)
    
    # Blend humidity (moderately reliable)
    if consensus.consensus_humidity_pct is not None and "humidity_pct" in current:
        current_humidity = current["humidity_pct"]
        if isinstance(current_humidity, (int, float)):
            # Use less weight for humidity (PWS humidity sensors vary more)
            humidity_weight = pws_weight * 0.5
            updated["humidity_pct"] = int(round(
                current_humidity * (1 - humidity_weight) + 
                consensus.consensus_humidity_pct * humidity_weight
            ))
    
    # For pressure and wind, just note the values but don't blend strongly
    # These are more variable and model data is often better
    
    influence_pct = round(pws_weight * 100, 0)
    
    log.info("pws.blended",
            weight=pws_weight,
            influence_pct=influence_pct,
            consensus_temp=consensus.consensus_temp_f,
            original_temp=current.get("temp_f"),
            blended_temp=updated.get("temp_f"))
    
    return updated, influence_pct
