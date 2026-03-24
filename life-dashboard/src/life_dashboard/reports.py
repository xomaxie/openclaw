"""Enhanced report schemas for meteorology with rich data model."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


class ReportType(str, Enum):
    METEOROLOGY = "meteorology"
    DAILY = "daily"
    SITE = "site"


# ── Meteorology ──────────────────────────────────────────────────────────────

class CurrentWeather(BaseModel):
    temp_f: float
    condition: str
    humidity_pct: int = Field(ge=0, le=100)
    wind_mph: float
    wind_direction: str = "N"
    uv_index: float = Field(ge=0, le=11, default=0.0)
    pressure_in: float | None = None
    visibility_mi: float | None = None
    dewpoint_f: float | None = None
    feels_like_f: float | None = None


class HourlyForecast(BaseModel):
    hour: str  # ISO timestamp or HH:MM
    temp_f: float
    condition: str
    precip_chance: int = Field(ge=0, le=100, default=0)
    wind_mph: float | None = None


class DayForecast(BaseModel):
    day: str  # YYYY-MM-DD or weekday name
    date_iso: str | None = None
    high_f: float
    low_f: float
    condition: str
    precip_pct: int = Field(ge=0, le=100, description="Probability of precipitation %")
    hourly: list[HourlyForecast] = Field(default_factory=list)


class WeatherAlert(BaseModel):
    title: str
    severity: Literal["minor", "moderate", "severe", "extreme"]
    description: str
    effective: str | None = None
    expires: str | None = None
    instruction: str | None = None
    source: str  # e.g., "NWS"
    url: str | None = None


class RegionalSignal(BaseModel):
    name: str
    value: str | float
    unit: str | None = None
    trend: Literal["rising", "falling", "stable"] | None = None
    description: str | None = None


class WeatherSourceData(BaseModel):
    source: str  # e.g., "wttr.in", "open-meteo", "nws", "synoptic-pws"
    fetched_at: str
    raw_data: dict[str, Any] | None = None
    confidence: Literal["high", "medium", "low"] = "medium"


# ── METAR / Aviation Observation Models ─────────────────────────────────────

class METARStationObservation(BaseModel):
    """Individual METAR station observation from NWS API."""
    station_id: str
    station_name: str
    distance_miles: float  # from target location
    latitude: float
    longitude: float

    # Core observation fields
    text_description: str | None = None  # e.g. "Clear", "Partly Cloudy"
    temp_f: float | None = None
    dewpoint_f: float | None = None
    humidity_pct: float | None = None
    wind_speed_mph: float | None = None
    wind_direction: str | None = None  # cardinal
    wind_gust_mph: float | None = None
    pressure_in: float | None = None
    visibility_mi: float | None = None
    ceiling_ft: float | None = None  # cloud ceiling if present

    # Raw METAR string (from NWS 'raw_observation' field)
    raw_metar: str | None = None

    # Metadata
    observation_time: str | None = None  # ISO
    observation_age_minutes: float | None = None
    freshness: Literal["fresh", "recent", "stale"] = "fresh"


class METARLayerSummary(BaseModel):
    """Summary of METAR observations from nearby ASOS/ATCT stations."""
    enabled: bool = False
    fetch_success: bool = False
    fetch_error: str | None = None

    target_latitude: float | None = None
    target_longitude: float | None = None

    # List of nearby stations queried
    stations: list[METARStationObservation] = Field(default_factory=list)
    stations_responded: int = 0  # how many returned data

    # If multiple stations, consensus values
    consensus_temp_f: float | None = None
    consensus_humidity_pct: float | None = None
    consensus_wind_mph: float | None = None
    consensus_visibility_mi: float | None = None
    confidence: Literal["high", "medium", "low", "none"] = "none"
    confidence_reason: str = ""


# ── NWS Area Forecast Discussion (AFD) Models ────────────────────────────────

class AFDSummary(BaseModel):
    """NWS/NOAA Area Forecast Discussion — structured excerpt."""
    enabled: bool = False
    fetch_success: bool = False
    fetch_error: str | None = None

    # WFO identifier (e.g. "JAN", "EWX")
    wfo_office: str | None = None
    product_id: str | None = None  # NWS product UUID

    # Issuance metadata
    issuance_time: str | None = None  # ISO
    valid_time: str | None = None  # ISO
    expire_time: str | None = None  # ISO
    forecaster: str | None = None

    # Structured excerpt fields (always populated from LLM-extracted text)
    synopsis: str = ""  # 1-2 sentence overview
    short_term_summary: str = ""  # next 12-24h
    extended_summary: str = ""  # days 3-7
    concerns: str = ""  # any specific concerns mentioned

    # Raw first ~500 chars of discussion for transparency
    raw_excerpt: str = ""

    # Confidence that this AFD was successfully parsed
    parse_confidence: Literal["high", "medium", "low", "none"] = "none"


# ── SPC Convective Outlook Models ────────────────────────────────────────────

class SPCRiskArea(BaseModel):
    """Individual risk polygon from SPC outlook."""
    risk_type: Literal["TSTM", "MRGL", "SLGT", "ENH", "MDT", "HIGH", "TOTAL"] = "TSTM"
    label: str = ""  # e.g. "General Thunderstorms Risk"
    label2: str = ""  # secondary label
    # DN value: 0=TSTM, 1=MRGL, 2=SLGT, 3=ENH, 4=MDT, 5=HIGH
    dn_value: int = 0
    fill_color: str | None = None
    has_geometry: bool = False  # whether risk polygon exists (empty = no risk)


class SPCOutlookSummary(BaseModel):
    """Storm Prediction Center Day 1/2/3 convective outlook — structured."""
    enabled: bool = False
    fetch_success: bool = False
    fetch_error: str | None = None

    # Outlook day: 1, 2, or 3
    day: int = 1

    # Issuance metadata
    issuance_time: str | None = None  # ISO
    valid_time: str | None = None
    expire_time: str | None = None
    forecaster: str = ""

    # Overall categorical risk for the target area
    # DN values: 0=TSTM, 1=MRGL, 2=SLGT, 3=ENH, 4=MDT, 5=HIGH
    categorical_risk: Literal["TSTM", "MRGL", "SLGT", "ENH", "MDT", "HIGH", "NONE"] = "NONE"
    categorical_label: str = ""  # "General Thunderstorms Risk"
    categorical_dn: int = 0  # raw DN value

    # Per-hazard risk areas
    tornado: SPCRiskArea | None = None
    hail: SPCRiskArea | None = None
    wind: SPCRiskArea | None = None

    # Whether target location is covered by any risk polygon
    target_in_risk_area: bool = False
    target_risk_description: str = ""  # e.g. "Marginal Risk of Severe Thunderstorms"

    # Confidence in the outlook fetch
    confidence: Literal["high", "medium", "low", "none"] = "none"


# ── Personal Weather Station (PWS) Models ───────────────────────────────────

class PWSStationObservation(BaseModel):
    """Individual PWS station observation."""
    station_id: str
    provider: str  # e.g., "synoptic", "weatherflow", "ambient"
    name: str
    distance_miles: float
    latitude: float
    longitude: float
    
    # Observation data
    temp_f: float | None = None
    humidity_pct: float | None = None
    wind_mph: float | None = None
    wind_direction: str | None = None
    pressure_in: float | None = None
    dewpoint_f: float | None = None
    
    # Metadata
    observation_time: str  # ISO timestamp
    observation_age_minutes: float
    freshness: Literal["fresh", "recent", "stale"] = "fresh"
    quality_score: float = Field(ge=0, le=1, default=0.5)  # 0-1 based on age/distance
    
    # Raw API data (for debugging/transparency)
    raw_sensor_data: dict[str, Any] | None = None


class PWSConsensusSummary(BaseModel):
    """Summary of consensus across multiple PWS stations."""
    # Consensus values (computed from multiple stations)
    consensus_temp_f: float | None = None
    consensus_humidity_pct: float | None = None
    consensus_wind_mph: float | None = None
    consensus_pressure_in: float | None = None
    
    # Agreement metrics
    station_count: int = 0
    agreeing_stations: int = 0  # Stations within reasonable tolerance
    disagreement_pct: float = 0.0  # Variance between stations
    
    # Confidence assessment
    confidence: Literal["high", "medium", "low", "none"] = "none"
    confidence_reason: str = ""
    
    def is_usable_for_blend(self) -> bool:
        """Check if consensus is reliable enough to blend into current conditions."""
        return self.confidence in ("high", "medium") and self.agreeing_stations >= 2


class PWSLayerSummary(BaseModel):
    """Complete PWS layer summary for the report."""
    enabled: bool = False
    api_token_configured: bool = False
    fetch_success: bool = False
    fetch_error: str | None = None
    
    # Search parameters
    search_radius_miles: float = 15.0
    target_latitude: float | None = None
    target_longitude: float | None = None
    
    # Results
    stations_found: int = 0
    stations_with_data: int = 0
    stations: list[PWSStationObservation] = Field(default_factory=list)
    
    # Consensus
    consensus: PWSConsensusSummary | None = None
    
    # Blend status
    blended_into_current: bool = False
    blend_influence_pct: float = 0.0  # How much PWS influenced current conditions


class CurrentWeatherSummary(BaseModel):
    """Glanceable weather summary for dashboard display."""
    temp_f: float
    condition: str
    condition_icon: str = "🌤️"
    high_f: float | None = None
    low_f: float | None = None
    precip_chance: int | None = None
    alert_count: int = 0
    alert_severity: Literal["none", "minor", "moderate", "severe", "extreme"] = "none"


class MeteorologyReport(BaseModel):
    """Full meteorology report with LLM narrative and detailed data."""
    type: Literal["meteorology"] = "meteorology"
    location: str
    location_coords: dict[str, float] | None = None  # {"lat": 30.2672, "lon": -97.7431}
    generated_at: str | None = None  # ISO timestamp
    
    # Glanceable current conditions
    current: CurrentWeather
    
    # Forecast data
    forecast: list[DayForecast] = Field(default_factory=list)
    hourly_forecast: list[HourlyForecast] = Field(default_factory=list)
    
    # Alerts and warnings
    alerts: list[WeatherAlert] = Field(default_factory=list)
    
    # Regional signals (air quality, pollen, etc.)
    regional_signals: list[RegionalSignal] = Field(default_factory=list)
    
    # Personal Weather Station layer
    pws_layer: PWSLayerSummary | None = None

    # Official METAR / ASOS observation layer
    metar_layer: METARLayerSummary | None = None

    # NWS Area Forecast Discussion
    afd_layer: AFDSummary | None = None

    # SPC convective outlook context
    spc_outlook: SPCOutlookSummary | None = None

    # Data sources and provenance
    sources: list[WeatherSourceData] = Field(default_factory=list)
    
    # LLM-generated narrative
    narrative: str | None = Field(default=None, description="Full LLM-generated meteorology narrative")
    narrative_summary: str = Field(default="", description="Brief summary/teaser for dashboard")
    tts_script: str = Field(default="", description="TTS-ready spoken weather script")
    narrative_sections: dict[str, str] = Field(default_factory=dict, description="Structured narrative sections")
    
    # Legacy summary field for backwards compatibility
    summary: str = ""
    
    # Metadata
    llm_synthesis_enabled: bool = False
    llm_model: str | None = None
    llm_reasoning: str | None = None
    synthesis_duration_ms: int | None = None
    
    def get_summary(self) -> str:
        """Get the best summary available."""
        if self.narrative_summary:
            return self.narrative_summary
        if self.summary:
            return self.summary
        return f"Current: {self.current.condition}, {self.current.temp_f}°F"
    
    def to_glanceable(self) -> CurrentWeatherSummary:
        """Convert to glanceable summary for dashboard."""
        today = self.forecast[0] if self.forecast else None
        alert_severity = "none"
        for alert in self.alerts:
            if alert.severity == "extreme":
                alert_severity = "extreme"
                break
            elif alert.severity == "severe" and alert_severity not in ("extreme",):
                alert_severity = "severe"
            elif alert.severity == "moderate" and alert_severity not in ("extreme", "severe"):
                alert_severity = "moderate"
            elif alert.severity == "minor" and alert_severity == "none":
                alert_severity = "minor"
        
        return CurrentWeatherSummary(
            temp_f=self.current.temp_f,
            condition=self.current.condition,
            condition_icon=_condition_to_icon(self.current.condition),
            high_f=today.high_f if today else None,
            low_f=today.low_f if today else None,
            precip_chance=today.precip_pct if today else None,
            alert_count=len(self.alerts),
            alert_severity=alert_severity,
        )


def _condition_to_icon(condition: str) -> str:
    """Map condition to emoji icon."""
    c = (condition or "").lower()
    if any(word in c for word in ["sun", "clear", "fair"]):
        return "☀️"
    if any(word in c for word in ["mostly", "mainly clear"]):
        return "🌤️"
    if any(word in c for word in ["partly", "partly cloudy", "scattered"]):
        return "⛅"
    if any(word in c for word in ["cloud", "overcast"]):
        return "☁️"
    if any(word in c for word in ["rain", "drizzle", "showers"]):
        return "🌧️"
    if any(word in c for word in ["storm", "thunder", "lightning"]):
        return "⛈️"
    if any(word in c for word in ["snow", "sleet", "ice", "freezing"]):
        return "❄️"
    if any(word in c for word in ["fog", "mist", "haze"]):
        return "🌫️"
    if any(word in c for word in ["wind", "breezy", "gusty"]):
        return "💨"
    return "🌤️"


# ── Daily Report ──────────────────────────────────────────────────────────────

class EmailSummary(BaseModel):
    from_addr: str
    subject: str
    snippet: str
    date: datetime
    importance: Literal["high", "normal", "low"] = "normal"
    message_id: str | None = None


class IssueSummary(BaseModel):
    repo: str  # e.g. "altoredhealth/api"
    number: int
    title: str
    body: str | None = None
    labels: list[str] = Field(default_factory=list)
    due: date | None = None
    url: str | None = None


class ActionItem(BaseModel):
    text: str
    source: Literal["email", "github", "weather", "site"]


class DailyReport(BaseModel):
    type: Literal["daily"] = "daily"
    date: date
    emails: list[EmailSummary] = Field(default_factory=list)
    issues: list[IssueSummary] = Field(default_factory=list)
    action_items: list[ActionItem] = Field(default_factory=list)
    summary: str = ""

    def model_dump(self, **kwargs):
        # Convert date to string for JSON serialization
        data = super().model_dump(**kwargs)
        if isinstance(data.get("date"), date):
            data["date"] = data["date"].isoformat()
        return data


# ── Site Report ───────────────────────────────────────────────────────────────

class SiteAnalytics(BaseModel):
    visitors: int = 0
    pageviews: int = 0
    top_pages: list[str] = Field(default_factory=list)


class SiteCalls(BaseModel):
    total: int = 0
    missed: int = 0
    avg_duration_sec: float = 0.0


class SiteReport(BaseModel):
    type: Literal["site"] = "site"
    site: str
    date: date
    analytics: SiteAnalytics = Field(default_factory=SiteAnalytics)
    calls: SiteCalls = Field(default_factory=SiteCalls)
    summary: str = ""

    def model_dump(self, **kwargs):
        # Convert date to string for JSON serialization
        data = super().model_dump(**kwargs)
        if isinstance(data.get("date"), date):
            data["date"] = data["date"].isoformat()
        return data


# ── Union type ────────────────────────────────────────────────────────────────

Report = Annotated[MeteorologyReport | DailyReport | SiteReport, Field(discriminator="type")]
