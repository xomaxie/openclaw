/**
 * Compass Weather Report Page
 * Standalone weather report with full meteorology details
 */

const API_BASE = window.location.origin;

// Initialize on load
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

function init() {
  void loadWeatherReport();
  // Auto-refresh every 5 minutes
  setInterval(loadWeatherReport, 300000);
}

async function loadWeatherReport() {
  showLoading();

  try {
    // Check for report ID in query params
    const urlParams = new URLSearchParams(window.location.search);
    const reportId = urlParams.get("id");

    let data;
    if (reportId) {
      // Fetch specific report
      const response = await fetch(`${API_BASE}/api/reports/${reportId}`);
      if (!response.ok) {
        throw new Error("Report not found");
      }
      data = { report: await response.json() };
    } else {
      // Fetch latest report
      const response = await fetch(`${API_BASE}/api/meteorology/latest`);
      if (!response.ok) {
        throw new Error("No weather data available");
      }
      data = await response.json();
    }

    renderWeatherReport(data);
  } catch (error) {
    console.error("Error loading weather report:", error);
    showError();
  }
}

function showLoading() {
  document.getElementById("loading-state").style.display = "block";
  document.getElementById("weather-content").style.display = "none";
  document.getElementById("error-state").style.display = "none";
}

function showError() {
  document.getElementById("loading-state").style.display = "none";
  document.getElementById("weather-content").style.display = "none";
  document.getElementById("error-state").style.display = "block";
}

function renderWeatherReport(data) {
  const report = data.report;
  if (!report) {
    showError();
    return;
  }

  const content = report.content || {};
  const current = content.current || {};
  const forecast = content.forecast || [];
  const hourly = content.hourly_forecast || [];
  const alerts = content.alerts || [];
  const sources = content.sources || [];
  const sections = content.narrative_sections || {};
  const narrative = content.narrative;
  const pwsLayer = content.pws_layer;
  const metarLayer = content.metar_layer;
  const afdLayer = content.afd_layer;
  const spcOutlook = content.spc_outlook;
  const ttsScript = data.tts_script || content.tts_script || "";
  const ttsCacheKey = report.id || report.generated_at || report.created_at || String(Date.now());

  // Update timestamp
  const generatedAt = report.generated_at || report.created_at;
  if (generatedAt) {
    updateFreshness(generatedAt);
  }

  // Render alerts
  renderAlerts(alerts);

  // Render current conditions
  renderCurrentConditions(current, content);

  // Render optional TTS before narrative so the card can account for it
  renderTTS(ttsScript, ttsCacheKey);
  renderNarrative(narrative, sections, spcOutlook);

  // Render hourly forecast
  renderHourly(hourly);

  // Render 3-day forecast
  renderForecast(forecast);

  // Render PWS layer
  renderPWS(pwsLayer);

  // Render METAR / official observations
  renderMETAR(metarLayer);

  // Render AFD / forecast discussion
  renderAFD(afdLayer);

  // Render sources
  renderSources(sources);

  // Show content
  document.getElementById("loading-state").style.display = "none";
  document.getElementById("weather-content").style.display = "block";
  document.getElementById("error-state").style.display = "none";
}

function renderAlerts(alerts) {
  const container = document.getElementById("alerts-section");
  if (!alerts || alerts.length === 0) {
    container.innerHTML = "";
    return;
  }

  container.innerHTML = alerts
    .map(
      (alert) => `
        <div class="alert-banner severity-${alert.severity || "minor"}">
            <div class="alert-title">
                <span>⚠️</span>
                <span>${escapeHtml(alert.title || "Weather Alert")}</span>
            </div>
            <div class="alert-description">${escapeHtml(alert.description || "")}</div>
            ${alert.instruction ? `<div class="alert-meta">${escapeHtml(alert.instruction)}</div>` : ""}
        </div>
    `,
    )
    .join("");
}

function renderCurrentConditions(current, content) {
  // Icon
  document.getElementById("current-icon").textContent = getWeatherIcon(current.condition);

  // Temperature
  document.getElementById("current-temp").textContent =
    current.temp_f !== undefined ? `${Math.round(current.temp_f)}°F` : "--°";

  // Condition
  document.getElementById("current-condition").textContent = current.condition || "--";

  // Summary (if available)
  const summaryEl = document.getElementById("current-summary");
  if (content.narrative_summary) {
    summaryEl.textContent = content.narrative_summary;
  } else {
    summaryEl.textContent = "";
  }

  // Location badge
  if (content.location) {
    document.getElementById("location-badge").textContent = content.location;
  }

  // Details grid
  const details = [
    {
      label: "Humidity",
      value: current.humidity_pct !== undefined ? `${current.humidity_pct}%` : "--",
    },
    {
      label: "Wind",
      value: current.wind_mph
        ? `${current.wind_mph} mph ${current.wind_direction || ""}`.trim()
        : "--",
    },
    { label: "UV Index", value: current.uv_index !== undefined ? current.uv_index : "--" },
    { label: "Pressure", value: current.pressure_in ? `${current.pressure_in}"` : "--" },
    {
      label: "Feels Like",
      value: current.feels_like_f !== undefined ? `${Math.round(current.feels_like_f)}°F` : "--",
    },
    { label: "Visibility", value: current.visibility_mi ? `${current.visibility_mi} mi` : "--" },
  ];

  document.getElementById("details-grid").innerHTML = details
    .map(
      (d) => `
        <div class="detail-item">
            <div class="detail-label">${d.label}</div>
            <div class="detail-value">${d.value}</div>
        </div>
    `,
    )
    .join("");
}

function renderNarrative(narrative, sections, spcOutlook) {
  const card = document.getElementById("narrative-card");
  const container = document.getElementById("narrative-content");
  const spcBadge = document.getElementById("spc-outlook-badge");
  const hasNarrative = !!narrative || Object.keys(sections).length > 0;
  const ttsScript = (document.getElementById("tts-script")?.textContent || "").trim();
  const hasTTS = !!ttsScript;

  if (!hasNarrative && !hasTTS) {
    card.style.display = "none";
    return;
  }

  card.style.display = "block";

  // Render SPC risk badge if there's active convective risk
  if (
    spcOutlook &&
    spcOutlook.fetch_success &&
    spcOutlook.categorical_risk &&
    spcOutlook.categorical_risk !== "TSTM"
  ) {
    const riskClass = "risk-" + spcOutlook.categorical_risk.toLowerCase();
    const riskLabel =
      spcOutlook.categorical_risk +
      " Risk" +
      (spcOutlook.target_in_risk_area ? " — Area Included" : "");
    spcBadge.className = "spc-outlook-badge " + riskClass;
    spcBadge.textContent = "⛈ " + riskLabel;
    spcBadge.style.display = "inline-flex";
  } else if (spcOutlook && spcOutlook.fetch_success && spcOutlook.categorical_risk === "TSTM") {
    spcBadge.className = "spc-outlook-badge risk-tstm";
    spcBadge.textContent = "⛈ General Thunderstorms";
    spcBadge.style.display = "inline-flex";
  } else {
    spcBadge.style.display = "none";
  }

  const orderedSections = [
    ["executive_summary", "📋 Executive Summary"],
    ["current_analysis", "🧭 Current Analysis"],
    ["short_term_forecast", "🔮 Short-Term Forecast"],
    ["extended_outlook", "📅 Extended Outlook"],
    ["recommendations", "💡 Recommendations"],
    ["notable_patterns", "📈 Notable Patterns"],
    ["data_confidence", "✅ Data Confidence"],
  ];

  let html = orderedSections
    .filter(([key]) => sections[key])
    .map(
      ([key, label]) => `
            <div class="narrative-section-title">${label}</div>
            <div class="narrative-section-text">${escapeHtml(sections[key])}</div>
        `,
    )
    .join("");

  // Fallback to full narrative if no structured sections are available
  if (!html && narrative) {
    html += `
            <div class="narrative-section-title">📊 Full Analysis</div>
            <div class="narrative-section-text">${escapeHtml(narrative)}</div>
        `;
  }

  container.innerHTML = html;
}

function renderTTS(ttsScript, ttsCacheKey) {
  const section = document.getElementById("tts-section");
  const scriptEl = document.getElementById("tts-script");
  const audioEl = document.getElementById("tts-audio");
  const playButton = document.getElementById("tts-play-button");
  const statusEl = document.getElementById("tts-status");

  if (!section || !scriptEl || !audioEl || !playButton || !statusEl) {
    return;
  }

  if (!ttsScript) {
    section.style.display = "none";
    audioEl.removeAttribute("src");
    audioEl.load();
    return;
  }

  const nextAudioEl = audioEl.cloneNode(true);
  audioEl.replaceWith(nextAudioEl);
  const nextPlayButton = playButton.cloneNode(true);
  playButton.replaceWith(nextPlayButton);

  section.style.display = "block";
  scriptEl.textContent = ttsScript;
  statusEl.textContent = "Ready";
  nextAudioEl.pause();
  nextAudioEl.removeAttribute("src");
  nextAudioEl.load();

  nextPlayButton.addEventListener("click", async () => {
    nextPlayButton.disabled = true;
    statusEl.textContent = "Loading audio…";

    try {
      const cacheBust = encodeURIComponent(ttsCacheKey || String(Date.now()));
      nextAudioEl.src = `${API_BASE}/api/meteorology/tts?v=${cacheBust}&_=${Date.now()}`;
      nextAudioEl.load();
      await nextAudioEl.play();
      statusEl.textContent = "Playing";
    } catch (error) {
      console.error("Error playing TTS audio:", error);
      statusEl.textContent = "Audio unavailable";
    } finally {
      nextPlayButton.disabled = false;
    }
  });

  nextAudioEl.addEventListener("play", () => {
    statusEl.textContent = "Playing";
  });
  nextAudioEl.addEventListener("pause", () => {
    if (!nextAudioEl.ended) {
      statusEl.textContent = "Paused";
    }
  });
  nextAudioEl.addEventListener("ended", () => {
    statusEl.textContent = "Finished";
  });
  nextAudioEl.addEventListener("error", () => {
    statusEl.textContent = "Audio unavailable";
  });
}

function renderHourly(hourly) {
  const container = document.getElementById("hourly-grid");

  if (!hourly || hourly.length === 0) {
    container.innerHTML =
      '<div style="color: var(--text-tertiary); text-align: center; width: 100%;">No hourly data available</div>';
    return;
  }

  // Show next 24 hours (or all available)
  const hours = hourly.slice(0, 24);

  container.innerHTML = hours
    .map((h) => {
      const time = formatHour(h.hour);
      return `
            <div class="hourly-card">
                <div class="hourly-time">${time}</div>
                <div class="hourly-icon">${getWeatherIcon(h.condition)}</div>
                <div class="hourly-temp">${h.temp_f !== undefined ? Math.round(h.temp_f) + "°" : "--"}</div>
                ${h.precip_chance > 0 ? `<div class="hourly-precip">${h.precip_chance}% 💧</div>` : ""}
            </div>
        `;
    })
    .join("");
}

function renderForecast(forecast) {
  const container = document.getElementById("forecast-grid");

  if (!forecast || forecast.length === 0) {
    container.innerHTML =
      '<div style="color: var(--text-tertiary); text-align: center; width: 100%;">No forecast data available</div>';
    return;
  }

  // Show up to 3 days
  const days = forecast.slice(0, 3);

  container.innerHTML = days
    .map(
      (day) => `
        <div class="forecast-day-card">
            <div class="forecast-day-name">${formatDayName(day.day)}</div>
            <div class="forecast-day-icon">${getWeatherIcon(day.condition)}</div>
            <div class="forecast-day-temps">${Math.round(day.high_f || 0)}° / ${Math.round(day.low_f || 0)}°</div>
            <div class="forecast-day-condition">${day.condition || "--"}</div>
            ${day.precip_pct ? `<div class="forecast-day-precip">${day.precip_pct}% chance of rain</div>` : ""}
        </div>
    `,
    )
    .join("");
}

function renderSources(sources) {
  const container = document.getElementById("sources-grid");

  if (!sources || sources.length === 0) {
    container.innerHTML =
      '<span style="color: var(--text-tertiary);">No source information available</span>';
    return;
  }

  container.innerHTML = sources
    .map(
      (s) => `
        <div class="source-pill confidence-${s.confidence || "medium"}">
            <span>${escapeHtml(s.source || "Unknown")}</span>
            <span class="source-confidence">${s.confidence || "medium"}</span>
        </div>
    `,
    )
    .join("");
}

function renderPWS(pwsLayer) {
  const card = document.getElementById("pws-card");
  const consensusContainer = document.getElementById("pws-consensus");
  const stationsContainer = document.getElementById("pws-stations");
  const badge = document.getElementById("pws-badge");

  // Hide if no PWS layer
  if (!pwsLayer) {
    card.style.display = "none";
    return;
  }

  // Show card but handle different states
  card.style.display = "block";

  // Not configured state
  if (!pwsLayer.api_token_configured) {
    badge.textContent = "Not Configured";
    badge.style.background = "var(--bg-elevated)";
    badge.style.color = "var(--text-tertiary)";
    consensusContainer.innerHTML = `
            <div style="color: var(--text-secondary); font-size: 0.875rem; line-height: 1.5;">
                Personal Weather Station data is not configured.<br>
                <span style="color: var(--text-tertiary); font-size: 0.75rem;">
                    Set SYNOPTIC_API_TOKEN in your .env file to enable local station observations.
                </span>
            </div>
        `;
    stationsContainer.innerHTML = "";
    return;
  }

  // Fetch failed state
  if (!pwsLayer.fetch_success) {
    badge.textContent = "Error";
    badge.style.background = "rgba(239, 68, 68, 0.2)";
    badge.style.color = "var(--danger)";
    consensusContainer.innerHTML = `
            <div style="color: var(--text-secondary); font-size: 0.875rem;">
                Unable to fetch local station data: ${escapeHtml(pwsLayer.fetch_error || "Unknown error")}
            </div>
        `;
    stationsContainer.innerHTML = "";
    return;
  }

  // No stations found
  if (pwsLayer.stations_with_data === 0) {
    badge.textContent = "No Stations";
    badge.style.background = "var(--bg-elevated)";
    badge.style.color = "var(--text-tertiary)";
    consensusContainer.innerHTML = `
            <div style="color: var(--text-secondary); font-size: 0.875rem;">
                No active weather stations found within ${pwsLayer.search_radius_miles} miles.
            </div>
        `;
    stationsContainer.innerHTML = "";
    return;
  }

  // Success - show stations
  const consensus = pwsLayer.consensus;
  const confidence = consensus ? consensus.confidence : "none";

  // Set badge based on confidence
  badge.textContent =
    confidence === "high"
      ? "High Confidence"
      : confidence === "medium"
        ? "Medium Confidence"
        : confidence === "low"
          ? "Low Confidence"
          : "Observed";
  badge.style.background =
    confidence === "high"
      ? "rgba(34, 197, 94, 0.2)"
      : confidence === "medium"
        ? "rgba(245, 158, 11, 0.2)"
        : confidence === "low"
          ? "rgba(249, 115, 22, 0.2)"
          : "var(--bg-elevated)";
  badge.style.color =
    confidence === "high"
      ? "var(--success)"
      : confidence === "medium"
        ? "var(--warning)"
        : confidence === "low"
          ? "var(--danger)"
          : "var(--text-secondary)";

  // Render consensus summary
  let consensusHtml = "";
  if (consensus) {
    consensusHtml = `
            <div style="background: var(--bg-tertiary); padding: 0.75rem; border-radius: var(--radius-sm); margin-bottom: 0.75rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <span style="font-weight: 500; color: var(--text-primary);">
                        ${pwsLayer.stations_with_data} stations within ${pwsLayer.search_radius_miles} miles
                    </span>
                    <span style="font-size: 0.75rem; text-transform: uppercase; padding: 0.25rem 0.5rem; border-radius: 3px; background: ${confidence === "high" ? "rgba(34, 197, 94, 0.2)" : confidence === "medium" ? "rgba(245, 158, 11, 0.2)" : "var(--bg-elevated)"}; color: ${confidence === "high" ? "var(--success)" : confidence === "medium" ? "var(--warning)" : "var(--text-tertiary)"};">
                        ${confidence} consensus
                    </span>
                </div>
                ${
                  consensus.consensus_temp_f
                    ? `
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 0.5rem; font-size: 0.875rem;">
                        <div><span style="color: var(--text-tertiary);">Consensus Temp:</span> <strong>${Math.round(consensus.consensus_temp_f)}°F</strong></div>
                        ${consensus.consensus_humidity_pct ? `<div><span style="color: var(--text-tertiary);">Humidity:</span> <strong>${Math.round(consensus.consensus_humidity_pct)}%</strong></div>` : ""}
                        ${consensus.consensus_wind_mph ? `<div><span style="color: var(--text-tertiary);">Wind:</span> <strong>${consensus.consensus_wind_mph} mph</strong></div>` : ""}
                    </div>
                `
                    : ""
                }
                ${consensus.confidence_reason ? `<div style="margin-top: 0.5rem; font-size: 0.75rem; color: var(--text-tertiary);">${escapeHtml(consensus.confidence_reason)}</div>` : ""}
                ${pwsLayer.blended_into_current ? `<div style="margin-top: 0.5rem; font-size: 0.75rem; color: var(--accent-teal);">✓ Blended into current conditions (${Math.round(pwsLayer.blend_influence_pct)}% weight)</div>` : ""}
            </div>
        `;
  }
  consensusContainer.innerHTML = consensusHtml;

  // Render individual stations
  const stations = pwsLayer.stations || [];
  stationsContainer.innerHTML = `
        <div style="font-size: 0.75rem; color: var(--text-tertiary); margin-bottom: 0.5rem; text-transform: uppercase; letter-spacing: 0.05em;">Individual Stations</div>
        <div style="display: flex; gap: 0.5rem; overflow-x: auto; padding-bottom: 0.5rem;">
            ${stations
              .slice(0, 8)
              .map(
                (station) => `
                <div style="min-width: 140px; background: var(--bg-tertiary); padding: 0.75rem; border-radius: var(--radius-sm); border-left: 3px solid ${station.freshness === "fresh" ? "var(--success)" : station.freshness === "recent" ? "var(--warning)" : "var(--danger)"};">
                    <div style="font-size: 0.75rem; font-weight: 500; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
                        ${escapeHtml(station.name)}
                    </div>
                    <div style="font-size: 0.6875rem; color: var(--text-tertiary); margin-bottom: 0.5rem;">
                        ${station.distance_miles.toFixed(1)} mi • ${station.observation_age_minutes < 60 ? Math.round(station.observation_age_minutes) + " min ago" : Math.round(station.observation_age_minutes / 60) + "h ago"}
                    </div>
                    ${station.temp_f ? `<div style="font-size: 1.125rem; font-weight: 600; color: var(--text-primary);">${Math.round(station.temp_f)}°F</div>` : '<div style="font-size: 0.875rem; color: var(--text-tertiary);">No temp data</div>'}
                    ${station.humidity_pct ? `<div style="font-size: 0.6875rem; color: var(--text-secondary);">${Math.round(station.humidity_pct)}% humidity</div>` : ""}
                </div>
            `,
              )
              .join("")}
        </div>
    `;
}

function renderMETAR(metarLayer) {
  const card = document.getElementById("metar-card");
  const contentDiv = document.getElementById("metar-content");

  if (!metarLayer || !metarLayer.fetch_success || metarLayer.stations_responded === 0) {
    card.style.display = "none";
    return;
  }

  card.style.display = "block";
  const stations = metarLayer.stations || [];

  // Consensus row
  let consensusHtml = "";
  if (
    metarLayer.consensus_temp_f ||
    metarLayer.consensus_humidity_pct ||
    metarLayer.consensus_wind_mph
  ) {
    const confidence = metarLayer.confidence || "none";
    consensusHtml = `
            <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.75rem;">
                <div style="font-size: 0.6875rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-tertiary);">Official ASOS Consensus</div>
                <span style="font-size: 0.6875rem; padding: 0.2rem 0.5rem; border-radius: 3px; background: ${confidence === "high" ? "rgba(34,197,94,0.2)" : confidence === "medium" ? "rgba(245,158,11,0.2)" : "var(--bg-elevated)"}; color: ${confidence === "high" ? "var(--success)" : confidence === "medium" ? "var(--warning)" : "var(--text-tertiary)"};">${confidence}</span>
            </div>
            <div class="metar-consensus-row">
                ${
                  metarLayer.consensus_temp_f
                    ? `
                    <div class="metar-consensus-item">
                        <div class="metar-consensus-value">${Math.round(metarLayer.consensus_temp_f)}°F</div>
                        <div class="metar-consensus-label">Temperature</div>
                    </div>`
                    : ""
                }
                ${
                  metarLayer.consensus_humidity_pct
                    ? `
                    <div class="metar-consensus-item">
                        <div class="metar-consensus-value">${Math.round(metarLayer.consensus_humidity_pct)}%</div>
                        <div class="metar-consensus-label">Humidity</div>
                    </div>`
                    : ""
                }
                ${
                  metarLayer.consensus_wind_mph
                    ? `
                    <div class="metar-consensus-item">
                        <div class="metar-consensus-value">${Math.round(metarLayer.consensus_wind_mph)} mph</div>
                        <div class="metar-consensus-label">Wind</div>
                    </div>`
                    : ""
                }
                ${
                  metarLayer.consensus_visibility_mi
                    ? `
                    <div class="metar-consensus-item">
                        <div class="metar-consensus-value">${metarLayer.consensus_visibility_mi} mi</div>
                        <div class="metar-consensus-label">Visibility</div>
                    </div>`
                    : ""
                }
            </div>`;
  }

  // Individual station cards
  const stationsHtml = stations
    .map((obs) => {
      const age = obs.observation_age_minutes
        ? obs.observation_age_minutes < 60
          ? `${Math.round(obs.observation_age_minutes)}m ago`
          : `${Math.round(obs.observation_age_minutes / 60)}h ago`
        : "";
      const freshnessClass = obs.freshness || "stale";
      const details = [];
      if (obs.humidity_pct) {
        details.push(`${Math.round(obs.humidity_pct)}% RH`);
      }
      if (obs.wind_speed_mph) {
        details.push(`wind ${Math.round(obs.wind_speed_mph)} mph ${obs.wind_direction || ""}`);
      }
      if (obs.visibility_mi) {
        details.push(`vis ${obs.visibility_mi} mi`);
      }
      if (obs.ceiling_ft) {
        details.push(`ceiling ${Math.round(obs.ceiling_ft)} ft`);
      }
      return `
            <div class="metar-station-card ${freshnessClass}">
                <div class="metar-station-name">${escapeHtml(obs.station_id)}</div>
                <div class="metar-station-desc">${escapeHtml(obs.text_description || "N/A")} ${age ? "• " + age : ""}</div>
                ${obs.temp_f ? `<div class="metar-station-temp">${Math.round(obs.temp_f)}°F</div>` : ""}
                ${details.length ? `<div class="metar-station-details">${details.join(" • ")}</div>` : ""}
            </div>
        `;
    })
    .join("");

  contentDiv.innerHTML =
    consensusHtml +
    `
        <div class="metar-stations-row">${stationsHtml}</div>
        <div style="font-size: 0.6875rem; color: var(--text-tertiary);">
            Official NWS / FAA ASOS observations • Data may lag 5-15 minutes
        </div>
    `;
}

function renderAFD(afdLayer) {
  const card = document.getElementById("afd-card");
  const contentDiv = document.getElementById("afd-content");
  const badge = document.getElementById("afd-badge");

  if (!afdLayer || !afdLayer.fetch_success) {
    card.style.display = "none";
    return;
  }

  card.style.display = "block";
  badge.textContent = afdLayer.wfo_office || "AFD";

  let metaHtml = "";
  if (afdLayer.forecaster || afdLayer.issuance_time) {
    metaHtml = `<div class="afd-meta">`;
    if (afdLayer.forecaster) {
      metaHtml += `<div class="afd-meta-item"><span style="color: var(--text-tertiary);">Forecaster:</span> <strong>${escapeHtml(afdLayer.forecaster)}</strong></div>`;
    }
    if (afdLayer.issuance_time) {
      const d = new Date(afdLayer.issuance_time);
      metaHtml += `<div class="afd-meta-item"><span style="color: var(--text-tertiary);">Issued:</span> ${d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })} UTC</div>`;
    }
    metaHtml += `</div>`;
  }

  let synopsisHtml = "";
  if (afdLayer.synopsis) {
    synopsisHtml = `
            <div class="afd-synopsis">
                <div class="afd-synopsis-label">NWS Synopsis</div>
                <div class="afd-synopsis-text">${escapeHtml(afdLayer.synopsis)}</div>
            </div>`;
  }

  let sectionsHtml = "";
  if (afdLayer.short_term_summary || afdLayer.extended_summary || afdLayer.concerns) {
    sectionsHtml = `
            <div class="afd-sections">
                ${
                  afdLayer.short_term_summary
                    ? `
                    <div class="afd-section">
                        <div class="afd-section-label">Short-Term (Next 12-24h)</div>
                        <div class="afd-section-text">${escapeHtml(afdLayer.short_term_summary)}</div>
                    </div>`
                    : ""
                }
                ${
                  afdLayer.concerns
                    ? `
                    <div class="afd-section">
                        <div class="afd-section-label">Meteorologist Concerns</div>
                        <div class="afd-section-text">${escapeHtml(afdLayer.concerns)}</div>
                    </div>`
                    : ""
                }
                ${
                  afdLayer.extended_summary
                    ? `
                    <div class="afd-section" style="grid-column: 1 / -1;">
                        <div class="afd-section-label">Extended Outlook</div>
                        <div class="afd-section-text">${escapeHtml(afdLayer.extended_summary)}</div>
                    </div>`
                    : ""
                }
            </div>`;
  }

  contentDiv.innerHTML = metaHtml + synopsisHtml + sectionsHtml;
}

function updateFreshness(timestamp) {
  const date = new Date(timestamp);
  const now = new Date();
  const ageMinutes = Math.floor((now - date) / 60000);

  // Update timestamp display
  const timeStr = date.toLocaleString("en-US", {
    weekday: "short",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
  document.getElementById("report-timestamp").textContent = timeStr;
  document.getElementById("freshness-timestamp").textContent = timeStr;

  // Update freshness indicator
  const dot = document.getElementById("freshness-dot");
  const text = document.getElementById("freshness-text");

  if (ageMinutes < 60) {
    dot.className = "freshness-dot";
    text.textContent = `Report generated ${ageMinutes < 1 ? "just now" : ageMinutes + " min ago"}`;
  } else if (ageMinutes < 180) {
    dot.className = "freshness-dot stale";
    const hours = Math.floor(ageMinutes / 60);
    text.textContent = `Report generated ${hours}h ago`;
  } else {
    dot.className = "freshness-dot old";
    const hours = Math.floor(ageMinutes / 60);
    text.textContent = `Report generated ${hours}h ago — may be stale`;
  }
}

async function refreshWeather() {
  const btn = document.querySelector(".btn-primary");
  if (btn) {
    btn.disabled = true;
    btn.innerHTML =
      '<span class="spinner" style="width: 14px; height: 14px;"></span> Refreshing...';
  }

  try {
    // Trigger ingestion
    await fetch(`${API_BASE}/api/ingest/weather`, { method: "POST" });
    // Reload report
    await loadWeatherReport();
  } catch (error) {
    console.error("Error refreshing weather:", error);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = "<span>↻</span><span>Refresh</span>";
    }
  }
}

// Utilities
function getWeatherIcon(condition) {
  const c = (condition || "").toLowerCase();
  if (c.includes("sun") || c.includes("clear") || c.includes("fair")) {
    return "☀️";
  }
  if (c.includes("mostly") || c.includes("mainly")) {
    return "🌤️";
  }
  if (c.includes("partly")) {
    return "⛅";
  }
  if (c.includes("cloud") || c.includes("overcast")) {
    return "☁️";
  }
  if (c.includes("rain") || c.includes("drizzle") || c.includes("shower")) {
    return "🌧️";
  }
  if (c.includes("storm") || c.includes("thunder")) {
    return "⛈️";
  }
  if (c.includes("snow") || c.includes("sleet") || c.includes("ice")) {
    return "❄️";
  }
  if (c.includes("fog") || c.includes("mist")) {
    return "🌫️";
  }
  if (c.includes("wind")) {
    return "💨";
  }
  return "🌤️";
}

function formatHour(hourStr) {
  if (!hourStr) {
    return "--";
  }
  // Handle ISO format or simple time
  if (hourStr.includes("T")) {
    const time = hourStr.split("T")[1].substring(0, 5);
    const [h] = time.split(":");
    const hour = parseInt(h);
    const ampm = hour >= 12 ? "PM" : "AM";
    const displayHour = hour % 12 || 12;
    return `${displayHour} ${ampm}`;
  }
  return hourStr;
}

function formatDayName(dateStr) {
  if (!dateStr) {
    return "--";
  }
  const date = new Date(dateStr);
  const today = new Date();
  const tomorrow = new Date(today);
  tomorrow.setDate(tomorrow.getDate() + 1);

  if (date.toDateString() === today.toDateString()) {
    return "Today";
  }
  if (date.toDateString() === tomorrow.toDateString()) {
    return "Tomorrow";
  }
  return date.toLocaleDateString("en-US", { weekday: "short" });
}

function escapeHtml(text) {
  if (!text) {
    return "";
  }
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

// Expose functions to window for onclick handlers
window.loadWeatherReport = loadWeatherReport;
window.refreshWeather = refreshWeather;
ow.refreshWeather = refreshWeather;
hWeather = refreshWeather;
ow.refreshWeather = refreshWeather;
