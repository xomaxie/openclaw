/**
 * Compass Dashboard - Main Application
 */

const API_BASE = window.location.origin;
const STORAGE_KEY = "compass_actions";

let dashboardData = null;
let actionItems = [];
let meteorologyData = null;

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}

function init() {
  loadActionItems();
  void loadAll();
  setupEventListeners();
  setInterval(loadAll, 60000);
}

function setupEventListeners() {
  const addInput = document.getElementById("add-action-input");
  if (addInput) {
    addInput.addEventListener("keypress", (e) => {
      if (e.key === "Enter") {
        addActionItem(e.target.value);
        e.target.value = "";
      }
    });
  }
}

async function fetchDashboard() {
  try {
    const response = await fetch(`${API_BASE}/api/dashboard`);
    if (!response.ok) {
      throw new Error("Failed to fetch dashboard");
    }
    return await response.json();
  } catch {
    return null;
  }
}

async function fetchLogs() {
  try {
    const response = await fetch(`${API_BASE}/api/logs?limit=5`);
    if (!response.ok) {
      throw new Error("Failed to fetch logs");
    }
    return await response.json();
  } catch {
    return null;
  }
}

async function fetchLatestMeteorology() {
  try {
    const response = await fetch(`${API_BASE}/api/meteorology/latest`);
    if (!response.ok) {
      throw new Error("Failed to fetch meteorology");
    }
    return await response.json();
  } catch {
    return null;
  }
}

async function loadAll() {
  const [dashboard, logs, meteorology] = await Promise.all([
    fetchDashboard(),
    fetchLogs(),
    fetchLatestMeteorology(),
  ]);

  dashboardData = dashboard;
  meteorologyData = meteorology?.report?.content;

  const locationEl = document.getElementById("location");
  if (locationEl && meteorologyData?.location) {
    locationEl.textContent = meteorologyData.location.replace(",", ", ");
  }

  renderWeatherSignal(dashboard, meteorology);
  renderWeatherDetail(dashboard, meteorology);
  renderSite(dashboard);
  renderDaily(dashboard);
  renderCounts(dashboard);
  renderLogs(logs);
  updateLastUpdated();
}

async function refreshAll() {
  const btn = document.getElementById("refresh-btn");
  const originalText = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner" style="width: 14px; height: 14px;"></span> Refreshing...';

  try {
    await fetch(`${API_BASE}/api/ingest/weather`, { method: "POST" });
    await loadAll();
  } catch (error) {
    console.error("Error refreshing:", error);
  } finally {
    btn.disabled = false;
    btn.innerHTML = originalText;
  }
}

function loadActionItems() {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      actionItems = JSON.parse(stored);
    }
  } catch {
    actionItems = [];
  }
  renderChecklist();
}

function saveActionItems() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(actionItems));
}

function addActionItem(text) {
  if (!text || !text.trim()) {
    return;
  }
  actionItems.unshift({
    id: "local_" + Date.now(),
    text: text.trim(),
    completed: false,
    priority: "medium",
    source: "manual",
    createdAt: new Date().toISOString(),
  });
  saveActionItems();
  renderChecklist();
}

function toggleActionItem(id) {
  const item = actionItems.find((i) => i.id === id);
  if (item) {
    item.completed = !item.completed;
    saveActionItems();
    renderChecklist();
  }
}

function getPriorityClass(priority) {
  const map = { high: "priority-high", medium: "priority-medium", low: "priority-low" };
  return map[priority] || "";
}

function getSourceLabel(source) {
  const map = { email: "📧 Email", github: "🐙 GitHub", manual: "✏️ Manual" };
  return map[source] || "📝 Task";
}

function renderChecklist() {
  const container = document.getElementById("checklist-container");
  if (!container) {
    return;
  }

  if (dashboardData?.current_daily?.action_items) {
    dashboardData.current_daily.action_items.forEach((apiItem) => {
      if (!actionItems.some((local) => local.text === apiItem.text)) {
        actionItems.push({
          id: "api_" + Date.now(),
          text: apiItem.text,
          completed: false,
          priority: apiItem.priority || "medium",
          source: apiItem.source || "api",
          createdAt: new Date().toISOString(),
        });
      }
    });
    saveActionItems();
  }

  const sorted = [...actionItems].toSorted((a, b) => {
    if (a.completed !== b.completed) {
      return a.completed ? 1 : -1;
    }
    const priorityOrder = { high: 0, medium: 1, low: 2 };
    return (priorityOrder[a.priority] || 1) - (priorityOrder[b.priority] || 1);
  });

  const incompleteCount = sorted.filter((i) => !i.completed).length;
  const countEl = document.getElementById("action-count");
  if (countEl) {
    countEl.textContent = incompleteCount > 0 ? `${incompleteCount} pending` : "All caught up";
    countEl.style.color = incompleteCount > 0 ? "var(--accent-gold)" : "var(--success)";
  }

  if (sorted.length === 0) {
    container.innerHTML = `<div class="empty-state" style="padding: 1.5rem;"><div style="font-size: 2rem;">✅</div><p style="font-size: 0.875rem; color: var(--text-tertiary);">No action items</p></div>`;
    return;
  }

  container.innerHTML = sorted
    .map(
      (item) => `
        <div class="checklist-item ${item.completed ? "completed" : ""} ${getPriorityClass(item.priority)}" onclick="window.compassToggleItem('${item.id}')">
            <div class="checkbox"><span class="checkbox-check">✓</span></div>
            <div class="checklist-content">
                <div class="checklist-text">${escapeHtml(item.text)}</div>
                <div class="checklist-meta">${getSourceLabel(item.source)}</div>
            </div>
        </div>
    `,
    )
    .join("");
}

window.compassToggleItem = toggleActionItem;
window.refreshAll = refreshAll;

// Navigation to standalone weather report page
function showMeteorologyDetail() {
  window.location.href = "/weather";
}

function renderWeatherSignal(data, meteorology) {
  const container = document.getElementById("weather-signal");
  const badge = document.getElementById("weather-alert-badge");
  if (!container) {
    return;
  }

  if (!data || !data.current_weather) {
    container.innerHTML = '<div style="color: var(--text-tertiary);">No weather data</div>';
    return;
  }

  const content = meteorology?.report?.content || data.current_weather;
  const current = content.current || data.current_weather.current;
  const alerts = content.alerts || [];
  const narrative_summary = content.narrative_summary || content.summary || "";
  const pwsLayer = content.pws_layer;

  // Check for PWS indicator
  let pwsIndicator = "";
  if (pwsLayer && pwsLayer.fetch_success && pwsLayer.stations_with_data > 0) {
    const confidence = pwsLayer.consensus?.confidence || "low";
    const confidenceEmoji = confidence === "high" ? "●" : confidence === "medium" ? "◐" : "○";
    const confidenceColor =
      confidence === "high"
        ? "var(--success)"
        : confidence === "medium"
          ? "var(--warning)"
          : "var(--text-tertiary)";
    pwsIndicator = `<div style="font-size: 0.625rem; color: ${confidenceColor}; margin-top: 2px; display: flex; align-items: center; gap: 3px;" title="Local PWS stations: ${confidence} confidence">${confidenceEmoji} Local stations</div>`;
  }

  if (badge) {
    if (alerts.length > 0) {
      badge.style.display = "block";
      badge.textContent = "!";
      badge.style.color = "var(--danger)";
    } else {
      badge.style.display = "none";
    }
  }

  container.innerHTML = `
        <div class="weather-signal-icon">${getWeatherIcon(current.condition)}</div>
        <div class="weather-signal-info">
            <div class="weather-signal-temp">${Math.round(current.temp_f)}°</div>
            <div class="weather-signal-condition">${current.condition}</div>
            ${narrative_summary ? `<div class="weather-signal-summary">${narrative_summary.substring(0, 80)}${narrative_summary.length > 80 ? "..." : ""}</div>` : ""}
            ${pwsIndicator}
        </div>
    `;
}

function renderWeatherDetail(data, meteorology) {
  const container = document.getElementById("weather-detail");
  if (!container) {
    return;
  }

  if (!data || !data.current_weather) {
    container.innerHTML = `<div class="empty-state"><div style="font-size: 3rem;">🌤️</div><p>No data</p></div>`;
    return;
  }

  const content = meteorology?.report?.content || data.current_weather;
  const current = content.current || data.current_weather.current;
  const forecast = content.forecast || data.current_weather.forecast || [];
  const alerts = content.alerts || [];
  const narrative_summary = content.narrative_summary || content.summary || "";

  let html = `
        <div class="weather-main">
            <div class="weather-main-icon">${getWeatherIcon(current.condition)}</div>
            <div class="weather-main-info">
                <div class="weather-main-temp">${Math.round(current.temp_f)}°F</div>
                <div class="weather-main-condition">${current.condition}</div>
            </div>
        </div>
        <div class="weather-details">
            <div class="weather-detail"><div class="weather-detail-label">Humidity</div><div class="weather-detail-value">${current.humidity_pct}%</div></div>
            <div class="weather-detail"><div class="weather-detail-label">Wind</div><div class="weather-detail-value">${current.wind_mph} mph</div></div>
            <div class="weather-detail"><div class="weather-detail-label">UV</div><div class="weather-detail-value">${current.uv_index}</div></div>
        </div>
    `;

  if (forecast.length > 0) {
    html += `<div class="forecast"><div class="forecast-title">3-Day Forecast</div><div class="forecast-days">`;
    forecast.slice(0, 3).forEach((day) => {
      html += `
                <div class="forecast-day">
                    <div class="forecast-day-name">${formatDayName(day.day)}</div>
                    <div class="forecast-day-temps">${Math.round(day.high_f)}° / ${Math.round(day.low_f)}°</div>
                    <div class="forecast-day-condition">${day.condition}</div>
                </div>
            `;
    });
    html += "</div></div>";
  }

  if (narrative_summary) {
    html += `<div class="summary-box">${narrative_summary}</div>`;
  }

  if (alerts.length > 0) {
    html += `<div style="margin-top: 1rem; padding: 0.75rem; background: rgba(239, 68, 68, 0.1); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 6px; color: var(--danger); font-size: 0.875rem;">⚠️ ${alerts.length} weather alert${alerts.length > 1 ? "s" : ""} active</div>`;
  }

  container.innerHTML = html;
}

function renderSite(data) {
  const container = document.getElementById("site-content");
  if (!container) {
    return;
  }

  if (!data || !data.current_site) {
    container.innerHTML = `<div class="empty-state"><div style="font-size: 3rem;">📊</div><p>No data</p></div>`;
    return;
  }

  const site = data.current_site;
  const analytics = site.analytics || {};

  let html = `
        <div class="metric-list">
            <div class="metric"><span class="metric-label">Visitors (24h)</span><span class="metric-value highlight">${analytics.visitors || 0}</span></div>
            <div class="metric"><span class="metric-label">Pageviews (24h)</span><span class="metric-value">${analytics.pageviews || 0}</span></div>
            <div class="metric"><span class="metric-label">Calls</span><span class="metric-value">${site.calls?.total || 0}</span></div>
        </div>
    `;

  if (site.summary) {
    html += `<div class="summary-box">${site.summary}</div>`;
  }

  container.innerHTML = html;
}

function renderDaily(data) {
  const container = document.getElementById("daily-detail");
  const briefContainer = document.getElementById("quick-brief");

  if (!data || !data.current_daily) {
    if (container) {
      container.innerHTML = `<div class="empty-state"><div style="font-size: 3rem;">📅</div><p>No daily report</p></div>`;
    }
    if (briefContainer) {
      const date = new Date().toLocaleDateString("en-US", {
        weekday: "long",
        month: "short",
        day: "numeric",
      });
      briefContainer.innerHTML = `<div class="brief-date">${date}</div><div class="brief-text">No daily summary available.</div>`;
    }
    return;
  }

  const daily = data.current_daily;

  if (briefContainer) {
    const date = new Date().toLocaleDateString("en-US", {
      weekday: "long",
      month: "short",
      day: "numeric",
    });
    briefContainer.innerHTML = `<div class="brief-date">${date}</div><div class="brief-text">${daily.summary || "No daily summary available."}</div>`;
  }

  if (container) {
    let html = `<div class="metric-list">`;
    if (daily.emails?.length > 0) {
      html += `<div class="metric"><span class="metric-label">Unread Emails</span><span class="metric-value ${daily.emails.length > 5 ? "warning" : ""}">${daily.emails.length}</span></div>`;
    }
    if (daily.issues?.length > 0) {
      html += `<div class="metric"><span class="metric-label">GitHub Issues</span><span class="metric-value ${daily.issues.length > 3 ? "warning" : ""}">${daily.issues.length}</span></div>`;
    }
    html += `</div>`;

    if (daily.summary) {
      html += `<div class="summary-box">${daily.summary}</div>`;
    }

    container.innerHTML = html;
  }
}

function renderCounts(data) {
  const container = document.getElementById("counts-grid");
  if (!container) {
    return;
  }

  let emails = 0,
    issues = 0;

  if (data?.current_daily) {
    emails = data.current_daily.emails?.length || 0;
    issues = data.current_daily.issues?.length || 0;
  }

  const localPending = actionItems.filter((i) => !i.completed).length;

  container.innerHTML = `
        <div class="count-card"><div class="count-value ${emails === 0 ? "zero" : emails > 5 ? "warning" : ""}">${emails}</div><div class="count-label">Emails</div></div>
        <div class="count-card"><div class="count-value ${issues === 0 ? "zero" : issues > 3 ? "warning" : ""}">${issues}</div><div class="count-label">Issues</div></div>
        <div class="count-card"><div class="count-value ${localPending === 0 ? "zero" : "warning"}">${localPending}</div><div class="count-label">Actions</div></div>
    `;
}

function renderLogs(data) {
  const container = document.getElementById("logs-content");
  if (!container) {
    return;
  }

  if (!data || !data.runs || data.runs.length === 0) {
    container.innerHTML = `<div class="empty-state"><div style="font-size: 2rem;">📋</div><p>No recent activity</p></div>`;
    return;
  }

  container.innerHTML = `
        <div class="logs-list">
            ${data.runs
              .map((run) => {
                const statusClass =
                  run.status === "success"
                    ? "success"
                    : run.status === "error"
                      ? "error"
                      : "running";
                return `<div class="log-entry"><span class="log-time">${formatTime(run.started_at)}</span><span class="log-pipeline">${run.pipeline}</span><span class="log-status ${statusClass}">${run.status}</span></div>`;
              })
              .join("")}
        </div>
    `;
}

function updateLastUpdated() {
  const el = document.getElementById("last-updated");
  if (el) {
    el.textContent = new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
}

function formatTime(dateStr) {
  if (!dateStr) {
    return "--:--";
  }
  return new Date(dateStr).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

function getWeatherIcon(condition) {
  const c = (condition || "").toLowerCase();
  if (c.includes("sun") || c.includes("clear")) {
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
  if (c.includes("rain") || c.includes("drizzle")) {
    return "🌧️";
  }
  if (c.includes("storm") || c.includes("thunder")) {
    return "⛈️";
  }
  if (c.includes("snow") || c.includes("sleet")) {
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

function escapeHtml(text) {
  if (!text) {
    return "";
  }
  const div = document.createElement("div");
  div.textContent = text;
  return div.innerHTML;
}

window.showMeteorologyDetail = showMeteorologyDetail;
