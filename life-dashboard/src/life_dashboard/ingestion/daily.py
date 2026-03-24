"""Daily report ingestion pipeline — emails + GitHub issues → DailyReport."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import structlog

from life_dashboard.reports import DailyReport, EmailSummary, IssueSummary, ActionItem

log = structlog.get_logger(__name__)

# ── Email Fetching ─────────────────────────────────────────────────────────────

def _imap_fetch_emails(
    host: str,
    port: int,
    username: str,
    password: str,
    since_dt: datetime,
) -> list[EmailSummary]:
    """
    Fetch recent emails from an IMAP account.
    Returns a list of EmailSummary objects.
    """
    import imaplib
    import email
    from email.header import decode_header

    emails: list[EmailSummary] = []
    try:
        log.info("email.connect", host=host, port=port, user=username)
        mail = imaplib.IMAP4_SSL(host, port)
        mail.login(username, password)
        mail.select("INBOX")

        # Search for emails since the given datetime
        since_str = since_dt.strftime("%d-%b-%Y")
        status, message_ids = mail.search(None, f"SINCE {since_str}")

        if status != "OK":
            log.warn("email.search_failed", status=status, user=username)
            return []

        for msg_id in message_ids[0].split():
            try:
                status, raw_data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue
                msg = email.message_from_bytes(raw_data[0][1])

                # Decode subject
                subject_parts: list[str] = []
                for part, enc in decode_header(msg["Subject"] or "No Subject"):
                    if isinstance(part, bytes):
                        subject_parts.append(part.decode(enc or "utf-8", errors="replace"))
                    else:
                        subject_parts.append(str(part))
                subject = "".join(subject_parts)

                # Parse date
                date_str = msg["Date"]
                try:
                    msg_date = email.utils.parsedate_to_datetime(date_str)
                except Exception:
                    msg_date = datetime.now(timezone.utc)

                # Sender
                from_addr = email.utils.parseaddr(msg["From"])[1]

                # Body snippet (first 200 chars of plain text)
                snippet = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            payload = part.get_payload(decode=True)
                            if payload:
                                snippet = payload.decode("utf-8", errors="replace")[:200].strip()
                                break
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        snippet = payload.decode("utf-8", errors="replace")[:200].strip()

                # Determine importance based on sender/subject keywords
                importance = "normal"
                lower_subject = subject.lower()
                if any(kw in lower_subject for kw in ["urgent", "asap", "important", "deadline"]):
                    importance = "high"
                elif any(kw in lower_subject for kw in ["newsletter", "no-reply", "unsubscribe"]):
                    importance = "low"

                emails.append(EmailSummary(
                    from_addr=from_addr,
                    subject=subject,
                    snippet=snippet,
                    date=msg_date,
                    importance=importance,
                    message_id=msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                ))
            except Exception as exc:
                log.warn("email.parse_error", msg_id=msg_id, error=str(exc))
                continue

        mail.logout()
        log.info("email.fetched", user=username, count=len(emails))
    except Exception as exc:
        log.error("email.connection_error", host=host, user=username, error=str(exc))

    return emails


async def fetch_all_emails(config: dict, since_dt: datetime) -> list[EmailSummary]:
    """Fetch emails from all configured IMAP accounts."""
    all_emails: list[EmailSummary] = []
    accounts = config.get("email", {}).get("accounts", [])

    for account in accounts:
        name = account.get("name", "")
        # Resolve IMAP password from environment
        # Expected env vars: IMAP_PASSWORD_MAX, IMAP_PASSWORD_GMAIL1, IMAP_PASSWORD_GMAIL2
        if "maxscomputers.com" in name:
            password_env = "IMAP_PASSWORD_MAX"
        elif "xxoomaxie" in name:
            password_env = "IMAP_PASSWORD_GMAIL1"
        elif "micahdanthony" in name:
            password_env = "IMAP_PASSWORD_GMAIL2"
        else:
            password_env = f"IMAP_PASSWORD_{name.split('@')[0].upper()}"

        import os
        password = os.environ.get(password_env, "")
        if not password:
            log.warn("email.no_password", account=name, expected_env=password_env)
            continue

        emails = _imap_fetch_emails(
            host=account.get("imap_host", ""),
            port=account.get("imap_port", 993),
            username=name,
            password=password,
            since_dt=since_dt,
        )
        all_emails.extend(emails)

    # Sort by date descending
    all_emails.sort(key=lambda e: e.date, reverse=True)
    return all_emails


# ── GitHub Issues ─────────────────────────────────────────────────────────────

async def fetch_github_issues(config: dict, since_dt: datetime) -> list[IssueSummary]:
    """Fetch assigned GitHub issues from configured orgs."""
    import os
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        log.warn("github.no_token")
        return []

    orgs = config.get("github", {}).get("default_orgs", ["AltoredHealth"])
    all_issues: list[IssueSummary] = []

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        for org in orgs:
            try:
                # Fetch assigned issues for the org
                # GET /orgs/{org}/issues?filter=assigned&state=open&per_page=50
                url = f"https://api.github.com/orgs/{org}/issues"
                params = {
                    "filter": "assigned",
                    "state": "open",
                    "per_page": 50,
                    "sort": "updated",
                    "direction": "desc",
                }
                resp = await client.get(url, headers=headers, params=params)
                if resp.status_code == 404:
                    # Try as a user instead of org
                    url = f"https://api.github.com/users/{org}/issues"
                    resp = await client.get(url, headers=headers, params=params)

                if resp.status_code != 200:
                    log.warn("github.api_error", org=org, status=resp.status_code)
                    continue

                items = resp.json()
                for item in items:
                    # Skip pull requests (GitHub issues API includes PRs)
                    if item.get("pull_request"):
                        continue

                    # Parse due date from labels (look for "due:YYYY-MM-DD" pattern)
                    due_date = None
                    labels: list[str] = []
                    for label in item.get("labels", []):
                        label_name = label.get("name", "")
                        labels.append(label_name)
                        if label_name.startswith("due:"):
                            try:
                                due_date = datetime.strptime(
                                    label_name[4:], "%Y-%m-%d"
                                ).date()
                            except ValueError:
                                pass

                    # Only include if updated since our cutoff
                    updated_at = datetime.fromisoformat(
                        item["updated_at"].replace("Z", "+00:00")
                    )
                    if updated_at < since_dt:
                        continue

                    # Extract repo name from repository_url
                    repo_url = item.get("repository_url", "")
                    repo = repo_url.split("/")[-1] if repo_url else "unknown"

                    all_issues.append(IssueSummary(
                        repo=repo,
                        number=item.get("number", 0),
                        title=item.get("title", ""),
                        body=item.get("body") or None,
                        labels=labels,
                        due=due_date,
                        url=item.get("html_url"),
                    ))

                log.info("github.issues_fetched", org=org, count=len(items))

            except Exception as exc:
                log.error("github.fetch_error", org=org, error=str(exc))

    # Sort by due date (soonest first), then by updated_at
    def sort_key(issue: IssueSummary) -> tuple:
        if issue.due:
            return (0, issue.due)
        return (1, datetime.max)
    all_issues.sort(key=sort_key)

    return all_issues


# ── LLM Synthesis ─────────────────────────────────────────────────────────────

async def synthesize_daily_report(
    emails: list[EmailSummary],
    issues: list[IssueSummary],
    llm_api_key: str,
    model: str = "openai/gpt-4o-mini",
) -> DailyReport:
    """
    Use LLM to synthesize emails + issues into a DailyReport.
    Falls back to rule-based extraction if LLM call fails.
    """
    # Build a compact prompt input
    email_data = [
        {
            "from": e.from_addr,
            "subject": e.subject,
            "snippet": e.snippet[:150],
            "importance": e.importance,
            "date": e.date.isoformat(),
        }
        for e in emails[:20]  # Cap at 20 most recent
    ]

    issue_data = [
        {
            "repo": i.repo,
            "number": i.number,
            "title": i.title,
            "labels": i.labels,
            "due": str(i.due) if i.due else None,
            "url": i.url,
        }
        for i in issues[:20]
    ]

    prompt = f"""You are Max's daily report assistant. Given the following emails and GitHub issues from today, create a structured DailyReport.

## Emails
{json.dumps(email_data, indent=2)}

## GitHub Issues (assigned to Max)
{json.dumps(issue_data, indent=2)}

## Task
Create a DailyReport JSON object with:
- "type": "daily"
- "date": today's date in YYYY-MM-DD format
- "emails": array of email summaries (you can filter/reduce as needed)
- "issues": array of issue summaries
- "action_items": array of ActionItem objects with "text" and "source" (email/github)
- "summary": a brief 2-3 sentence summary of the day

Only include emails that require action or are from important contacts. Ignore newsletters and no-reply emails.

Respond ONLY with valid JSON matching this schema (no markdown, no explanation):
{{
  "type": "daily",
  "date": "2026-03-23",
  "emails": [{{ "from_addr": "...", "subject": "...", "snippet": "...", "date": "...", "importance": "normal" }}],
  "issues": [{{ "repo": "...", "number": 1, "title": "...", "labels": [], "due": null, "url": null }}],
  "action_items": [{{ "text": "...", "source": "email" }}],
  "summary": "..."
}}"""

    # Try LLM synthesis
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {llm_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 2048,
                    "temperature": 0.3,
                },
            )
            if resp.status_code == 200:
                result = resp.json()
                content = result["choices"][0]["message"]["content"].strip()
                # Strip markdown code blocks if present
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                content = content.strip()
                report_data = json.loads(content)
                return DailyReport(**report_data)
            else:
                log.warn("llm.synthesis_failed", status=resp.status_code, body=resp.text[:200])
    except Exception as exc:
        log.warn("llm.synthesis_error", error=str(exc))

    # Fallback: rule-based report without LLM
    log.info("daily.using_rule_based_report")
    action_items: list[ActionItem] = []

    for email in emails[:10]:
        if email.importance == "high":
            action_items.append(ActionItem(
                text=f"Follow up on: {email.subject}",
                source="email",
            ))

    for issue in issues:
        if issue.due:
            days_until = (issue.due - datetime.now().date()).days
            if days_until <= 3:
                action_items.append(ActionItem(
                    text=f"Review issue #{issue.number} in {issue.repo} (due {issue.due})",
                    source="github",
                ))

    return DailyReport(
        type="daily",
        date=datetime.now(timezone.utc).date(),
        emails=emails[:10],
        issues=issues,
        action_items=action_items,
        summary=f"You have {len(emails)} recent emails and {len(issues)} open GitHub issues assigned to you.",
    )


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def run_daily_pipeline(
    db=None,
    since_hours: int = 24,
) -> DailyReport:
    """
    Full daily report ingestion pipeline.
    1. Fetch emails from all IMAP accounts
    2. Fetch GitHub issues from configured orgs
    3. Synthesize with LLM (fallback to rule-based)
    4. Persist to DB (if db provided)
    5. Update dashboard_state (if db provided)
    Returns the report.
    """
    import os
    log.info("daily.pipeline.start")

    cfg = {}  # Will be passed from CLI
    # Load config lazily to avoid circular imports
    from life_dashboard.config import load_config
    cfg = load_config()

    since_dt = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    # Fetch emails and issues concurrently
    emails, issues = await fetch_all_emails(cfg, since_dt), await fetch_github_issues(cfg, since_dt)
    log.info("daily.raw_data", emails=len(emails), issues=len(issues))

    # Synthesize
    llm_api_key = os.environ.get("LLM_API_KEY", "")
    report = await synthesize_daily_report(emails, issues, llm_api_key)

    if db:
        run_id = db.start_run("daily")
        try:
            report_id = db.insert_report(
                rtype="daily",
                title=f"Daily Report — {report.date}",
                content=report.model_dump(),
                generated_at=datetime.now(timezone.utc),
                summary=report.summary,
                source_tag="daily",
                raw_inputs=[f"email:{e.from_addr}" for e in emails[:5]]
                + [f"github:{i.repo}#{i.number}" for i in issues[:5]],
            )
            db.set_state("current_daily", report.model_dump())
            db.set_state("latest_daily_report_id", report_id)
            db.finish_run(run_id, "success", report_id)
            log.info("daily.pipeline.done", report_id=report_id)
        except Exception as exc:
            db.finish_run(run_id, "error", error=str(exc))
            raise
    else:
        log.info("daily.pipeline.done", db=False)

    return report
