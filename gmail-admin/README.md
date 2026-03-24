# gmail-admin

A practical, CLI-first Gmail admin toolkit for Max.

This project wraps [`gog`](https://gogcli.sh/) with safer defaults for mailbox operations:

- `gog` is the preferred Gmail engine
- no `gcloud` dependency for the initial mailbox admin workflow
- dry-run by default for destructive or high-impact mailbox changes
- filter work starts with reviewed plans before applying live changes

## What it covers

- auth/bootstrap guidance for Gmail access
- inbox listing and message search
- label listing and label operations passthrough
- archive / trash / permanent delete flows
- apply/remove labels to matched messages
- filter-plan generation for later review/apply
- setup validation and install helpers

## Project layout

- `bin/gmail-admin` — main wrapper CLI
- `docs/auth.md` — OAuth setup without `gcloud`
- `docs/safety.md` — safety model and operational guardrails
- `scripts/install-gog.sh` — install helper for `gog`
- `scripts/validate.sh` — local sanity checks
- `examples/` — ready-to-copy examples
- `plans/` — generated filter plans
- `.env.example` — local config template

## Prerequisites

- `bash`
- `python3`
- `gog`
- a Google OAuth **Desktop app** client JSON

Optional but useful:

- `jq`
- Homebrew or Go toolchain for installing `gog`

## Quick start

### 1. Install gog

```bash
cd /root/.openclaw/workspace/gmail-admin
./scripts/install-gog.sh
```

If the script cannot install automatically on your system, it prints the exact next step.

### 2. Create local config

```bash
cd /root/.openclaw/workspace/gmail-admin
cp .env.example .env
```

Fill in:

- `GMAIL_ADMIN_ACCOUNT`
- `GMAIL_ADMIN_CREDENTIALS_JSON`

### 3. Store OAuth client credentials

```bash
./bin/gmail-admin auth credentials /path/to/client_secret.json
```

### 4. Add Gmail access for the account

```bash
./bin/gmail-admin auth add you@gmail.com
```

Headless/remote server flow:

```bash
./bin/gmail-admin auth add you@gmail.com --manual
```

Read-only bootstrap first:

```bash
./bin/gmail-admin auth add you@gmail.com --readonly
```

### 5. Validate local setup

```bash
./scripts/validate.sh
./bin/gmail-admin doctor
```

## Example workflows

### List recent inbox threads

```bash
./bin/gmail-admin list 'in:inbox newer_than:7d'
```

### Search individual messages

```bash
./bin/gmail-admin search 'from:billing@example.com newer_than:30d'
```

### Preview an archive operation

```bash
./bin/gmail-admin archive --query 'category:promotions older_than:30d'
```

### Execute the archive for real

```bash
./bin/gmail-admin archive --query 'category:promotions older_than:30d' --yes
```

### Apply a label to matched messages

```bash
./bin/gmail-admin apply-label 'Receipts' --query 'from:stripe.com newer_than:365d' --yes
```

### Move matched messages to trash

```bash
./bin/gmail-admin trash --query 'label:newsletters older_than:90d' --yes
```

### Permanently delete matched messages

```bash
./bin/gmail-admin delete --query 'in:trash older_than:30d' --yes
```

### Review labels

```bash
./bin/gmail-admin labels
./bin/gmail-admin labels get INBOX
./bin/gmail-admin labels create 'Waiting'
```

### Generate a filter plan for later review

```bash
./bin/gmail-admin plan-filters \
  --from 'noreply@example.com' \
  --label 'Notifications' \
  --archive \
  --out ./plans/notifications.json
```

## Safety model

The wrapper is intentionally conservative.

- destructive operations default to **preview only**
- you must pass `--yes` to execute archive/trash/delete/apply-label/remove-label
- filter creation is **planned first**, not auto-applied
- query-based mutations are capped unless you raise `--max`
- `delete` is permanent and should usually be limited to mail already in Trash

More details: `docs/safety.md`

## Auth model

The initial workflow avoids `gcloud` entirely.

- create a Google Cloud project in the web console
- create a **Desktop app** OAuth client
- download the client JSON
- store it with `gog auth credentials`
- authorize each Gmail account with `gog auth add`

More details: `docs/auth.md`

## Commands

```text
gmail-admin doctor
gmail-admin auth ...
gmail-admin list [query]
gmail-admin search <query>
gmail-admin labels [subcommand ...]
gmail-admin archive (--query <q> | <messageId>...)
gmail-admin trash (--query <q> | <messageId>...)
gmail-admin delete (--query <q> | <messageId>...)
gmail-admin apply-label <label> (--query <q> | <messageId>...)
gmail-admin remove-label <label> (--query <q> | <messageId>...)
gmail-admin plan-filters [options]
```

## Validation

```bash
./scripts/validate.sh
```

This checks:

- required files exist
- shell scripts parse cleanly
- `python3` exists
- `gog` availability / version if installed

## Notes

- `gog gmail search` returns threads.
- `gog gmail messages search` returns individual messages.
- `labels` is a passthrough to `gog gmail labels ...`.
- `gmailctl` is intentionally left as a later/optional layer for declarative filter management.
