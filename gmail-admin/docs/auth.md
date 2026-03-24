# Gmail auth setup without gcloud

This toolkit uses `gog` for Gmail access and does **not** require the `gcloud` CLI for the initial admin workflow.

## What you need

- a Google account you want to manage
- access to Google Cloud Console in a browser
- a **Desktop app** OAuth client JSON
- `gog` installed locally

## Step 1: create or pick a Google Cloud project

Use the Google Cloud Console web UI.

Recommended minimal API set for this project:

- Gmail API

Optional later APIs:

- People API
- Calendar API
- Drive API

## Step 2: configure OAuth consent

In Google Cloud Console:

1. open **APIs & Services → OAuth consent screen**
2. create or configure the app
3. if the app is in testing mode, add the Gmail account as a test user

## Step 3: create a Desktop app OAuth client

In Google Cloud Console:

1. open **APIs & Services → Credentials**
2. create credentials
3. choose **OAuth client ID**
4. choose **Desktop app**
5. download the JSON file

## Step 4: store the client credentials in gog

```bash
./bin/gmail-admin auth credentials /path/to/client_secret.json
```

Equivalent underlying command:

```bash
gog auth credentials /path/to/client_secret.json
```

## Step 5: add the Gmail account

Full-access mailbox admin flow:

```bash
./bin/gmail-admin auth add you@gmail.com
```

Read-only first pass:

```bash
./bin/gmail-admin auth add you@gmail.com --readonly
```

Headless/manual flow:

```bash
./bin/gmail-admin auth add you@gmail.com --manual
```

## Scope notes

For mailbox administration, the practical default is Gmail modify/settings access through `gog`.

Safer bootstrap option:

- start with `--readonly`
- verify search/list flows
- re-run full auth later if mutation commands are needed

## Recommended order

1. `auth credentials`
2. `auth add --readonly`
3. `doctor`
4. `list` / `search`
5. re-run `auth add` without `--readonly` when ready for mailbox mutations

## Multiple accounts

Set a default account in `.env`:

```bash
GMAIL_ADMIN_ACCOUNT=you@gmail.com
```

Or export it directly:

```bash
export GOG_ACCOUNT=you@gmail.com
```

You can also pass account-specific auth commands manually through `gog` if needed.
