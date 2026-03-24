# Example mailbox workflow

## Triage newsletters older than 30 days

Preview:

```bash
./bin/gmail-admin search 'label:newsletters older_than:30d'
./bin/gmail-admin archive --query 'label:newsletters older_than:30d'
```

Execute:

```bash
./bin/gmail-admin archive --query 'label:newsletters older_than:30d' --yes
```

## Tag receipts from Stripe

Preview:

```bash
./bin/gmail-admin search 'from:stripe.com newer_than:365d'
./bin/gmail-admin apply-label 'Receipts' --query 'from:stripe.com newer_than:365d'
```

Execute:

```bash
./bin/gmail-admin apply-label 'Receipts' --query 'from:stripe.com newer_than:365d' --yes
```

## Permanently clear old trash

Preview:

```bash
./bin/gmail-admin delete --query 'in:trash older_than:30d'
```

Execute:

```bash
./bin/gmail-admin delete --query 'in:trash older_than:30d' --yes
```
