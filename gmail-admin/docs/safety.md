# Safety model

This toolkit is designed to make Gmail admin work harder to mess up.

## Defaults

- high-impact actions are preview-first
- `--yes` is required to execute archive, trash, delete, apply-label, and remove-label
- query-based operations default to `--max 100`
- filter creation is planned, not auto-applied

## Operation risk levels

### Low risk

- `doctor`
- `auth status`
- `list`
- `search`
- `labels list`
- `labels get`
- `plan-filters`

### Medium risk

- `labels create`
- `labels rename`
- `apply-label`
- `remove-label`
- `archive`

### High risk

- `trash`
- `delete`
- `labels delete`
- live `gog gmail filters create/delete`

## Recommended habits

- start with read-only auth
- use tight Gmail queries before any mutation
- preview every query-based mutation first
- keep `--max` low on first run
- prefer trash before permanent delete
- use permanent delete mainly for already-trashed mail

## Example safe progression

1. search candidate messages
2. archive or label a small sample
3. inspect the result in Gmail
4. scale up the query only after confirming behavior

## Filter workflow

For filters, this project intentionally separates:

- planning
- review
- application

Use `plan-filters` to write a JSON plan, review it, then apply manually with `gog gmail filters create ...` once you trust it.

## Why no automatic filter apply yet?

Because filters can silently redirect, archive, star, forward, or trash mail at scale. Planning first is the right default until the policy layer is more mature.
