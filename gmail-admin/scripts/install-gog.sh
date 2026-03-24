#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: ./scripts/install-gog.sh [--yes]

Safe by default: prints what it would do unless --yes is passed.
Install preference order:
  1. Homebrew if available
  2. Build from source with Go if available
  3. Print manual instructions
USAGE
}

DRY_RUN=1
if [[ "${1:-}" == "--yes" ]]; then
  DRY_RUN=0
elif [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

have() { command -v "$1" >/dev/null 2>&1; }
run() {
  if [[ $DRY_RUN -eq 1 ]]; then
    printf '[dry-run] %s\n' "$*"
  else
    eval "$@"
  fi
}

if have gog; then
  echo "gog already installed: $(command -v gog)"
  exit 0
fi

if have brew; then
  echo "Homebrew detected."
  run "brew install gogcli"
  exit 0
fi

if have go && have git && have make; then
  echo "Go toolchain detected; source build path available."
  run "tmpdir=\$(mktemp -d) && git clone https://github.com/steipete/gogcli \"\$tmpdir/gogcli\" && cd \"\$tmpdir/gogcli\" && make && echo Built at \"\$tmpdir/gogcli/bin/gog\""
  exit 0
fi

cat <<'MANUAL'
Could not install automatically.

Manual options:
- Install Homebrew, then: brew install gogcli
- Or install Go + git + make, then build from source:
    git clone https://github.com/steipete/gogcli
    cd gogcli
    make

Once installed, verify with:
    gog --version
MANUAL
