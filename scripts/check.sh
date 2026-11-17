#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
if [[ -z "${CARGO:-}" ]]; then
  CARGO="$(command -v cargo || true)"
  if [[ -z "$CARGO" && -x "$HOME/.cargo/bin/cargo" ]]; then CARGO="$HOME/.cargo/bin/cargo"; fi
fi
: "${CARGO:?Install Rust or set CARGO to its executable path}"
export CARGO
"$CARGO" fmt --all --check
"$CARGO" clippy --workspace --all-targets --all-features --locked -- -D warnings
exec "$root/scripts/test.sh" "$@"
