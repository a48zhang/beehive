#!/usr/bin/env bash
# Build both process boundaries before running cross-package integration tests.
#!/usr/bin/env bash
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
if [[ -z "${CARGO:-}" ]]; then
  CARGO="$(command -v cargo || true)"
  if [[ -z "$CARGO" && -x "$HOME/.cargo/bin/cargo" ]]; then CARGO="$HOME/.cargo/bin/cargo"; fi
fi
: "${CARGO:?Install Rust or set CARGO to its executable path}"
target="${CARGO_TARGET_DIR:-$root/target}"
if [[ "$target" != /* ]]; then target="$root/$target"; fi
export CARGO_TARGET_DIR="$target"
export PI_RUNTIME_TEST_BIN="$target/debug/pi-runtime"
export PI_RUNTIME_TEST_PEER_BIN="$target/debug/pi-runtime-test-peer"
"$CARGO" build --workspace --all-features --bins --locked
"$CARGO" test --workspace --all-features --locked "$@"
