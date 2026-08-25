#!/usr/bin/env sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec "$root/build/counter-server" "${1:-9090}" "${2:-18446744073709551615}"
