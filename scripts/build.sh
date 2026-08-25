#!/usr/bin/env sh
set -eu
exec make -C "$(dirname "$0")/.." legacy-c
