#!/usr/bin/env sh
# Run this before publishing an experiment result. The sanitizer pass is
# deliberately separate so its lower performance is never mixed with a TPS
# number from the optimized binary.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
sanitizer_flags='-O1 -g -std=c++20 -Wall -Wextra -Werror -pthread -Iinclude -fsanitize=address,undefined -fno-omit-frame-pointer'

make -C "$root/cpp" clean all test
make -C "$root/cpp" clean all test CXXFLAGS="$sanitizer_flags"
make -C "$root/cpp" clean all test
make -C "$root" legacy-clean legacy-c legacy-test
