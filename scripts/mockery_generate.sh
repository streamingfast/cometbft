#!/bin/sh
#
# Invoke Mockery v2 to update generated mocks for the given type.
#
# Mockery is built from the pinned module in scripts/mockery, which raises
# golang.org/x/tools to a version that understands the current Go toolchain.

set -e

tooldir="$(cd "$(dirname "$0")/mockery" && pwd)"
GOWORK=off go build -C "$tooldir" -o "$tooldir/bin/mockery" github.com/vektra/mockery/v2

"$tooldir/bin/mockery" --disable-version-string --case underscore --name "$*"
