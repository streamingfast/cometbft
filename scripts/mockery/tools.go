//go:build tools

// Package tools pins the mockery build used by scripts/mockery_generate.sh.
// golang.org/x/tools is pinned above mockery's own requirement so that mockery
// can read the export data written by the Go toolchain in go.mod.
package tools

import _ "github.com/vektra/mockery/v2"
