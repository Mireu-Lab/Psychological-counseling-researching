#!/usr/bin/env bash
set -euo pipefail

go run main.go > processing.log 2>&1 &