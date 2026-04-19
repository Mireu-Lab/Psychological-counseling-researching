#!/usr/bin/env bash
set -euo pipefail

go mod tidy
go build -o app main.go
