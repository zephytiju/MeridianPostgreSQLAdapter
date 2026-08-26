#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

: "${MERIDIAN_POSTGRESQL_TEST_DSN:?set MERIDIAN_POSTGRESQL_TEST_DSN to a disposable PostGIS service}"

uv run ruff check .
uv run mypy
uv run python scripts/verify_contracts.py
uv run pytest -m 'not cluster' --cov --cov-report=xml
uv build
uv run twine check --strict dist/*
