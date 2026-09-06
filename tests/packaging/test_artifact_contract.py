# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from importlib import resources
from pathlib import Path


def test_compatibility_ledger_is_packaged_and_exact() -> None:
    package = resources.files("meridian_storage.adapters.postgresql")
    compatibility = json.loads(package.joinpath("compatibility.json").read_text())
    assert compatibility["packageVersion"] == "2.0.0"
    assert compatibility["dependencies"] == {
        "meridian-storage-core": "1.0.1",
        "meridian-storage-query": "1.0.2",
        "meridian-storage-semantics": "2.0.0",
    }
    assert compatibility["designRevisions"]["postgresqlPostgisLld"] == 40


def test_repository_has_apache_material_and_one_distribution() -> None:
    root = Path(__file__).resolve().parents[2]
    assert "Apache License" in (root / "LICENSE").read_text()
    assert "Meridian PostgreSQL/PostGIS Adapter" in (root / "NOTICE").read_text()
    pyproject = (root / "pyproject.toml").read_text()
    assert 'name = "meridian-storage-postgresql"' in pyproject
    assert 'license = "Apache-2.0"' in pyproject
    assert pyproject.count("[project]") == 1
