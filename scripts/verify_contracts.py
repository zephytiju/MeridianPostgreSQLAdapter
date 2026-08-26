# SPDX-License-Identifier: Apache-2.0
"""Verify checked-in fingerprints against the executable released contracts."""

from __future__ import annotations

import json
from pathlib import Path

from meridian_storage.adapters.postgresql.descriptor import (
    DESCRIPTOR,
    QUERY_CAPABILITIES,
    manifest,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    path = root / "contracts" / "adapter-capability" / "fingerprints.v1.json"
    ledger = json.loads(path.read_text(encoding="utf-8"))
    if set(ledger) != {
        "adapterDescriptorFingerprint",
        "formatVersion",
        "manifests",
        "queryCapabilityFingerprint",
    }:
        raise SystemExit("capability fingerprint ledger is not closed")
    if ledger["formatVersion"] != "meridian.postgresql.capability-fingerprints.v1":
        raise SystemExit("capability fingerprint ledger version differs")
    if ledger["adapterDescriptorFingerprint"] != DESCRIPTOR.fingerprint:
        raise SystemExit("Adapter descriptor fingerprint differs")
    if ledger["queryCapabilityFingerprint"] != QUERY_CAPABILITIES.fingerprint:
        raise SystemExit("Query capability fingerprint differs")
    observed = [
        {
            "engineProfile": profile,
            "engineVersion": version,
            "fingerprint": manifest(profile, version).fingerprint,
        }
        for profile, versions in DESCRIPTOR.supported_engine_versions.items()
        for version in versions
    ]
    if ledger["manifests"] != observed:
        raise SystemExit("Engine capability manifest fingerprints differ")
    print(f"verified {len(observed)} PostgreSQL/PostGIS capability manifests")


if __name__ == "__main__":
    main()
