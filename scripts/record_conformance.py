# SPDX-License-Identifier: Apache-2.0
"""Record observed engine/installed-package provenance; reject skipped acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from importlib.metadata import distributions
from pathlib import Path
from xml.etree import ElementTree

from psycopg import connect
from psycopg.rows import dict_row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", action="append", required=True)
    parser.add_argument("--image", default=os.environ.get("MERIDIAN_POSTGRESQL_IMAGE"))
    parser.add_argument("--output", default="postgresql-provenance.json")
    args = parser.parse_args()
    if not args.image:
        parser.error("--image or MERIDIAN_POSTGRESQL_IMAGE is required")
    results = []
    for name in args.junit:
        suites = ElementTree.parse(name).getroot().iter("testsuite")
        counts = {key: 0 for key in ("tests", "failures", "errors", "skipped")}
        for suite in suites:
            for key in counts:
                counts[key] += int(suite.get(key, "0"))
        if counts["tests"] == 0 or any(counts[key] for key in ("failures", "errors", "skipped")):
            raise SystemExit(f"required acceptance did not pass without skips: {name}: {counts}")
        results.append({"path": name, **counts})
    images = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image]))
    if not images[0]["RepoDigests"]:
        raise SystemExit("selected test image lacks immutable registry digest evidence")
    with connect(os.environ["MERIDIAN_POSTGRESQL_TEST_DSN"], row_factory=dict_row) as connection:
        observed = connection.execute(
            "SELECT current_setting('server_version') AS postgresql, "
            "PostGIS_Lib_Version() AS postgis, pg_is_in_recovery() AS recovery, "
            "(SELECT count(*) FROM pg_stat_replication WHERE state='streaming') AS standbys"
        ).fetchone()
    packages = {
        distribution.metadata["Name"]: distribution.version
        for distribution in distributions()
        if distribution.metadata["Name"].startswith(("meridian-", "psycopg"))
    }
    evidence = {
        "formatVersion": "meridian.postgresql.conformance.v1",
        "sourceCommit": os.environ.get("GITHUB_SHA") or subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "selectedEngineVersion": os.environ.get(
            "MERIDIAN_POSTGRESQL_ENGINE_VERSION", "16-postgis-3.4"
        ),
        "selectedEngineProfile": os.environ.get(
            "MERIDIAN_POSTGRESQL_ENGINE_PROFILE", "postgresql-postgis-local-single-primary"
        ),
        "selectedImage": args.image,
        "observedImageDigests": images[0]["RepoDigests"],
        "observedEngine": observed,
        "installedPackages": dict(sorted(packages.items())),
        "candidateArtifacts": {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(Path("dist").glob("*"))
            if p.is_file() and (p.name.endswith(".whl") or p.name.endswith(".tar.gz"))
        },
        "results": results,
    }
    Path(args.output).write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps(evidence, indent=2))


if __name__ == "__main__":
    main()
