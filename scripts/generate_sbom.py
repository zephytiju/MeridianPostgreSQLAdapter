# SPDX-License-Identifier: Apache-2.0
"""Generate deterministic SPDX 2.3 runtime dependency and artifact inventory."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from importlib.metadata import distribution
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def main():
    folder = Path(sys.argv[1])
    artifacts = sorted([*folder.glob("*.whl"), *folder.glob("*.tar.gz")])
    if len(artifacts) != 2:
        raise ValueError("one wheel and one source distribution are required")
    root = "meridian-storage-postgresql"
    pending = [root]
    packages = {}
    relationships = []
    while pending:
        name = canonicalize_name(pending.pop())
        if name in packages:
            continue
        installed = distribution(name)
        identity = "SPDXRef-" + name
        packages[name] = {
            "SPDXID": identity,
            "name": name,
            "versionInfo": installed.version,
            "downloadLocation": f"https://pypi.org/project/{name}/{installed.version}/",
            "filesAnalyzed": False,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": "Apache-2.0" if name == root else "NOASSERTION",
            "copyrightText": "NOASSERTION",
            "externalRefs": [
                {
                    "referenceCategory": "PACKAGE-MANAGER",
                    "referenceType": "purl",
                    "referenceLocator": f"pkg:pypi/{name}@{installed.version}",
                }
            ],
        }
        for requirement in installed.requires or ():
            selected = Requirement(requirement)
            if selected.marker is not None and not selected.marker.evaluate({"extra": ""}):
                continue
            dependency = canonicalize_name(selected.name)
            pending.append(dependency)
            # psycopg[binary] explicitly selects its binary extra in this package.
            if dependency == "psycopg" and "binary" in selected.extras:
                pending.append("psycopg-binary")
                relationships.append(
                    {
                        "spdxElementId": "SPDXRef-psycopg",
                        "relationshipType": "DEPENDS_ON",
                        "relatedSpdxElement": "SPDXRef-psycopg-binary",
                    }
                )
            relationships.append(
                {
                    "spdxElementId": identity,
                    "relationshipType": "DEPENDS_ON",
                    "relatedSpdxElement": "SPDXRef-" + dependency,
                }
            )
    files = [
        {
            "SPDXID": f"SPDXRef-Artifact-{index}",
            "fileName": "./" + path.name,
            "checksums": [
                {
                    "algorithm": "SHA256",
                    "checksumValue": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            ],
            "licenseConcluded": "NOASSERTION",
            "copyrightText": "NOASSERTION",
        }
        for index, path in enumerate(artifacts)
    ]
    identity = str(uuid5(NAMESPACE_URL, json.dumps([files, packages], sort_keys=True)))
    document = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": f"{root}-{distribution(root).version}",
        "documentNamespace": f"https://github.com/zephytiju/MeridianPostgreSQLAdapter/sbom/{identity}",
        "creationInfo": {
            "creators": ["Tool: meridian-postgresql-generate-sbom"],
            "created": datetime.fromtimestamp(int(os.environ["SOURCE_DATE_EPOCH"]), UTC).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
        "packages": [packages[name] for name in sorted(packages)],
        "files": files,
        "relationships": [
            {
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": "SPDXRef-" + root,
            },
            *sorted(relationships, key=lambda value: json.dumps(value, sort_keys=True)),
            *[
                {
                    "spdxElementId": "SPDXRef-" + root,
                    "relationshipType": "CONTAINS",
                    "relatedSpdxElement": item["SPDXID"],
                }
                for item in files
            ],
        ],
    }
    (folder / f"{root}-{distribution(root).version}.spdx.json").write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n"
    )


if __name__ == "__main__":
    main()
