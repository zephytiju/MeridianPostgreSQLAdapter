# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Example composition for the approved integer-version projection profile.

This is host/example code over released packages, not a new lifecycle API.
The deployment pins the projector, Schema, target and logical tenant/scope.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from meridian_storage.projection import ProjectionContext, Projector

from meridian_storage import Meridian


def canonical(value: Any) -> str:
    if isinstance(value, Mapping):
        value = {key: json.loads(canonical(item)) for key, item in value.items()}
    elif isinstance(value, (tuple, list)):
        value = [json.loads(canonical(item)) for item in value]
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def integer_version(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("the initial projection profile requires integer source versions")
    return value


def make_projector(meridian: Meridian, *, target: str, scope: Mapping[str, str]) -> Projector:
    structured = meridian.catalog("structured")
    pinned_scope = json.loads(canonical(scope))

    def project(source: Mapping[str, object], context: ProjectionContext):
        version = integer_version(context.source_version)
        source_key = digest(
            [
                context.projection,
                pinned_scope,
                context.source_catalog,
                context.source_resource,
                context.source_identity,
            ]
        )
        deleted = source.get("deleted", False)
        if not isinstance(deleted, bool):
            raise ValueError("deleted must be boolean")
        return structured.put(
            resource=target,
            mode="upsert",
            data={
                "id": digest([source_key, version]),
                "sourceKey": source_key,
                "sourceVersion": version,
                "deleted": deleted or context.mutation_kind == "delete",
                "document": json.loads(canonical(source)),
            },
        )

    return project


def latest_visible(
    rows: Iterable[Mapping[str, Any]],
    *,
    matches: Callable[[Mapping[str, Any]], bool] = lambda row: True,
) -> list[Mapping[str, Any]]:
    """Reduce a complete, bounded, scope-authorized snapshot before filtering.

    Callers must collect every page/version first. Pushing business predicates or
    tombstone filtering into the scan can resurrect stale rows. This example is
    not a streaming/global-search query planner or snapshot guarantee.
    """
    latest: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        key = row["sourceKey"]
        if not isinstance(key, str) or not isinstance(row["deleted"], bool):
            raise ValueError("invalid versioned target row")
        version = integer_version(row["sourceVersion"])
        if key not in latest or version > integer_version(latest[key]["sourceVersion"]):
            latest[key] = row
    return [row for _, row in sorted(latest.items()) if not row["deleted"] and matches(row)]
