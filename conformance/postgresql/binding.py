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

from __future__ import annotations

import os

from meridian_storage.runtime.config import BindingConfig, ClientPolicy, SecretReference, TLSPolicy
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from meridian_storage.adapters.postgresql.descriptor import manifest


def make_binding(
    endpoint: str,
    *,
    engine_profile: str | None = None,
    engine_version: str | None = None,
    expected_standbys: int | None = None,
    required_physical_fingerprint: str | None = None,
) -> tuple[BindingConfig, str, str]:
    engine_profile = engine_profile or os.environ.get(
        "MERIDIAN_POSTGRESQL_ENGINE_PROFILE", "postgresql-postgis-local-single-primary"
    )
    if expected_standbys is None:
        expected_standbys = 2 if engine_profile.endswith("cluster") else 0
    selected_version = engine_version or os.environ.get(
        "MERIDIAN_POSTGRESQL_ENGINE_VERSION", "16-postgis-3.4"
    )
    parsed = conninfo_to_dict(endpoint)
    user = parsed.pop("user", "meridian")
    password = parsed.pop("password", "meridian")
    clean_endpoint = make_conninfo("", **parsed)
    binding = BindingConfig(
        id="postgresql-test",
        adapter_id="postgresql",
        adapter_contract="1.0.0",
        engine_profile=engine_profile,
        engine_version=selected_version,
        endpoint=clean_endpoint,
        service_ref=None,
        physical_namespace="meridian_test",
        tls=TLSPolicy("disabled", None, None, None),
        identity_ref=SecretReference("test", "identity"),
        secret_ref=SecretReference("test", "credential"),
        client=ClientPolicy(
            min_size=1,
            max_size=4,
            acquire_timeout_ms=10_000,
            idle_timeout_ms=30_000,
            operation_timeout_ms=10_000,
            max_result_bytes=16 * 1024 * 1024,
            iterator_lifetime_ms=30_000,
        ),
        required_capability_fingerprint=manifest(
            engine_profile,
            selected_version,
        ).fingerprint,
        required_physical_fingerprint=required_physical_fingerprint,
        compatibility_pins={},
        settings={
            "formatVersion": "meridian.postgresql.settings.v1",
            "applicationName": "runner-conformance",
            "scopeKeys": ["workspace"],
            "topology": {"expectedStandbys": expected_standbys},
            "resources": [],
        },
        extensions={},
    )
    return binding, user, password
