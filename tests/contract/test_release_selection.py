# SPDX-License-Identifier: Apache-2.0
"""Synthetic release metadata tests do not establish server compatibility."""

from dataclasses import replace
from importlib.metadata import version

import pytest
from conftest import make_binding
from meridian_storage.errors import CompatibilityError
from meridian_storage.runtime.config import BindingConfig
from meridian_storage.spi.adapters import AdapterCreateContext, SecretValue

from meridian_storage.adapters.postgresql import PostgreSQLAdapterFactory
from meridian_storage.adapters.postgresql.descriptor import ENGINE_VERSIONS


def context_for(binding):
    return AdapterCreateContext(
        binding=binding, identity=SecretValue(b"meridian"), credential=SecretValue(b"meridian")
    )


@pytest.mark.parametrize("profile", ENGINE_VERSIONS)
@pytest.mark.parametrize("selection", ["18-postgis-3.6", "deployment-image-sha256-aabbcc"])
def test_unlisted_selection_round_trips_without_claiming_conformance(profile, selection):
    binding, _, _ = make_binding(
        "postgresql://localhost/meridian",
        engine_profile=profile,
        engine_version=selection,
        expected_standbys=2 if profile.endswith("cluster") else 0,
    )
    assert selection not in ENGINE_VERSIONS[profile]
    assert BindingConfig.from_mapping(binding.to_dict(), "binding").to_dict() == binding.to_dict()
    runtime = PostgreSQLAdapterFactory().create(context_for(binding))
    assert not runtime._opened  # Metadata construction has not contacted a server.
    runtime.close()


def test_real_core_distribution_selection_replaces_compiled_recipe():
    binding, _, _ = make_binding("postgresql://localhost/meridian")
    binding = replace(
        binding,
        compatibility_pins={
            "coreVersion": "1.0.0",  # Core SPI is not a package name/release.
            "meridian-storage-core": version("meridian-storage-core"),
        },
    )
    assert binding.compatibility_pins["meridian-storage-core"] != "1.0.1"
    runtime = PostgreSQLAdapterFactory().create(context_for(binding))
    runtime.close()


@pytest.mark.parametrize("selected", ["1.0.1", "malformed-lock"])
def test_deployment_package_drift_is_not_waived(selected):
    binding, _, _ = make_binding("postgresql://localhost/meridian")
    binding = replace(binding, compatibility_pins={"meridian-storage-core": selected})
    with pytest.raises(CompatibilityError, match="deployment lock drift"):
        PostgreSQLAdapterFactory().create(context_for(binding))


def test_required_contract_profile_and_canonical_fingerprint_remain_gates():
    binding, _, _ = make_binding("postgresql://localhost/meridian")
    factory = PostgreSQLAdapterFactory()
    for changed, message in (
        (replace(binding, adapter_contract="99.0.0"), "Adapter V1 contract"),
        (replace(binding, engine_profile="unknown-profile"), "Engine profile"),
        (replace(binding, engine_version="deployment-image-other"), "capability fingerprint"),
    ):
        with pytest.raises(CompatibilityError, match=message):
            factory.create(context_for(changed))
