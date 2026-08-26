# SPDX-License-Identifier: Apache-2.0
from meridian_storage.adapters.postgresql.migration import RecoveryHook


def test_recovery_hook_is_descriptive_and_platform_owned() -> None:
    hook = RecoveryHook("postgresql-postgis-cluster")
    assert hook.logical_export_supported
    assert hook.physical_backup_authority == "platform-iac"
    assert hook.physical_restore_authority == "platform-iac"
    assert not hasattr(hook, "backup")
    assert not hasattr(hook, "restore")
