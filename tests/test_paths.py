import pytest

from payer_lakehouse.paths import LakehousePaths


def test_dev_paths_use_approved_volume_structure():
    p = LakehousePaths()
    assert p.entity_landing("member_master") == (
        "/Volumes/payer_02_dev/landing/raw_files/landing/member_master"
    )
    assert p.bronze_table("member_master", "snapshot") == (
        "payer_02_dev.bronze.member_master_snapshot"
    )
    assert p.checkpoint("member_master_snapshot") == (
        "/Volumes/payer_02_dev/control/checkpoints/member_master_snapshot"
    )


@pytest.mark.parametrize("target,catalog", [
    ("dev", "payer_02_dev"), ("test", "payer_02_test"),
    ("prod", "payer_02_prod")
])
def test_isolated_catalogs(target, catalog):
    assert LakehousePaths(target=target, catalog=catalog).catalog == catalog


def test_cannot_accidentally_route_dev_to_prod_catalog():
    with pytest.raises(ValueError, match="target must match"):
        LakehousePaths(target="dev", catalog="payer_02_prod")


def test_reject_invalid_identifiers():
    with pytest.raises(ValueError, match="Invalid identifier"):
        LakehousePaths().entity_landing("member-master/../other")


def test_snapshot_and_cdc_checkpoints_are_distinct():
    p = LakehousePaths()
    assert p.checkpoint("member_master_snapshot") != p.checkpoint("member_master_cdc")
