"""Centralized, testable Unity Catalog path conventions (no Spark dependency)."""

import re
from dataclasses import dataclass

_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")
_ALLOWED_TARGET_CATALOGS = {
    "dev": "payer_02_dev",
    "test": "payer_02_test",
    "prod": "payer_02_prod",
}


def assert_identifier(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


@dataclass(frozen=True)
class LakehousePaths:
    """No credentials or workspace addresses needed: UC volumes are target-aware."""

    target: str = "dev"
    catalog: str = "payer_02_dev"
    landing_schema: str = "landing"
    landing_volume: str = "raw_files"
    control_schema: str = "control"
    control_volume: str = "checkpoints"

    def __post_init__(self) -> None:
        if self.target not in _ALLOWED_TARGET_CATALOGS:
            raise ValueError(f"Unknown target: {self.target}")
        if self.catalog != _ALLOWED_TARGET_CATALOGS[self.target]:
            raise ValueError("Bundle target must match its approved catalog")
        for name in (self.catalog, self.landing_schema, self.landing_volume,
                     self.control_schema, self.control_volume):
            assert_identifier(name)

    def entity_landing(self, entity: str) -> str:
        assert_identifier(entity)
        return (f"/Volumes/{self.catalog}/{self.landing_schema}/"
                f"{self.landing_volume}/landing/{entity}")

    def checkpoint(self, stream_name: str) -> str:
        assert_identifier(stream_name)
        return (f"/Volumes/{self.catalog}/{self.control_schema}/"
                f"{self.control_volume}/{stream_name}")

    def bronze_table(self, entity: str, feed: str) -> str:
        assert_identifier(entity)
        if feed not in ("snapshot", "cdc", "events"):
            raise ValueError("feed must be snapshot, cdc or events")
        return f"{self.catalog}.bronze.{entity}_{feed}"
