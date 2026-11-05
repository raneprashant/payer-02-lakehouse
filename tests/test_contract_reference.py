"""Test the copied generator contract so Lakehouse exercise names cannot drift."""
import json
from pathlib import Path

CONTRACT = Path(__file__).resolve().parents[1] / "docs" / "SOURCE_CONTRACTS_REFERENCE.json"


def test_contracts_have_29_entities():
    data = json.loads(CONTRACT.read_text(encoding="utf-8"))
    assert len(data["sources"]) == 29
    assert len(data["snapshot_steps"]) == 12


def test_enrollment_has_four_dates_and_no_enrollment_status():
    data = json.loads(CONTRACT.read_text(encoding="utf-8"))
    cols = {c["name"] for c in data["sources"]["enrollment"]["business_columns"]}
    for field in ("coverage_start_date", "coverage_end_date", "enrollment_start_date", "enrollment_end_date"):
        assert field in cols
    assert "enrollment_status" not in cols


def test_address_bridge_has_start_and_end_dates():
    data = json.loads(CONTRACT.read_text(encoding="utf-8"))
    cols = {c["name"] for c in data["sources"]["entity_address"]["business_columns"]}
    assert {"effective_start_date", "effective_end_date"} <= cols
