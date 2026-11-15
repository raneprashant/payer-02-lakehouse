"""Dry-run first, then idempotently initialize all 55 Bronze Delta targets."""
import fnmatch
import os

from payer_lakehouse.bronze.snapshot_config import SNAPSHOT_CONFIG
from payer_lakehouse.bronze.cdc_config import CDC_CONFIG
from payer_lakehouse.bronze.event_config import EVENT_CONFIG
from payer_lakehouse.bronze.source_contracts import (
    BUSINESS, expected_schema, expected_source_columns,
)


def get_feeds():
    mutable = set(SNAPSHOT_CONFIG)
    if mutable != set(CDC_CONFIG) or len(mutable) != 26:
        raise ValueError("Snapshot/CDC config entity mismatch or wrong count")
    if set(EVENT_CONFIG) & mutable or len(EVENT_CONFIG) != 3:
        raise ValueError("Event config mismatch or wrong count")
    if set(BUSINESS) != (mutable | set(EVENT_CONFIG)):
        raise ValueError("Source contracts do not match the 29 configured entities")
    feeds = []
    for entity in sorted(mutable):
        feeds += [
            (entity, "snapshot", SNAPSHOT_CONFIG[entity]),
            (entity, "cdc", CDC_CONFIG[entity]),
        ]
    for entity, config in sorted(EVENT_CONFIG.items()):
        feeds.append((entity, "events", config))
    assert len(feeds) == 55
    return feeds


def columns_by_name(schema):
    result = {}
    for field in schema.fields:
        low = field.name.lower()
        if low in result:
            raise ValueError(f"Duplicate case-insensitive field: {field.name}")
        result[low] = field
    return result


def compare_schemas(expected, actual, *, label, allow_extra=True):
    a, b = columns_by_name(expected), columns_by_name(actual)
    errors, notes = [], []
    for name, exp in a.items():
        got = b.get(name)
        if got is None:
            errors.append(f"{label} missing {exp.name}: {exp.dataType.simpleString()}")
        elif got.dataType != exp.dataType:
            errors.append(
                f"{label} {exp.name}: expected {exp.dataType.simpleString()}, "
                f"found {got.dataType.simpleString()}"
            )
    extra = sorted(set(b) - set(a))
    if extra:
        msg = f"{label} additional fields: {', '.join(extra)}"
        (notes if allow_extra else errors).append(msg)
    return errors, notes


def sample_source_schema(spark, path, pattern, file_type):
    """Inspect first available raw source file; no files means no sample."""
    try:
        names = sorted(n for n in os.listdir(path)
                       if fnmatch.fnmatch(n, pattern))
    except FileNotFoundError:
        names = []
    if not names:
        return None, None
    filename = names[0]
    file_path = f"{path.rstrip('/')}/{filename}"
    reader = spark.read.format(file_type)
    if file_type == "csv":
        reader = reader.option("header", "true").option("inferSchema", "false")
    if file_type == "json":
        reader = reader.option("primitivesAsString", "true")
    return reader.load(file_path).schema, filename


def inspect_one(spark, catalog, entity, feed, config):
    file_type, pattern = config["file_type"], config["file_pattern"]
    suffix = "events" if feed == "events" else feed
    target = f"{catalog}.bronze.{entity}_{suffix}"
    exp = expected_schema(entity, feed, file_type)
    errors, notes = [], []
    exists = spark.catalog.tableExists(target)
    if exists:
        actual = spark.table(target).schema
        ee, nn = compare_schemas(exp, actual, label="TABLE")
        errors += ee
        notes += nn
    source_dir = f"/Volumes/{catalog}/landing/raw_files/landing/{entity}"
    source_schema, sample = sample_source_schema(
        spark, source_dir, pattern, file_type
    )
    if source_schema is not None:
        # Compare only source fields; audit fields are generated in Spark.
        expected_source = expected_source_columns(entity, feed)
        physical_source = {
            k: ("STRING" if file_type in ("json", "csv") else t)
            for k, t in expected_source.items()
        }
        src_expected = expected_schema(entity, feed, file_type)
        src_names = {x.lower() for x in physical_source}
        filtered = type(src_expected)([
            f for f in src_expected.fields if f.name.lower() in src_names
        ])
        ee, nn = compare_schemas(filtered, source_schema, label="SOURCE")
        errors += ee
        notes += nn
    return {
        "entity": entity, "feed": feed, "target": target,
        "exists": exists, "sample": sample,
        "schema": exp, "errors": errors, "notes": notes,
    }


def run(spark, catalog, apply=False):
    if catalog not in ("payer_02_dev", "payer_02_test", "payer_02_prod"):
        raise ValueError(f"Invalid catalog: {catalog}")
    all_feeds = get_feeds()
    reports = []
    print(f"MODE={'APPLY' if apply else 'DRY_RUN'} CATALOG={catalog}")

    # GLOBAL PREFLIGHT: do not write anything until all checks pass.
    for entity, feed, config in all_feeds:
        item = inspect_one(spark, catalog, entity, feed, config)
        reports.append(item)
        state = "ERROR" if item["errors"] else (
            "EXISTS" if item["exists"] else "WOULD_CREATE"
        )
        print(f"{state:12} {item['target']}  sample={item['sample'] or 'none'}")
        for message in item["notes"]:
            print(f"  INFO  {message}")
        for message in item["errors"]:
            print(f"  ERROR {message}")

    bad = [r for r in reports if r["errors"]]
    print(f"SUMMARY total={len(reports)} existing={sum(r['exists'] for r in reports)} "
          f"missing={sum(not r['exists'] for r in reports)} "
          f"errors={len(bad)}")
    if bad:
        if not apply:
            print(
                f"DRY RUN COMPLETED WITH {len(bad)} "
                "SCHEMA ERROR(S)."
            )
            print(
                "No tables were modified. "
                "Review ERROR lines before applying."
            )
            return

        raise RuntimeError(
            "SCHEMA PREFLIGHT FAILED; NO TABLES CREATED. "
            "Review ERROR lines before applying."
        )

    if not apply:
        print("DRY RUN PASSED; no tables changed")
        return

    created = 0
    for row in reports:
        if row["exists"]:
            continue
        columns = ",\n    ".join(
            f"`{f.name}` {f.dataType.simpleString().upper()}"
            for f in row["schema"].fields
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {row['target']} "
                  f"(\n    {columns}\n) USING DELTA")
        created += 1
        print(f"CREATED {row['target']}")

    missing = [r["target"] for r in reports
               if not spark.catalog.tableExists(r["target"])]
    if missing:
        raise RuntimeError(f"Still missing after apply: {missing}")
    print(f"SUCCESS: 55 Bronze tables present; newly created={created}")
