"""Payer 360 v2.0: logical source columns + physical Bronze schemas.

CSV/JSON fields default to STRING with the current Auto Loader readers.
Parquet uses source logical types from the approved blueprint.
No .schema() is passed to Auto Loader; contracts govern table bootstrap.
"""
from pyspark.sql.types import (
    StructField, StructType, StringType,
    DateType, TimestampType, TimestampNTZType,
    LongType, IntegerType, BooleanType, DecimalType
)

import re



# Each entry is column_name:Spark-SQL-type, separated by whitespace.
# This includes all 29 entities from Blueprint v2.0 §06.
BUSINESS = {
    "member_master": """member_id:STRING first_name:STRING last_name:STRING
        birth_date:DATE sex_code:STRING member_status:STRING household_id:STRING
        death_date:DATE phone:STRING email:STRING""",
    "subscription": """subscription_id:STRING subscriber_member_id:STRING
        coverage_market:STRING funding_arrangement:STRING employer_group_id:STRING
        employer_subgroup_id:STRING subscription_status:STRING
        subscription_start_date:DATE subscription_end_date:DATE""",
    "subscription_membership": """subscription_member_id:STRING subscription_id:STRING
        member_id:STRING relationship_code:STRING relationship_start_date:DATE
        relationship_end_date:DATE membership_status:STRING""",
    "coverage": """coverage_id:STRING member_id:STRING subscription_id:STRING
        benefit_plan_id:STRING coverage_start_date:DATE coverage_end_date:DATE
        coverage_status:STRING termination_reason_code:STRING coverage_revision:INT""",
    "enrollment": """enrollment_id:STRING coverage_id:STRING member_id:STRING
        coverage_start_date:DATE coverage_end_date:DATE
        enrollment_start_date:DATE enrollment_end_date:DATE tier_code:STRING
        pcp_practitioner_role_id:STRING change_reason_code:STRING
        coverage_revision:INT""",
    "product": """product_id:STRING product_code:STRING product_name:STRING
        line_of_business:STRING product_type:STRING effective_start_date:DATE
        effective_end_date:DATE product_status:STRING""",
    "benefit_plan": """benefit_plan_id:STRING product_id:STRING plan_code:STRING
        plan_name:STRING coverage_type:STRING deductible_individual:DECIMAL(12,2)
        deductible_family:DECIMAL(12,2) oop_max_individual:DECIMAL(12,2)
        oop_max_family:DECIMAL(12,2) copay_primary_care:DECIMAL(12,2)
        copay_specialist:DECIMAL(12,2) coinsurance_pct:DECIMAL(5,2)
        effective_start_date:DATE effective_end_date:DATE plan_status:STRING""",
    "employer_group": """employer_group_id:STRING group_code:STRING
        group_name:STRING group_type:STRING industry_category:STRING
        sponsor_state:STRING effective_start_date:DATE
        effective_end_date:DATE group_status:STRING""",
    "employer_subgroup": """employer_subgroup_id:STRING employer_group_id:STRING
        subgroup_code:STRING subgroup_name:STRING subgroup_category:STRING
        effective_start_date:DATE effective_end_date:DATE subgroup_status:STRING""",
    "plan_offering": """offering_id:STRING employer_group_id:STRING
        employer_subgroup_id:STRING benefit_plan_id:STRING offering_scope:STRING
        offering_start_date:DATE offering_end_date:DATE offering_status:STRING""",
    "plan_network": """plan_network_id:STRING benefit_plan_id:STRING
        network_id:STRING network_type:STRING effective_start_date:DATE
        effective_end_date:DATE mapping_status:STRING""",
    "specialty": """specialty_id:STRING specialty_code:STRING specialty_name:STRING
        specialty_category:STRING specialty_status:STRING""",
    "network": """network_id:STRING network_code:STRING network_name:STRING
        network_type:STRING network_status:STRING effective_start_date:DATE
        effective_end_date:DATE""",
    "supplier": """supplier_id:STRING organization_npi:STRING
        supplier_name:STRING supplier_type:STRING tax_id_synthetic:STRING
        supplier_status:STRING effective_start_date:DATE effective_end_date:DATE""",
    "supplier_location": """supplier_location_id:STRING supplier_id:STRING
        location_name:STRING location_type:STRING facility_npi:STRING
        location_status:STRING effective_start_date:DATE effective_end_date:DATE""",
    "practitioner": """practitioner_id:STRING individual_npi:STRING
        first_name:STRING last_name:STRING credential_code:STRING
        practitioner_status:STRING effective_start_date:DATE effective_end_date:DATE""",
    "practitioner_specialty": """practitioner_specialty_id:STRING
        practitioner_id:STRING specialty_id:STRING is_primary:BOOLEAN
        effective_start_date:DATE effective_end_date:DATE""",
    "practitioner_role": """practitioner_role_id:STRING practitioner_id:STRING
        supplier_location_id:STRING role_code:STRING specialty_id:STRING
        role_status:STRING effective_start_date:DATE effective_end_date:DATE""",
    "network_participation": """participation_id:STRING network_id:STRING
        participant_entity_type:STRING participant_entity_id:STRING
        supplier_location_id:STRING participation_status:STRING
        effective_start_date:DATE effective_end_date:DATE""",
    "provider_contract": """contract_id:STRING network_id:STRING
        contract_entity_type:STRING contract_entity_id:STRING
        contract_name:STRING contract_status:STRING
        effective_start_date:DATE effective_end_date:DATE""",
    "provider_contract_rate": """contract_rate_id:STRING contract_id:STRING
        benefit_plan_id:STRING procedure_code:STRING modifier_code:STRING
        place_of_service_code:STRING negotiated_rate:DECIMAL(14,2)
        effective_start_date:DATE effective_end_date:DATE""",
    "address_master": """address_id:STRING address_line_1:STRING
        address_line_2:STRING city:STRING state_code:STRING postal_code:STRING
        county_name:STRING country_code:STRING address_quality_code:STRING
        latitude:DECIMAL(10,7) longitude:DECIMAL(10,7)""",
    "entity_address": """entity_address_id:STRING entity_type:STRING
        entity_id:STRING address_id:STRING address_type:STRING
        is_primary:BOOLEAN address_status:STRING effective_start_date:DATE
        effective_end_date:DATE""",
    "claim_header": """claim_id:STRING member_id:STRING claim_type:STRING
        service_from_date:DATE service_to_date:DATE received_date:DATE
        adjudication_date:DATE billing_supplier_id:STRING
        facility_location_id:STRING primary_diagnosis_code:STRING
        source_claim_status:STRING original_claim_id:STRING submitted_plan_id:STRING""",
    "claim_line": """claim_id:STRING line_number:INT service_date:DATE
        rendering_practitioner_role_id:STRING procedure_code:STRING
        modifier_code:STRING units:DECIMAL(10,2)
        billed_amount:DECIMAL(14,2) allowed_amount:DECIMAL(14,2)
        member_responsibility_amount:DECIMAL(14,2)
        payer_liability_amount:DECIMAL(14,2) adjudication_status:STRING
        denial_reason_code:STRING""",
    "reference_codes": """code_set:STRING code:STRING description:STRING
        category:STRING effective_start_date:DATE effective_end_date:DATE
        code_status:STRING""",
    "claim_status_event": """claim_status_event_id:STRING claim_id:STRING
        status_code:STRING status_reason_code:STRING
        status_effective_date:DATE event_ts:TIMESTAMP source_sequence:BIGINT""",
    "claim_adjustment": """adjustment_id:STRING claim_id:STRING line_number:INT
        adjustment_type:STRING adjustment_reason_code:STRING
        allowed_amount_delta:DECIMAL(14,2)
        member_responsibility_delta:DECIMAL(14,2)
        payer_liability_delta:DECIMAL(14,2)
        adjustment_effective_date:DATE event_ts:TIMESTAMP
        reversal_of_adjustment_id:STRING""",
    "payment_remittance": """remittance_transaction_id:STRING
        remittance_batch_id:STRING payment_id:STRING claim_id:STRING
        line_number:INT payee_supplier_id:STRING payment_date:DATE
        posted_date:DATE transaction_type:STRING
        payment_amount:DECIMAL(14,2) payment_status:STRING
        original_transaction_id:STRING currency_code:STRING""",
}

SNAPSHOT_ENVELOPE = """
    source_system:STRING
    batch_id:STRING
    snapshot_as_of_ts:TIMESTAMP_NTZ
    snapshot_high_watermark:BIGINT
"""

CDC_ENVELOPE = """
    source_system:STRING
    batch_id:STRING
    cdc_event_id:STRING
    operation:STRING
    source_sequence:BIGINT
    source_updated_at:TIMESTAMP_NTZ
    source_record_version:INT
    event_ts:TIMESTAMP_NTZ
"""

EVENT_ENVELOPE = """source_system:STRING batch_id:STRING"""

# EXACT columns currently produced by ingestion.add_audit_columns().
BRONZE_AUDIT = """_ingest_ts:TIMESTAMP _source_file:STRING
    _ingest_run_id:STRING _batch_id:STRING _ingest_date:DATE"""


def parse_columns(spec):
    result = {}
    for item in spec.split():
        name, type_name = item.split(":", 1)
        if name in result and result[name] != type_name:
            raise ValueError(f"Conflicting column type: {name}")
        result[name] = type_name
    return result


def expected_source_columns(entity, feed):
    if entity not in BUSINESS:
        raise ValueError(f"Unknown source entity: {entity}")
    spec = BUSINESS[entity]
    if feed == "snapshot":
        spec += " " + SNAPSHOT_ENVELOPE
    elif feed == "cdc":
        spec += " " + CDC_ENVELOPE
    elif feed == "events":
        spec += " " + EVENT_ENVELOPE
    else:
        raise ValueError(feed)
    return parse_columns(spec)


def physical_columns(entity, feed, file_type):
    # CSV/JSON are inferred as STRING by our current Auto Loader config.
    columns = expected_source_columns(entity, feed)
    if file_type in ("csv", "json"):
        columns = {key: "STRING" for key in columns}
    elif file_type != "parquet":
        raise ValueError(file_type)
    columns.update(parse_columns(BRONZE_AUDIT))
    return columns


def to_type(name):
    basic = {
        "STRING": StringType,
        "DATE": DateType,
        "TIMESTAMP": TimestampType,
        "TIMESTAMP_NTZ": TimestampNTZType,
        "BIGINT": LongType,
        "INT": IntegerType,
        "BOOLEAN": BooleanType,
    }

    if name in basic:
        return basic[name]()

    match = re.fullmatch(
        r"DECIMAL\((\d+),(\d+)\)", name
    )

    if match:
        return DecimalType(
            int(match.group(1)),
            int(match.group(2))
        )

    raise ValueError(
        f"Unrecognized type: {name}"
    )


def expected_schema(entity, feed, file_type):
    return StructType([
        StructField(name, to_type(type_name), True)
        for name, type_name in physical_columns(entity, feed, file_type).items()
    ])
