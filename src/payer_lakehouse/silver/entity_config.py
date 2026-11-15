
######################################
# Payer 360 - Silver Conformed Configs
######################################

ENTITY_CONFIG = {

    # ==================================
    # 01 - REFERENCE & NETWORK
    # ==================================

    "reference_codes": {
        "keys": ["code_set", "code", "effective_start_date"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    "specialty": {
        "keys": ["specialty_id"],
        "scd_type": 1,
        "strategy": "CDC_MERGE",
        "soft_delete": True,
    },

    "network": {
        "keys": ["network_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "network_code",
            "network_name",
            "network_type",
            "network_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    # ==================================
    # 02 - PRODUCT & PLAN
    # ==================================

    "product": {
        "keys": ["product_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "product_code",
            "product_name",
            "line_of_business",
            "product_type",
            "effective_start_date",
            "effective_end_date",
            "product_status",
            "is_deleted",
        ],
    },

    "benefit_plan": {
        "keys": ["benefit_plan_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "product_id",
            "plan_code",
            "plan_name",
            "coverage_type",
            "deductible_individual",
            "deductible_family",
            "oop_max_individual",
            "oop_max_family",
            "copay_primary_care",
            "copay_specialist",
            "coinsurance_pct",
            "effective_start_date",
            "effective_end_date",
            "plan_status",
            "is_deleted",
        ],
    },

    "employer_group": {
        "keys": ["employer_group_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "group_code",
            "group_name",
            "group_type",
            "group_status",
            "industry_category",
            "sponsor_state",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "employer_subgroup": {
        "keys": ["employer_subgroup_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "employer_group_id",
            "subgroup_code",
            "subgroup_name",
            "subgroup_category",
            "subgroup_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "plan_offering": {
        "keys": ["offering_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    "plan_network": {
        "keys": ["plan_network_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    # ==================================
    # 03 - ADDRESS & ENTITY
    # ==================================

    "address_master": {
        "keys": ["address_id"],
        "scd_type": 2,
        "strategy": "CDC_MERGE",
        "soft_delete": True,
        "track_history_column_list": [
            "address_line_1",
            "address_line_2",
            "city",
            "state_code",
            "postal_code",
            "county_name",
            "country_code",
            "address_quality_code",
            "latitude",
            "longitude",
            "is_deleted",
        ],
    },

    "entity_address": {
        "keys": ["entity_address_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "entity_type",
            "entity_id",
            "address_id",
            "address_type",
            "is_primary",
            "address_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    # ==================================
    # 04 - PROVIDER & CONTRACTS
    # ==================================

    "supplier": {
        "keys": ["supplier_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "organization_npi",
            "supplier_name",
            "supplier_type",
            "tax_id_synthetic",
            "supplier_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "supplier_location": {
        "keys": ["supplier_location_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "supplier_id",
            "location_name",
            "location_type",
            "facility_npi",
            "location_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "practitioner": {
        "keys": ["practitioner_id"],
        "scd_type": 2,
        "strategy": "CDC_MERGE",
        "soft_delete": True,
        "track_history_column_list": [
            "individual_npi",
            "first_name",
            "last_name",
            "credential_code",
            "practitioner_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "practitioner_specialty": {
        "keys": ["practitioner_specialty_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    "practitioner_role": {
        "keys": ["practitioner_role_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "practitioner_id",
            "supplier_location_id",
            "role_code",
            "specialty_id",
            "role_status",
            "effective_start_date",
            "effective_end_date",
            "is_deleted",
        ],
    },

    "network_participation": {
        "keys": ["participation_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    "provider_contract": {
        "keys": ["contract_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    "provider_contract_rate": {
        "keys": ["contract_rate_id"],
        "scd_type": 1,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
    },

    # ==================================
    # 05 - MEMBERSHIP & ELIGIBILITY
    # ==================================

    "member_master": {
        "keys": ["member_id"],
        "scd_type": 2,
        "strategy": "AUTO_CDC",
        "soft_delete": True,
        "track_history_column_list": [
            "first_name",
            "last_name",
            "birth_date",
            "sex_code",
            "member_status",
            "household_id",
            "death_date",
            "phone",
            "email",
            "is_deleted",
        ],
    },

    "subscription": {
        "keys": ["subscription_id"],
        "scd_type": 2,
        "strategy": "AUTO_CDC",
        "soft_delete": True,
        "track_history_column_list": [
            "subscriber_member_id",
            "coverage_market",
            "funding_arrangement",
            "employer_group_id",
            "employer_subgroup_id",
            "subscription_status",
            "subscription_start_date",
            "subscription_end_date",
            "is_deleted",
        ],
    },

    "subscription_membership": {
        "keys": ["subscription_member_id"],
        "scd_type": 2,
        "strategy": "EFFECTIVE_DATED_UPSERT",
        "soft_delete": True,
        "track_history_column_list": [
            "subscription_id",
            "member_id",
            "relationship_code",
            "relationship_start_date",
            "relationship_end_date",
            "membership_status",
            "is_deleted",
        ],
    },

    "coverage": {
        "keys": ["coverage_id"],
        "scd_type": 1,
        "strategy": "AFFECTED_COVERAGE_REBUILD",
        "soft_delete": True,
    },

    "enrollment": {
        "keys": ["enrollment_id"],
        "scd_type": 1,
        "strategy": "TEMPORAL_RECONSTRUCTION",
        "soft_delete": True,
    },

    # ==================================
    # 06 - CLAIMS & FINANCE
    # ==================================

    "claim_header": {
        "keys": ["claim_id"],
        "scd_type": 1,
        "strategy": "CDC_MERGE",
        "soft_delete": True,
    },

    "claim_line": {
        "keys": ["claim_id", "line_number"],
        "scd_type": 1,
        "strategy": "CDC_MERGE",
        "soft_delete": True,
    },

    "claim_status_event": {
        "keys": ["claim_status_event_id"],
        "strategy": "APPEND_EVENT",
        "soft_delete": False,
    },

    "claim_adjustment": {
        "keys": ["adjustment_id"],
        "strategy": "APPEND_EVENT",
        "soft_delete": False,
    },

    "payment_remittance": {
        "keys": ["remittance_transaction_id"],
        "strategy": "APPEND_EVENT",
        "soft_delete": False,
    },
}



def get_entity_config(entity_name):
    return ENTITY_CONFIG[entity_name]



# Business columns for Membership & Eligibility entities.
# The type is the target Silver data type.

ENTITY_BUSINESS_COLUMNS = {

    "member_master": {
        "member_id": "string",
        "first_name": "string",
        "last_name": "string",
        "birth_date": "date",
        "sex_code": "string",
        "member_status": "string",
        "household_id": "string",
        "death_date": "date",
        "phone": "string",
        "email": "string",
    },
    
    "subscription": {
        "subscription_id": "string",
        "subscriber_member_id": "string",
        "coverage_market": "string",
        "funding_arrangement": "string",
        "employer_group_id": "string",
        "employer_subgroup_id": "string",
        "subscription_status": "string",
        "subscription_start_date": "date",
        "subscription_end_date": "date",
    },

    "subscription_membership": {
        "subscription_member_id": "string",
        "subscription_id": "string",
        "member_id": "string",
        "relationship_code": "string",
        "relationship_start_date": "date",
        "relationship_end_date": "date",
        "membership_status": "string",
    },

    "coverage": {
        "coverage_id": "string",
        "member_id": "string",
        "subscription_id": "string",
        "benefit_plan_id": "string",
        "coverage_start_date": "date",
        "coverage_end_date": "date",
        "coverage_status": "string",
        "termination_reason_code": "string",
        "coverage_revision": "int",
    },

    "enrollment": {
        "enrollment_id": "string",
        "coverage_id": "string",
        "member_id": "string",
        "coverage_start_date": "date",
        "coverage_end_date": "date",
        "enrollment_start_date": "date",
        "enrollment_end_date": "date",
        "tier_code": "string",
        "pcp_practitioner_role_id": "string",
        "change_reason_code": "string",
        "coverage_revision": "int",
    },
}

    # PRODUCT & PLAN DOMAIN

ENTITY_BUSINESS_COLUMNS.update({

    "product": {
        "product_id": "string",
        "product_code": "string",
        "product_name": "string",
        "line_of_business": "string",
        "product_type": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "product_status": "string",
    },

    "benefit_plan": {
        "benefit_plan_id": "string",
        "product_id": "string",
        "plan_code": "string",
        "plan_name": "string",
        "coverage_type": "string",
        "deductible_individual": "decimal(12,2)",
        "deductible_family": "decimal(12,2)",
        "oop_max_individual": "decimal(12,2)",
        "oop_max_family": "decimal(12,2)",
        "copay_primary_care": "decimal(12,2)",
        "copay_specialist": "decimal(12,2)",
        "coinsurance_pct": "decimal(5,2)",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "plan_status": "string",
    },

    "employer_group": {
        "employer_group_id": "string",
        "group_code": "string",
        "group_name": "string",
        "group_type": "string",
        "industry_category": "string",
        "sponsor_state": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "group_status": "string",
    },

    "employer_subgroup": {
        "employer_subgroup_id": "string",
        "employer_group_id": "string",
        "subgroup_code": "string",
        "subgroup_name": "string",
        "subgroup_category": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "subgroup_status": "string",
    },

    "plan_offering": {
        "offering_id": "string",
        "employer_group_id": "string",
        "employer_subgroup_id": "string",
        "benefit_plan_id": "string",
        "offering_scope": "string",
        "offering_start_date": "date",
        "offering_end_date": "date",
        "offering_status": "string",
    },

    "plan_network": {
        "plan_network_id": "string",
        "benefit_plan_id": "string",
        "network_id": "string",
        "network_type": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "mapping_status": "string",
    },
})


ENTITY_BUSINESS_COLUMNS.update({

    
    # REFERENCE & NETWORK BUSINESS COLUMNS

    "reference_codes": {
        "code_set": "string",
        "code": "string",
        "description": "string",
        "category": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
        "code_status": "string",
    },

    "specialty": {
        "specialty_id": "string",
        "specialty_code": "string",
        "specialty_name": "string",
        "specialty_category": "string",
        "specialty_status": "string",
    },

    "network": {
        "network_id": "string",
        "network_code": "string",
        "network_name": "string",
        "network_type": "string",
        "network_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

})


ENTITY_BUSINESS_COLUMNS.update({

# address_master and entity_address  COLUMNS

    "address_master": {
        "address_id": "string",
        "address_line_1": "string",
        "address_line_2": "string",
        "city": "string",
        "state_code": "string",
        "postal_code": "string",
        "county_name": "string",
        "country_code": "string",
        "address_quality_code": "string",
        "latitude": "decimal(10,7)",
        "longitude": "decimal(10,7)",
    },

    "entity_address": {
        "entity_address_id": "string",
        "entity_type": "string",
        "entity_id": "string",
        "address_id": "string",
        "address_type": "string",
        "is_primary": "boolean",
        "address_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

})



# ==========================================
# PROVIDER & CONTRACTS BUSINESS COLUMNS
# ==========================================

ENTITY_BUSINESS_COLUMNS.update({

    "supplier": {
        "supplier_id": "string",
        "organization_npi": "string",
        "supplier_name": "string",
        "supplier_type": "string",
        "tax_id_synthetic": "string",
        "supplier_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "supplier_location": {
        "supplier_location_id": "string",
        "supplier_id": "string",
        "location_name": "string",
        "location_type": "string",
        "facility_npi": "string",
        "location_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "practitioner": {
        "practitioner_id": "string",
        "individual_npi": "string",
        "first_name": "string",
        "last_name": "string",
        "credential_code": "string",
        "practitioner_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "practitioner_specialty": {
        "practitioner_specialty_id": "string",
        "practitioner_id": "string",
        "specialty_id": "string",
        "is_primary": "boolean",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "practitioner_role": {
        "practitioner_role_id": "string",
        "practitioner_id": "string",
        "supplier_location_id": "string",
        "role_code": "string",
        "specialty_id": "string",
        "role_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "network_participation": {
        "participation_id": "string",
        "network_id": "string",
        "participant_entity_type": "string",
        "participant_entity_id": "string",
        "supplier_location_id": "string",
        "participation_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "provider_contract": {
        "contract_id": "string",
        "network_id": "string",
        "contract_entity_type": "string",
        "contract_entity_id": "string",
        "contract_name": "string",
        "contract_status": "string",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

    "provider_contract_rate": {
        "contract_rate_id": "string",
        "contract_id": "string",
        "benefit_plan_id": "string",
        "procedure_code": "string",
        "modifier_code": "string",
        "place_of_service_code": "string",
        "negotiated_rate": "decimal(14,2)",
        "effective_start_date": "date",
        "effective_end_date": "date",
    },

})


# ============================================================
# 06 - CLAIMS & FINANCE BUSINESS COLUMNS
# ============================================================

ENTITY_BUSINESS_COLUMNS.update({

    "claim_header": {
        "claim_id": "string",
        "member_id": "string",
        "claim_type": "string",
        "service_from_date": "date",
        "service_to_date": "date",
        "received_date": "date",
        "adjudication_date": "date",
        "billing_supplier_id": "string",
        "facility_location_id": "string",
        "primary_diagnosis_code": "string",
        "source_claim_status": "string",
        "original_claim_id": "string",
        "submitted_plan_id": "string",
    },

    "claim_line": {
        "claim_id": "string",
        "line_number": "int",
        "service_date": "date",
        "rendering_practitioner_role_id": "string",
        "procedure_code": "string",
        "modifier_code": "string",
        "units": "decimal(10,2)",
        "billed_amount": "decimal(14,2)",
        "allowed_amount": "decimal(14,2)",
        "member_responsibility_amount": "decimal(14,2)",
        "payer_liability_amount": "decimal(14,2)",
        "adjudication_status": "string",
        "denial_reason_code": "string",
    },

    "claim_status_event": {
        "claim_status_event_id": "string",
        "claim_id": "string",
        "status_code": "string",
        "status_reason_code": "string",
        "status_effective_date": "date",
        "event_ts": "timestamp",
        "source_sequence": "long",
    },

    "claim_adjustment": {
        "adjustment_id": "string",
        "claim_id": "string",
        "line_number": "int",
        "adjustment_type": "string",
        "adjustment_reason_code": "string",
        "allowed_amount_delta": "decimal(14,2)",
        "member_responsibility_delta": "decimal(14,2)",
        "payer_liability_delta": "decimal(14,2)",
        "adjustment_effective_date": "date",
        "event_ts": "timestamp",
        "reversal_of_adjustment_id": "string",
    },

    "payment_remittance": {
        "remittance_transaction_id": "string",
        "remittance_batch_id": "string",
        "payment_id": "string",
        "claim_id": "string",
        "line_number": "int",
        "payee_supplier_id": "string",
        "payment_date": "date",
        "posted_date": "date",
        "transaction_type": "string",
        "payment_amount": "decimal(14,2)",
        "payment_status": "string",
        "original_transaction_id": "string",
        "currency_code": "string",
    },

})
