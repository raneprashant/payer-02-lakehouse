CDC_CONFIG = {
    #Product, Group and plan Domain config
    "product": {
        "file_type": "json",
        "file_pattern": "product_cdc_*.json"
    },
    "benefit_plan": {
        "file_type": "json",
        "file_pattern": "benefit_plan_cdc_*.json"
    },
    "employer_group": {
        "file_type": "json",
        "file_pattern": "employer_group_cdc_*.json"
    },
    "employer_subgroup": {
        "file_type": "json",
        "file_pattern": "employer_subgroup_cdc_*.json"
    },
    "plan_offering": {
        "file_type": "json",
        "file_pattern": "plan_offering_cdc_*.json"
    },
    "plan_network": {
        "file_type": "json",
        "file_pattern": "plan_network_cdc_*.json"
    },
    
    #Reference Codes and network Domain config
    "reference_codes": {
        "file_type": "json",
        "file_pattern": "reference_codes_cdc_*.json"
    },
    "specialty": {
        "file_type" : "json",
        "file_pattern": "specialty_cdc_*.json"
    },
    "network": {
            "file_type" : "json",
            "file_pattern": "network_cdc_*.json"
    },
    
    #Address and entity Domain config
    "address_master":{
        "file_type":"json",
        "file_pattern":"address_master_cdc_*.json",
    },

    "entity_address":{
        "file_type":"json",
        "file_pattern":"entity_address_cdc_*.json",
    },

    #Provider and Contracts domaini config 

    "practitioner":{
        "file_type":"json",
        "file_pattern":"practitioner_cdc_*.json",        
    },
    "practitioner_specialty":{
        "file_type":"json",
        "file_pattern":"practitioner_specialty_cdc_*.json",      
    },
    "practitioner_role":{
        "file_type":"json",
        "file_pattern":"practitioner_role_cdc_*.json",      
    },
    "supplier": {
        "file_type":"json",
        "file_pattern":"supplier_cdc_*.json",      
    },
    "supplier_location":{
        "file_type":"json",
        "file_pattern":"supplier_location_cdc_*.json",      
    },
    "network_participation":{
        "file_type":"json",
        "file_pattern":"network_participation_cdc_*.json",      
    },
    "provider_contract":{
        "file_type":"json",
        "file_pattern":"provider_contract_cdc_*.json",      
    },
    "provider_contract_rate":{
        "file_type":"json",
        "file_pattern":"provider_contract_rate_cdc_*.json",      
    },

    #Membership and Eligibility domain config 
    "member_master":{
        "file_type":"parquet",
        "file_pattern":"member_master_cdc_*.parquet",    
    },
    "subscription":{
        "file_type":"json",
        "file_pattern":"subscription_cdc_*.json",    
    },
    "subscription_membership":{
        "file_type":"json",
        "file_pattern":"subscription_membership_cdc_*.json",    
    },
    "coverage":{
        "file_type":"parquet",
        "file_pattern":"coverage_cdc_*.parquet",    
    },
    "enrollment":{
        "file_type":"parquet",
        "file_pattern":"enrollment_cdc_*.parquet",    
    },

    #Claims and Finance Domain
    "claim_header":{
        "file_type":"parquet",
        "file_pattern":"claim_header_cdc_*.parquet",            
    },
    "claim_line":{
        "file_type":"parquet",
        "file_pattern":"claim_line_cdc_*.parquet",    
    }

}