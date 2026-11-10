SNAPSHOT_CONFIG = {
    #Product, Group and plan Domain config
    "product": {
        "file_type": "csv",
        "file_pattern": "product_snapshot_*.csv"
    },
    "benefit_plan": {
        "file_type": "csv",
        "file_pattern": "benefit_plan_snapshot_*.csv"
    },
    "employer_group": {
        "file_type": "csv",
        "file_pattern": "employer_group_snapshot_*.csv"
    },
    "employer_subgroup": {
        "file_type": "csv",
        "file_pattern": "employer_subgroup_snapshot_*.csv"
    },
    "plan_offering": {
        "file_type": "csv",
        "file_pattern": "plan_offering_snapshot_*.csv"
    },
    "plan_network": {
        "file_type": "csv",
        "file_pattern": "plan_network_snapshot_*.csv"
    },
    
    #Reference Codes and network Domain config
    "reference_codes": {
        "file_type": "csv",
        "file_pattern": "reference_codes_snapshot_*.csv"
    },
    "specialty": {
        "file_type" : "csv",
        "file_pattern": "specialty_snapshot_*.csv"
    },
    "network": {
            "file_type" : "csv",
            "file_pattern": "network_snapshot_*.csv"
    },
    
    #Address and entity Domain config
    "address_master":{
        "file_type":"csv",
        "file_pattern":"address_master_snapshot_*.csv",
    },

    "entity_address":{
        "file_type":"csv",
        "file_pattern":"entity_address_snapshot_*.csv",
    },

    #Provider and Contracts domaini config 

    "practitioner":{
        "file_type":"csv",
        "file_pattern":"practitioner_snapshot_*.csv",        
    },
    "practitioner_specialty":{
        "file_type":"csv",
        "file_pattern":"practitioner_specialty_snapshot_*.csv",      
    },
    "practitioner_role":{
        "file_type":"csv",
        "file_pattern":"practitioner_role_snapshot_*.csv",      
    },
    "supplier": {
        "file_type":"csv",
        "file_pattern":"supplier_snapshot_*.csv",      
    },
    "supplier_location":{
        "file_type":"csv",
        "file_pattern":"supplier_location_snapshot_*.csv",      
    },
    "network_participation":{
        "file_type":"csv",
        "file_pattern":"network_participation_snapshot_*.csv",      
    },
    "provider_contract":{
        "file_type":"csv",
        "file_pattern":"provider_contract_snapshot_*.csv",      
    },
    "provider_contract_rate":{
        "file_type":"csv",
        "file_pattern":"provider_contract_rate_snapshot_*.csv",      
    },

    #Membership and Eligibility domain config 
    "member_master":{
        "file_type":"parquet",
        "file_pattern":"member_master_snapshot_*.parquet",    
    },
    "subscription":{
        "file_type":"csv",
        "file_pattern":"subscription_snapshot_*.csv",    
    },
    "subscription_membership":{
        "file_type":"csv",
        "file_pattern":"subscription_membership_snapshot_*.csv",    
    },
    "coverage":{
        "file_type":"parquet",
        "file_pattern":"coverage_snapshot_*.parquet",    
    },
    "enrollment":{
        "file_type":"parquet",
        "file_pattern":"enrollment_snapshot_*.parquet",    
    },

    #Claims and Finance Domain
    "claim_header":{
        "file_type":"parquet",
        "file_pattern":"claim_header_snapshot_*.parquet",            
    },
    "claim_line":{
        "file_type":"parquet",
        "file_pattern":"claim_line_snapshot_*.parquet",    
    }

}