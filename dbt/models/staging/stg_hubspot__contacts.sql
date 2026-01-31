-- Staging model for HubSpot contacts from API
-- Data extracted via Meltano tap-hubspot (meltanolabs v3 API)

with source as (
    select * from {{ source('hubspot', 'contacts') }}
),

staged as (
    select
        -- Primary key
        id::text as contact_id,

        -- Attributes (meltanolabs tap uses property_ prefix)
        property_firstname as first_name,
        property_lastname as last_name,
        property_email as email,
        property_phone as phone,
        property_company as company,
        property_jobtitle as job_title,
        property_lifecyclestage as lifecycle_stage,
        property_hs_lead_status as lead_status,

        -- Timestamps
        property_createdate as created_at,
        coalesce(property_lastmodifieddate, lastmodifieddate) as updated_at,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version,

        -- Source identifier
        'api' as source_type

    from source
)

select * from staged
