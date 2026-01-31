-- Staging model for HubSpot contacts from API
-- Data extracted via Meltano tap-hubspot (meltanolabs v3 API)

with source as (
    select * from {{ source('hubspot', 'contacts') }}
),

staged as (
    select
        -- Primary key
        id::text as contact_id,

        -- Attributes (extracted from JSON properties column)
        properties->>'firstname' as first_name,
        properties->>'lastname' as last_name,
        properties->>'email' as email,
        properties->>'phone' as phone,
        properties->>'company' as company,
        properties->>'jobtitle' as job_title,
        properties->>'lifecyclestage' as lifecycle_stage,
        properties->>'hs_lead_status' as lead_status,

        -- Timestamps
        (properties->>'createdate')::timestamp as created_at,
        coalesce((properties->>'lastmodifieddate')::timestamp, lastmodifieddate) as updated_at,

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
