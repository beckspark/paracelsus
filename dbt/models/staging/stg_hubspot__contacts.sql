-- Staging model for HubSpot contacts from API
-- Data extracted via Meltano tap-hubspot (Singer format)

with source as (
    select * from {{ source('hubspot', 'contacts') }}
),

staged as (
    select
        -- Primary key (HubSpot uses 'vid' or 'id' depending on tap version)
        coalesce(id::text, vid::text) as contact_id,

        -- Attributes (tap-hubspot flattens properties)
        properties__firstname as first_name,
        properties__lastname as last_name,
        properties__email as email,
        properties__phone as phone,
        properties__company as company,
        properties__jobtitle as job_title,
        properties__lifecyclestage as lifecycle_stage,
        properties__hs_lead_status as lead_status,

        -- Timestamps
        properties__createdate::timestamp as created_at,
        properties__lastmodifieddate::timestamp as updated_at,

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
