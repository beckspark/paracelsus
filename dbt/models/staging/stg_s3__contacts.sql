-- Staging model for HubSpot contacts CSV from S3
-- Data extracted via Meltano tap-s3-csv (Singer format)

with source as (
    select * from {{ source('s3_landing', 'contacts_csv') }}
),

staged as (
    select
        -- Primary key
        id as contact_id,

        -- Attributes
        firstname as first_name,
        lastname as last_name,
        email,
        phone,
        company,
        jobtitle as job_title,
        lifecyclestage as lifecycle_stage,
        hs_lead_status as lead_status,

        -- Timestamps (from CSV, may need casting)
        createdate::timestamp as created_at,
        lastmodifieddate::timestamp as last_modified_at,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version,

        -- Source identifier
        'csv' as source_type

    from source
)

select * from staged
