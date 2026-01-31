-- Staging model for patient cases from OLTP source
-- Data extracted via Meltano tap-postgres (Singer format)

with source as (
    select * from {{ source('oltp', 'cases') }}
),

staged as (
    select
        -- Primary key
        id::text as case_id,

        -- Foreign keys
        provider_id::text as provider_id,

        -- Attributes
        patient_mrn,
        case_type,
        status,
        priority,

        -- Timestamps
        created_at,
        closed_at,
        updated_at,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
