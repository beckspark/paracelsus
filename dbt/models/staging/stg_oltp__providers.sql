-- Staging model for providers (NPs/PAs) from OLTP source
-- Data extracted via Meltano tap-postgres (Singer format)

with source as (
    select * from {{ source('oltp', 'providers') }}
),

staged as (
    select
        -- Primary key
        id::text as provider_id,

        -- Business keys
        npi,

        -- Attributes
        first_name,
        last_name,
        provider_type,
        supervising_physician_id::text as supervising_physician_id,
        state_id::text as state_id,
        email,
        phone,
        hire_date,
        is_active,

        -- Timestamps
        created_at,
        updated_at,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
