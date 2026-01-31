-- Staging model for physicians from OLTP source
-- Data extracted via Meltano tap-postgres (Singer format)

with source as (
    select * from {{ source('oltp', 'physicians') }}
),

staged as (
    select
        -- Primary key
        id::text as physician_id,

        -- Business keys
        npi,

        -- Attributes
        first_name,
        last_name,
        specialty,
        state_license_id::text as state_license_id,
        email,
        phone,
        is_active,

        -- Timestamps
        created_at,
        updated_at,

        -- Singer metadata (added by Meltano target-postgres)
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
