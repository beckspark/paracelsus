-- Staging model for states reference from OLTP source
-- Data extracted via Meltano tap-postgres (Singer format)

with source as (
    select * from {{ source('oltp', 'states') }}
),

staged as (
    select
        -- Primary key
        id::text as state_id,

        -- Attributes
        code as state_code,
        name as state_name,
        supervision_requirements,
        review_frequency_days,

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
