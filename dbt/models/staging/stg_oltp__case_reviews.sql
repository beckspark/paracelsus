-- Staging model for case reviews from OLTP source
-- Data extracted via Meltano tap-postgres (Singer format)

with source as (
    select * from {{ source('oltp', 'case_reviews') }}
),

staged as (
    select
        -- Primary key
        id::text as review_id,

        -- Foreign keys
        case_id::text as case_id,
        physician_id::text as physician_id,

        -- Attributes
        review_date,
        review_status,
        notes,
        due_date,
        completed_at,

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
