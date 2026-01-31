-- Dimension: States (reference data for supervision requirements)

with states as (
    select * from {{ ref('stg_oltp__states') }}
),

final as (
    select
        -- Surrogate key
        row_number() over (order by state_code) as state_key,

        -- Attributes
        state_code,
        state_name,
        supervision_requirements,
        review_frequency_days,

        -- Audit
        created_at as last_updated_at

    from states
)

select * from final
