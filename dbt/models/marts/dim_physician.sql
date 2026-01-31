-- Dimension: Physicians (supervising doctors)
-- SCD Type 1 for simplicity in POC (could be Type 2 for history tracking)

with physicians as (
    select * from {{ ref('stg_oltp__physicians') }}
),

states as (
    select * from {{ ref('stg_oltp__states') }}
),

final as (
    select
        -- Surrogate key
        row_number() over (order by p.physician_id) as physician_key,

        -- Natural key
        p.physician_id::uuid as physician_id,

        -- Attributes
        p.npi,
        p.first_name || ' ' || p.last_name as full_name,
        p.first_name,
        p.last_name,
        p.specialty,
        p.email,
        p.phone,
        p.is_active,

        -- State info (denormalized for easier querying)
        s.state_code,
        s.state_name,

        -- SCD metadata
        p.created_at::date as effective_from,
        null::date as effective_to,
        true as is_current,

        -- Audit
        p.updated_at as last_updated_at

    from physicians p
    left join states s on p.state_license_id = s.state_id
)

select * from final
