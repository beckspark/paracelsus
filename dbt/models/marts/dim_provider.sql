-- Dimension: Providers (NPs and PAs being supervised)
-- SCD Type 1 for simplicity in POC

with providers as (
    select * from {{ ref('stg_oltp__providers') }}
),

physicians as (
    select * from {{ ref('dim_physician') }}
),

states as (
    select * from {{ ref('stg_oltp__states') }}
),

final as (
    select
        -- Surrogate key
        row_number() over (order by p.provider_id) as provider_key,

        -- Natural key
        p.provider_id::uuid as provider_id,

        -- Attributes
        p.npi,
        p.first_name || ' ' || p.last_name as full_name,
        p.first_name,
        p.last_name,
        p.provider_type,
        p.email,
        p.phone,
        p.hire_date,
        p.is_active,

        -- Supervising physician (FK to dim_physician)
        ph.physician_key as supervising_physician_key,
        ph.full_name as supervising_physician_name,

        -- State info (denormalized)
        s.state_code,
        s.state_name,
        s.review_frequency_days,

        -- SCD metadata
        p.created_at::date as effective_from,
        null::date as effective_to,
        true as is_current,

        -- Audit
        p.updated_at as last_updated_at

    from providers p
    left join physicians ph on p.supervising_physician_id::uuid = ph.physician_id
    left join states s on p.state_id = s.state_id
)

select * from final
