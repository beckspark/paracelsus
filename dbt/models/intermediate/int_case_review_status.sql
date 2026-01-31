-- Intermediate model: Case review status with enriched data
-- Joins cases with reviews and adds computed fields

with cases as (
    select * from {{ ref('stg_oltp__cases') }}
),

reviews as (
    select * from {{ ref('stg_oltp__case_reviews') }}
),

providers as (
    select * from {{ ref('stg_oltp__providers') }}
),

case_with_reviews as (
    select
        c.case_id,
        c.provider_id,
        c.patient_mrn,
        c.case_type,
        c.status as case_status,
        c.priority,
        c.created_at as case_created_at,
        c.closed_at as case_closed_at,

        r.review_id,
        r.physician_id,
        r.review_date,
        r.review_status,
        r.due_date,
        r.completed_at as review_completed_at,

        p.supervising_physician_id,
        p.provider_type,
        p.state_id,

        -- Computed fields
        case
            when r.review_status = 'completed' then r.completed_at::date - r.due_date
            when r.review_status = 'overdue' then current_date - r.due_date
            else null
        end as days_from_due_date,

        case
            when r.review_status = 'completed' then
                extract(day from (r.completed_at - c.created_at))
            else null
        end as days_to_complete_review,

        case
            when r.review_status = 'overdue' then true
            when r.due_date < current_date and r.review_status = 'pending' then true
            else false
        end as is_overdue

    from cases c
    left join reviews r on c.case_id = r.case_id
    left join providers p on c.provider_id = p.provider_id
)

select * from case_with_reviews
