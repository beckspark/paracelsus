-- Intermediate model: Daily metrics per physician
-- Aggregates case review data at the physician + date level

with case_reviews as (
    select * from {{ ref('int_case_review_status') }}
),

providers as (
    select * from {{ ref('stg_oltp__providers') }}
),

-- Get all physicians who supervise providers
physicians_with_providers as (
    select distinct
        supervising_physician_id as physician_id,
        count(distinct provider_id) as total_providers_supervised
    from providers
    where supervising_physician_id is not null
    group by supervising_physician_id
),

-- Generate date spine for the analysis period
date_spine as (
    select generate_series(
        '{{ var("start_date") }}'::date,
        current_date,
        '1 day'::interval
    )::date as date_key
),

-- Cross join physicians with dates to ensure we have a row for each combination
physician_dates as (
    select
        p.physician_id,
        p.total_providers_supervised,
        d.date_key
    from physicians_with_providers p
    cross join date_spine d
),

-- Aggregate reviews by physician and date
daily_review_metrics as (
    select
        physician_id,
        review_date as date_key,

        -- Completed reviews
        count(case when review_status = 'completed' then 1 end) as cases_reviewed,

        -- Pending reviews (due on or after this date)
        count(case when review_status = 'pending' and due_date >= review_date then 1 end) as cases_pending_review,

        -- Overdue reviews
        count(case when is_overdue then 1 end) as cases_overdue,

        -- Average days to review (for completed reviews)
        avg(case when review_status = 'completed' then days_to_complete_review end) as avg_days_to_review

    from case_reviews
    where review_date is not null
    group by physician_id, review_date
),

-- Join everything together
final as (
    select
        pd.physician_id,
        pd.date_key,
        pd.total_providers_supervised,
        coalesce(drm.cases_reviewed, 0) as cases_reviewed,
        coalesce(drm.cases_pending_review, 0) as cases_pending_review,
        coalesce(drm.cases_overdue, 0) as cases_overdue,
        drm.avg_days_to_review
    from physician_dates pd
    left join daily_review_metrics drm
        on pd.physician_id = drm.physician_id
        and pd.date_key = drm.date_key
)

select * from final
