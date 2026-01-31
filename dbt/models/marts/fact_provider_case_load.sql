-- Fact: Provider Case Load (grain: physician + date)
-- Key business metrics for supervising physician workload

with daily_metrics as (
    select * from {{ ref('int_physician_daily_metrics') }}
),

physicians as (
    select * from {{ ref('dim_physician') }}
),

final as (
    select
        -- Dimension keys
        p.physician_key,
        dm.date_key,

        -- Metrics
        dm.cases_reviewed,
        dm.cases_pending_review,
        dm.cases_overdue,
        dm.total_providers_supervised,
        round(dm.avg_days_to_review::numeric, 2) as avg_days_to_review,

        -- Derived metrics
        dm.cases_reviewed + dm.cases_pending_review + dm.cases_overdue as total_cases,

        case
            when (dm.cases_reviewed + dm.cases_pending_review + dm.cases_overdue) > 0 then
                round(
                    (dm.cases_overdue::numeric / (dm.cases_reviewed + dm.cases_pending_review + dm.cases_overdue)) * 100,
                    2
                )
            else 0
        end as overdue_rate_pct,

        -- Workload indicators
        case
            when dm.cases_overdue > 5 then 'critical'
            when dm.cases_overdue > 2 then 'warning'
            when dm.cases_pending_review > 10 then 'high'
            else 'normal'
        end as workload_status

    from daily_metrics dm
    inner join physicians p on dm.physician_id::uuid = p.physician_id
)

select * from final
