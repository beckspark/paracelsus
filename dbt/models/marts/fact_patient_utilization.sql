-- Fact: Patient utilization metrics (grain: provider_key x date_key)
-- Key business metrics for provider appointment and encounter utilization
with
    appointment_util as (select * from {{ ref("int_appointment_utilization") }}),

    dim_providers as (select * from {{ ref("dim_provider") }}),

    dim_physicians as (select * from {{ ref("dim_physician") }}),

    daily_agg as (
        select
            au.provider_npi,
            au.scheduled_start::date as activity_date,

            -- Appointment counts
            count(*) as total_appointments,
            count(*) filter (where au.is_completed) as completed_appointments,
            count(*) filter (where au.is_no_show) as no_show_appointments,
            count(*) filter (where au.is_cancelled) as cancelled_appointments,

            -- Patient counts
            count(distinct au.patient_id) as unique_patients,

            -- Duration
            round(avg(au.duration_minutes)::numeric, 1) as avg_duration_minutes,
            sum(au.duration_minutes) as total_scheduled_minutes,

            -- Encounter counts
            count(*) filter (where au.has_encounter) as appointments_with_encounter,
            count(distinct au.primary_diagnosis_code) filter (
                where au.has_encounter
            ) as unique_diagnoses

        from appointment_util au
        where au.provider_npi is not null
        group by au.provider_npi, au.scheduled_start::date
    ),

    final as (
        select
            -- Dimension keys
            dp.provider_key,
            da.activity_date as date_key,

            -- Supervising physician
            dp.supervising_physician_key,

            -- Appointment metrics
            da.total_appointments,
            da.completed_appointments,
            da.no_show_appointments,
            da.cancelled_appointments,
            da.unique_patients,

            -- Duration metrics
            da.avg_duration_minutes,
            da.total_scheduled_minutes,

            -- Encounter metrics
            da.appointments_with_encounter,
            da.unique_diagnoses,

            -- Rates
            case
                when da.total_appointments > 0
                then
                    round(
                        da.completed_appointments::numeric
                        / da.total_appointments
                        * 100,
                        2
                    )
                else 0
            end as completion_rate_pct,

            case
                when da.total_appointments > 0
                then
                    round(
                        da.no_show_appointments::numeric / da.total_appointments * 100,
                        2
                    )
                else 0
            end as no_show_rate_pct,

            case
                when da.total_appointments > 0
                then
                    round(
                        da.cancelled_appointments::numeric
                        / da.total_appointments
                        * 100,
                        2
                    )
                else 0
            end as cancellation_rate_pct,

            -- Utilization status
            case
                when
                    da.total_appointments > 0
                    and round(
                        da.no_show_appointments::numeric / da.total_appointments * 100,
                        2
                    )
                    > 25
                then 'high_no_show'
                when
                    da.total_appointments > 0
                    and round(
                        da.completed_appointments::numeric
                        / da.total_appointments
                        * 100,
                        2
                    )
                    >= 75
                then 'well_utilized'
                when
                    da.total_appointments > 0
                    and round(
                        da.completed_appointments::numeric
                        / da.total_appointments
                        * 100,
                        2
                    )
                    >= 50
                then 'moderate'
                else 'underutilized'
            end as utilization_status

        from daily_agg da
        inner join dim_providers dp on da.provider_npi = dp.npi
    )

select *
from final
