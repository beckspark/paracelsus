-- Intermediate model: Appointment utilization metrics
-- Links appointments to encounters and providers via NPI
with
    appointments as (select * from {{ ref("stg_emr__appointments") }}),

    encounters as (select * from {{ ref("stg_emr__encounters") }}),

    providers as (select * from {{ ref("stg_oltp__providers") }}),

    appointment_with_context as (
        select
            a.appointment_id,
            a.appointment_status,
            a.appointment_type,
            a.scheduled_start,
            a.scheduled_end,
            a.duration_minutes,
            a.created_at,
            a.patient_id,
            a.provider_npi,

            -- Provider linkage via NPI
            p.provider_id,
            p.supervising_physician_id,

            -- Status flags
            a.appointment_status = 'fulfilled' as is_completed,
            a.appointment_status = 'noshow' as is_no_show,
            a.appointment_status = 'cancelled' as is_cancelled,

            -- Check if a matching encounter exists (same patient + provider + same day)
            case
                when e.encounter_id is not null then true else false
            end as has_encounter,

            e.encounter_id,
            e.encounter_class,
            e.primary_diagnosis_code,
            e.primary_diagnosis_desc

        from appointments a
        left join providers p on a.provider_npi = p.npi
        left join
            encounters e
            on a.patient_id = e.patient_id
            and a.provider_npi = e.provider_npi
            and a.scheduled_start::date = e.encounter_start::date
    )

select *
from appointment_with_context
