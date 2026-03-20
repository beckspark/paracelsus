-- Dimension: Patients from EMR system
-- SCD Type 1 for simplicity in POC
with
    patients as (select * from {{ ref("stg_emr__patients") }}),

    encounter_stats as (
        select
            patient_id,
            count(*) as total_encounters,
            count(distinct provider_npi) as provider_count,
            min(encounter_start) as first_encounter_date,
            max(encounter_start) as last_encounter_date
        from {{ ref("stg_emr__encounters") }}
        group by patient_id
    ),

    final as (
        select
            -- Surrogate key
            row_number() over (order by p.patient_id) as patient_key,

            -- Natural key
            p.patient_id,

            -- Business key
            p.mrn,

            -- Demographics
            p.first_name || ' ' || p.last_name as full_name,
            p.first_name,
            p.last_name,
            p.date_of_birth,
            p.gender,
            p.insurance_type,

            -- Contact
            p.phone,
            p.email,

            -- Location
            p.city,
            p.state,
            p.postal_code,

            -- Encounter aggregates
            coalesce(es.total_encounters, 0) as total_encounters,
            coalesce(es.provider_count, 0) as provider_count,
            es.first_encounter_date,
            es.last_encounter_date

        from patients p
        left join encounter_stats es on p.patient_id = es.patient_id
    )

select *
from final
