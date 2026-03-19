-- Intermediate model: Provider panel metrics
-- Aggregates encounters per provider per patient for panel size calculations

with encounters as (
    select * from {{ ref('stg_emr__encounters') }}
),

providers as (
    select * from {{ ref('stg_oltp__providers') }}
),

patient_provider_agg as (
    select
        e.provider_npi,
        p.provider_id,
        p.supervising_physician_id,
        e.patient_id,

        count(*) as encounter_count,
        min(e.encounter_start) as first_encounter_date,
        max(e.encounter_start) as last_encounter_date,
        count(distinct e.encounter_class) as distinct_encounter_classes,
        count(distinct e.primary_diagnosis_code) as distinct_diagnoses

    from encounters e
    left join providers p on e.provider_npi = p.npi
    group by e.provider_npi, p.provider_id, p.supervising_physician_id, e.patient_id
)

select * from patient_provider_agg
