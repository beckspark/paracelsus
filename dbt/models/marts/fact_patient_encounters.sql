-- Fact: Patient encounters from HL7 ADT messages (grain: one row per admission/visit_number)
-- Links to dim_patient via MRN, dim_provider via NPI

with encounters as (
    select * from {{ ref('int_patient_encounters') }}
),

dim_patients as (
    select * from {{ ref('dim_patient') }}
),

dim_providers as (
    select * from {{ ref('dim_provider') }}
),

dim_physicians as (
    select * from {{ ref('dim_physician') }}
),

final as (
    select
        -- Dimension keys
        dp.patient_key,
        dprov.provider_key,
        dph.physician_key as supervising_physician_key,

        -- Natural keys for traceability
        e.visit_number,
        e.patient_mrn,
        e.attending_provider_npi,

        -- Timing
        e.admit_datetime,
        e.discharge_datetime,
        e.admit_datetime::date as admit_date_key,

        -- Classification
        e.patient_class,
        e.adt_status,
        e.encounter_type,

        -- Clinical (from EMR cross-source join)
        e.chief_complaint,
        e.primary_diagnosis_desc,
        e.primary_diagnosis_code,

        -- Metrics
        e.length_of_stay_hours,

        -- Flags
        e.emr_encounter_id is not null as has_emr_encounter,
        e.discharge_datetime is not null as is_discharged

    from encounters e
    left join dim_patients dp on e.patient_mrn = dp.mrn
    left join dim_providers dprov on e.attending_provider_npi = dprov.npi
    left join dim_physicians dph on dprov.supervising_physician_key = dph.physician_key
)

select * from final
