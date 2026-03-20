-- Intermediate: Patient encounters from HL7 ADT messages
-- Joins admissions ↔ discharges on visit_number, then enriches via MRN and NPI
-- cross-source join to EMR encounters and patient dimension

with admissions as (
    select * from {{ ref('stg_hl7__admissions') }}
),

discharges as (
    select * from {{ ref('stg_hl7__discharges') }}
),

emr_patients as (
    select * from {{ ref('stg_emr__patients') }}
),

emr_encounters as (
    select * from {{ ref('stg_emr__encounters') }}
),

adt_combined as (
    select
        a.message_id as admit_message_id,
        a.patient_mrn,
        a.patient_last_name,
        a.patient_first_name,
        a.patient_dob,
        a.patient_gender,
        a.attending_provider_npi,
        a.patient_class,
        a.admit_datetime,
        a.visit_number,
        a.sending_facility,

        -- Discharge info (null if still admitted)
        d.message_id as discharge_message_id,
        d.discharge_datetime,

        -- Derived fields
        case
            when d.discharge_datetime is not null then 'discharged'
            else 'admitted'
        end as adt_status,

        case
            when d.discharge_datetime is not null
            then round(extract(epoch from (d.discharge_datetime - a.admit_datetime)) / 3600.0, 1)
        end as length_of_stay_hours

    from admissions a
    left join discharges d on a.visit_number = d.visit_number
),

-- Cross-source join: HL7 MRN → EMR patient_id
with_emr_patient as (
    select
        ac.*,
        ep.patient_id as emr_patient_id
    from adt_combined ac
    left join emr_patients ep on ac.patient_mrn = ep.mrn
),

-- Cross-source join: match to EMR encounters via patient + provider + date
with_emr_encounter as (
    select
        wp.*,
        ee.encounter_id as emr_encounter_id,
        ee.encounter_type,
        ee.chief_complaint,
        ee.primary_diagnosis_desc,
        ee.primary_diagnosis_code
    from with_emr_patient wp
    left join emr_encounters ee
        on wp.emr_patient_id = ee.patient_id
        and wp.attending_provider_npi = ee.provider_npi
        and wp.admit_datetime::date = ee.encounter_start::date
)

select * from with_emr_encounter
