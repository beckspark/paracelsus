-- Staging model for FHIR R4 Encounter resources from EMR
-- Data extracted via Meltano tap-rest-api-msdk
-- Some fields auto-flattened (class_code, period_start, subject_reference),
-- others stored as JSONB (participant, reasonCode, diagnosis, type)

with source as (
    select * from {{ source('emr', 'fhir_encounters') }}
),

staged as (
    select
        -- Primary key
        id as encounter_id,

        -- Status and classification
        status as encounter_status,
        class_code as encounter_class,

        -- Type (from JSONB type array)
        type::jsonb -> 0 -> 'coding' -> 0 ->> 'display' as encounter_type,

        -- Patient reference (already flattened by tap)
        replace(subject_reference, 'Patient/', '') as patient_id,

        -- Provider NPI (from JSONB participant array)
        participant::jsonb -> 0 -> 'individual' -> 'identifier' ->> 'value' as provider_npi,

        -- Period (already flattened by tap)
        period_start::timestamp as encounter_start,
        period_end::timestamp as encounter_end,

        -- Reason and diagnosis (from JSONB)
        "reasonCode"::jsonb -> 0 ->> 'text' as chief_complaint,
        diagnosis::jsonb -> 0 -> 'condition' ->> 'display' as primary_diagnosis_desc,
        diagnosis::jsonb -> 0 -> 'condition' -> 'extension' -> 0 ->> 'valueString' as primary_diagnosis_code,

        -- Length (already flattened by tap)
        length_value as length_minutes,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
