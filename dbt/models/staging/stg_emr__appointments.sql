-- Staging model for FHIR R4 Appointment resources from EMR
-- Data extracted via Meltano tap-rest-api-msdk
-- Nested FHIR arrays stored as JSONB; extracted here with ->> and -> operators

with source as (
    select * from {{ source('emr', 'fhir_appointments') }}
),

staged as (
    select
        -- Primary key
        id as appointment_id,

        -- Status
        status as appointment_status,

        -- Service type (from JSONB serviceType array)
        "serviceType"::jsonb -> 0 -> 'coding' -> 0 ->> 'display' as appointment_type,

        -- Schedule
        "start"::timestamp as scheduled_start,
        "end"::timestamp as scheduled_end,
        "minutesDuration" as duration_minutes,
        created::timestamp as created_at,

        -- Patient reference (extract patient ID from "Patient/pat-001" reference)
        replace(participant::jsonb -> 0 -> 'actor' ->> 'reference', 'Patient/', '') as patient_id,

        -- Provider NPI (from practitioner participant identifier)
        participant::jsonb -> 1 -> 'actor' -> 'identifier' ->> 'value' as provider_npi,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
