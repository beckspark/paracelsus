-- Staging model for FHIR R4 Patient resources from EMR
-- Data extracted via Meltano tap-rest-api-msdk
-- Nested FHIR arrays stored as JSONB; extracted here with ->> and -> operators
with
    source as (select * from {{ source("emr", "fhir_patients") }}),

    staged as (
        select
            -- Primary key
            id as patient_id,

            -- Business key (MRN from identifier array)
            identifier::jsonb -> 0 ->> 'value' as mrn,

            -- Demographics
            name::jsonb -> 0 -> 'given' ->> 0 as first_name,
            name::jsonb -> 0 ->> 'family' as last_name,
            "birthDate" as date_of_birth,
            gender,

            -- Contact info (telecom array: index 0 = phone, index 1 = email)
            telecom::jsonb -> 0 ->> 'value' as phone,
            telecom::jsonb -> 1 ->> 'value' as email,

            -- Address
            address::jsonb -> 0 -> 'line' ->> 0 as address_line,
            address::jsonb -> 0 ->> 'city' as city,
            address::jsonb -> 0 ->> 'state' as state,
            address::jsonb -> 0 ->> 'postalCode' as postal_code,

            -- Custom extension: insurance type
            extension::jsonb -> 0 ->> 'valueString' as insurance_type,

            -- Singer metadata
            _sdc_extracted_at,
            _sdc_received_at,
            _sdc_batched_at,
            _sdc_table_version

        from source
    )

select *
from staged
