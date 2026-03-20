-- Staging model for HL7v2 ADT^A01 admission messages
-- Messages received via MLLP, parsed by mock-hl7-engine, extracted via tap-hl7-engine
with
    source as (select * from {{ source("hl7_engine", "hl7_admissions") }}),

    staged as (
        select
            -- Primary key
            message_id,

            -- Patient identifiers
            patient_mrn,
            patient_last_name,
            patient_first_name,
            patient_dob,
            patient_gender,

            -- Provider
            attending_provider_npi,

            -- Admission details
            patient_class,
            {{ hl7_timestamp("admit_datetime") }} as admit_datetime,
            visit_number,

            -- Message metadata
            sending_facility,
            {{ hl7_timestamp("message_datetime") }} as message_datetime,

            -- Singer metadata
            _sdc_extracted_at,
            _sdc_received_at,
            _sdc_batched_at,
            _sdc_table_version

        from source
    )

select *
from staged
