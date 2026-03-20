-- Staging model for HL7v2 ADT^A03 discharge messages
-- Data parsed from pipe-delimited HL7 to CSV, landed in S3, extracted via tap-s3-csv

with source as (
    select * from {{ source('s3_landing', 'hl7_discharges') }}
),

staged as (
    select
        -- Primary key
        message_id,

        -- Patient identifiers
        patient_mrn,
        patient_last_name,
        patient_first_name,

        -- Provider
        attending_provider_npi,

        -- Admission/discharge details
        patient_class,
        {{ hl7_timestamp('admit_datetime') }} as admit_datetime,
        {{ hl7_timestamp('discharge_datetime') }} as discharge_datetime,
        visit_number,

        -- Message metadata
        sending_facility,
        {{ hl7_timestamp('message_datetime') }} as message_datetime,

        -- Singer metadata
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,
        _sdc_table_version

    from source
)

select * from staged
