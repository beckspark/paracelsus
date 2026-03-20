-- Staging model for HL7v2 ORU^R01 lab result messages
-- One row per OBX segment (individual test result)
-- Messages received via MLLP, parsed by mock-hl7-engine, extracted via tap-hl7-engine
with
    source as (select * from {{ source("hl7_engine", "hl7_lab_results") }}),

    staged as (
        select
            -- Message key (not unique per row — multiple OBX per message)
            message_id,

            -- Patient identifiers
            patient_mrn,
            patient_last_name,
            patient_first_name,

            -- Provider
            ordering_provider_npi,

            -- Order details
            order_number,

            -- Test result (one per OBX segment)
            test_code,
            test_name,
            result_value,
            result_units,
            reference_range,
            abnormal_flag,

            -- Timestamps
            {{ hl7_timestamp("observation_datetime") }} as observation_datetime,

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
