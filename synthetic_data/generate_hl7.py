"""Generate mock HL7v2 messages (ADT^A01, ADT^A03, ORU^R01) and parse to CSV.

HL7v2 is a pipe-delimited message protocol transmitted over MLLP/TCP.
Real pipelines use an integration engine (Mirth Connect, Apache Camel) to parse
segments into flat files and drop them in S3. We mock this pattern by generating
actual HL7v2 message strings, parsing them back into dicts, and writing CSVs.

NPI + MRN COORDINATION:
    Replicates seed sequences from synthetic_data/generate.py (provider NPIs) and
    mock_emr/generate_fixtures.py (patient MRNs). Same pattern as _replicate_oltp_seed_sequence().
"""

import csv
import io
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

# Must match synthetic_data/generate.py
SPECIALTIES = [
    "Internal Medicine",
    "Family Medicine",
    "Emergency Medicine",
    "Cardiology",
    "Pulmonology",
    "Gastroenterology",
    "Neurology",
    "Orthopedics",
    "Psychiatry",
    "Oncology",
]

INSURANCE_TYPES = ["medicare", "medicaid", "commercial", "self-pay", "tricare"]

# Lab panels with realistic reference ranges
LAB_PANELS = {
    "CBC": [
        ("WBC", "6032-9", "x10E3/uL", "4.5-11.0"),
        ("RBC", "789-8", "x10E6/uL", "4.5-5.5"),
        ("Hemoglobin", "718-7", "g/dL", "12.0-17.5"),
        ("Hematocrit", "4544-3", "%", "36.0-50.0"),
    ],
    "BMP": [
        ("Glucose", "2345-7", "mg/dL", "70-100"),
        ("BUN", "3094-0", "mg/dL", "7-20"),
        ("Creatinine", "2160-0", "mg/dL", "0.6-1.2"),
        ("Sodium", "2951-2", "mEq/L", "136-145"),
        ("Potassium", "2823-3", "mEq/L", "3.5-5.0"),
    ],
    "Lipid": [
        ("Cholesterol", "2093-3", "mg/dL", "125-200"),
        ("LDL", "2089-1", "mg/dL", "0-130"),
        ("HDL", "2085-9", "mg/dL", "40-60"),
        ("Triglycerides", "2571-8", "mg/dL", "0-150"),
    ],
}


def _generate_npi() -> str:
    """Generate a valid-looking NPI. Must match synthetic_data/generate.py."""
    return str(random.choice([1, 2])) + "".join(str(random.randint(0, 9)) for _ in range(9))


def _replicate_provider_npis() -> list[str]:
    """Replicate the OLTP seed sequence to capture 30 provider NPIs.

    Identical to mock_emr/generate_fixtures.py::_replicate_oltp_seed_sequence().
    """
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # 1. generate_states() burns 10 uuid4() calls
    for _ in range(10):
        uuid.uuid4()

    # 2. generate_physicians(count=10)
    for _ in range(10):
        random.choice(range(10))
        fake.first_name()
        fake.last_name()
        _generate_npi()
        random.choice(SPECIALTIES)
        uuid.uuid4()
        fake.phone_number()

    # 3. generate_providers(count=30) — capture NPIs
    provider_npis = []
    for _ in range(30):
        random.choice(range(10))
        random.choice(range(10))
        fake.first_name()
        fake.last_name()
        npi = _generate_npi()
        provider_npis.append(npi)
        random.choice(["NP", "PA"])
        uuid.uuid4()
        fake.phone_number()
        fake.date_between(start_date="-5y", end_date="-30d")

    return provider_npis


def _replicate_patient_mrns() -> list[dict]:
    """Replicate mock_emr patient seed sequence to capture MRNs + demographics.

    Re-seeds with 1042 (same as generate_fixtures.py) and replays generate_patients().
    """
    fake = Faker()
    Faker.seed(1042)
    random.seed(1042)

    patients = []
    for _ in range(200):
        first_name = fake.first_name()
        last_name = fake.last_name()
        gender = random.choice(["male", "female"])
        dob = fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat()
        mrn = f"MRN{random.randint(100000, 999999)}"
        random.choice(INSURANCE_TYPES)  # insurance — burn the call

        patients.append(
            {
                "mrn": mrn,
                "first_name": first_name,
                "last_name": last_name,
                "gender": gender,
                "dob": dob,
            }
        )

        # Burn remaining Faker calls from generate_patients loop:
        # phone_number, domain_name (for email), street_address, city, state_abbr, zipcode
        fake.phone_number()
        fake.domain_name()
        fake.street_address()
        fake.city()
        fake.state_abbr()
        fake.zipcode()

    return patients


# ---------------------------------------------------------------------------
# HL7v2 segment builders
# ---------------------------------------------------------------------------


def _hl7_timestamp(dt: datetime) -> str:
    """Format datetime as HL7v2 timestamp (YYYYMMDDHHmmss)."""
    return dt.strftime("%Y%m%d%H%M%S")


def _build_msh(sending_app: str, msg_type: str, msg_id: str, msg_dt: datetime) -> str:
    return (
        f"MSH|^~\\&|{sending_app}|HOSPITAL|WAREHOUSE|ANALYTICS|" f"{_hl7_timestamp(msg_dt)}||{msg_type}|{msg_id}|P|2.3"
    )


def _build_evn(event_code: str, event_dt: datetime) -> str:
    return f"EVN|{event_code}|{_hl7_timestamp(event_dt)}"


def _build_pid(patient: dict) -> str:
    gender_code = "M" if patient["gender"] == "male" else "F"
    dob = patient["dob"].replace("-", "")
    return (
        f"PID|1||{patient['mrn']}||"
        f"{patient['last_name']}^{patient['first_name']}||"
        f"{dob}|{gender_code}|||"
        f"123 Main St^^Springfield^IL^62701"
    )


def _build_pv1(
    patient_class: str,
    provider_npi: str,
    provider_last: str,
    provider_first: str,
    visit_number: str,
    admit_dt: datetime,
    discharge_dt: datetime | None = None,
) -> str:
    # Standard HL7v2 PV1 field positions (0-based, field[0]="PV1"):
    #   [2]=patient_class, [7]=attending, [19]=visit_number, [44]=admit_dt, [45]=discharge_dt
    fields = [""] * 46
    fields[0] = "PV1"
    fields[1] = "1"
    fields[2] = patient_class
    fields[7] = f"{provider_npi}^{provider_last}^{provider_first}"
    fields[19] = visit_number
    fields[44] = _hl7_timestamp(admit_dt)
    fields[45] = _hl7_timestamp(discharge_dt) if discharge_dt else ""
    return "|".join(fields)


def _build_obr(order_number: str, panel_name: str, obs_dt: datetime, provider_npi: str) -> str:
    # OBR: field[16] = ordering provider. Fields 0-15 padded with pipes.
    fields = ["OBR", "1", order_number, "", panel_name, "", "", _hl7_timestamp(obs_dt)]
    # Pad fields 8-15 (empty), then field 16 = provider NPI
    fields.extend([""] * 8)
    fields.append(provider_npi)
    return "|".join(fields)


def _build_obx(
    set_id: int,
    test_code: str,
    test_name: str,
    value: str,
    units: str,
    ref_range: str,
    abnormal_flag: str,
) -> str:
    return f"OBX|{set_id}|NM|{test_code}^{test_name}||{value}|{units}|{ref_range}|{abnormal_flag}|||F"


# ---------------------------------------------------------------------------
# HL7v2 message parser (pipe-delimited → dict)
# ---------------------------------------------------------------------------


def _parse_hl7_segments(raw_message: str) -> dict[str, list[str]]:
    """Parse raw HL7v2 message into segment name → list of fields."""
    segments = {}
    for line in raw_message.strip().split("\n"):
        fields = line.split("|")
        seg_name = fields[0]
        if seg_name not in segments:
            segments[seg_name] = []
        segments[seg_name].append(fields)
    return segments


def _parse_admission(raw: str) -> dict:
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    pv1 = segs["PV1"][0]

    # PID: [3]=MRN, [5]=name(last^first), [7]=DOB, [8]=gender
    name_parts = pid[5].split("^")
    # PV1 (standard HL7): [2]=patient_class, [7]=attending, [19]=visit_number, [44]=admit_dt
    attending = pv1[7].split("^") if len(pv1) > 7 else []

    return {
        "message_id": msh[9],
        "patient_mrn": pid[3],
        "patient_last_name": name_parts[0] if name_parts else "",
        "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
        "patient_dob": pid[7],
        "patient_gender": pid[8],
        "attending_provider_npi": attending[0] if attending else "",
        "patient_class": pv1[2],
        "admit_datetime": pv1[44] if len(pv1) > 44 else "",
        "visit_number": pv1[19] if len(pv1) > 19 else "",
        "sending_facility": msh[3],
        "message_datetime": msh[6],
    }


def _parse_discharge(raw: str) -> dict:
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    pv1 = segs["PV1"][0]

    name_parts = pid[5].split("^")
    # PV1 (standard HL7): [7]=attending, [19]=visit_number, [44]=admit_dt, [45]=discharge_dt
    attending = pv1[7].split("^") if len(pv1) > 7 else []

    return {
        "message_id": msh[9],
        "patient_mrn": pid[3],
        "patient_last_name": name_parts[0] if name_parts else "",
        "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
        "attending_provider_npi": attending[0] if attending else "",
        "patient_class": pv1[2],
        "admit_datetime": pv1[44] if len(pv1) > 44 else "",
        "discharge_datetime": pv1[45] if len(pv1) > 45 else "",
        "visit_number": pv1[19] if len(pv1) > 19 else "",
        "sending_facility": msh[3],
        "message_datetime": msh[6],
    }


def _parse_lab_result(raw: str) -> list[dict]:
    """Parse ORU^R01 into one dict per OBX segment."""
    segs = _parse_hl7_segments(raw)
    msh = segs["MSH"][0]
    pid = segs["PID"][0]
    obr = segs["OBR"][0]

    name_parts = pid[5].split("^")
    rows = []
    for obx in segs.get("OBX", []):
        test_parts = obx[3].split("^")
        rows.append(
            {
                "message_id": msh[9],
                "patient_mrn": pid[3],
                "patient_last_name": name_parts[0] if name_parts else "",
                "patient_first_name": name_parts[1] if len(name_parts) > 1 else "",
                "ordering_provider_npi": obr[16],
                "order_number": obr[2],
                "test_code": test_parts[0] if test_parts else "",
                "test_name": test_parts[1] if len(test_parts) > 1 else "",
                "result_value": obx[5],
                "result_units": obx[6],
                "reference_range": obx[7],
                "abnormal_flag": obx[8],
                "observation_datetime": msh[6],
                "sending_facility": msh[3],
                "message_datetime": msh[6],
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Message generators
# ---------------------------------------------------------------------------


def _generate_lab_value(ref_range: str) -> tuple[str, str]:
    """Generate a lab result value and abnormal flag based on reference range."""
    low, high = ref_range.split("-")
    low_f, high_f = float(low), float(high)
    normal_range = high_f - low_f

    roll = random.random()
    if roll < 0.7:
        # Normal
        value = round(random.uniform(low_f, high_f), 1)
        flag = "N"
    elif roll < 0.85:
        # High
        value = round(random.uniform(high_f, high_f + normal_range * 0.3), 1)
        flag = "H" if value < high_f + normal_range * 0.5 else "HH"
    else:
        # Low
        value = round(random.uniform(max(0, low_f - normal_range * 0.3), low_f), 1)
        flag = "L" if value > low_f - normal_range * 0.2 else "LL"

    return str(value), flag


def generate_hl7_messages(
    provider_npis: list[str],
    patients: list[dict],
) -> tuple[list[str], list[str], list[str]]:
    """Generate raw HL7v2 message strings.

    Returns (adt_a01_messages, adt_a03_messages, oru_r01_messages).
    """
    # Use a separate seed so we don't collide with OLTP/EMR sequences
    fake = Faker()
    Faker.seed(2042)
    random.seed(2042)

    adt_a01_messages = []
    adt_a03_messages = []
    oru_r01_messages = []

    # Track admissions for discharge generation
    admissions = []

    # --- ADT^A01 (Admit) — 80 messages ---
    for i in range(80):
        msg_id = f"ADT{i + 1:05d}"
        patient = random.choice(patients)
        npi = random.choice(provider_npis)
        patient_class = random.choices(["I", "E", "O"], weights=[0.5, 0.3, 0.2])[0]
        visit_number = f"VN{random.randint(10000, 99999)}"

        days_ago = random.randint(1, 60)
        hour = random.randint(0, 23)
        admit_dt = datetime.now().replace(hour=hour, minute=random.randint(0, 59), second=0, microsecond=0) - timedelta(
            days=days_ago
        )
        msg_dt = admit_dt

        provider_first = fake.first_name()
        provider_last = fake.last_name()

        raw = "\n".join(
            [
                _build_msh("ADMITSYS", "ADT^A01", msg_id, msg_dt),
                _build_evn("A01", admit_dt),
                _build_pid(patient),
                _build_pv1(patient_class, npi, provider_last, provider_first, visit_number, admit_dt),
            ]
        )
        adt_a01_messages.append(raw)
        admissions.append(
            {
                "patient": patient,
                "npi": npi,
                "patient_class": patient_class,
                "visit_number": visit_number,
                "admit_dt": admit_dt,
                "provider_first": provider_first,
                "provider_last": provider_last,
            }
        )

    # --- ADT^A03 (Discharge) — 60 messages (subset of admissions) ---
    discharged = random.sample(admissions, min(60, len(admissions)))
    for i, adm in enumerate(discharged):
        msg_id = f"DIS{i + 1:05d}"
        los_hours = random.randint(2, 168)  # 2 hours to 7 days
        discharge_dt = adm["admit_dt"] + timedelta(hours=los_hours)
        msg_dt = discharge_dt

        raw = "\n".join(
            [
                _build_msh("ADMITSYS", "ADT^A03", msg_id, msg_dt),
                _build_evn("A03", discharge_dt),
                _build_pid(adm["patient"]),
                _build_pv1(
                    adm["patient_class"],
                    adm["npi"],
                    adm["provider_last"],
                    adm["provider_first"],
                    adm["visit_number"],
                    adm["admit_dt"],
                    discharge_dt,
                ),
            ]
        )
        adt_a03_messages.append(raw)

    # --- ORU^R01 (Lab Results) — 120 messages ---
    panel_names = list(LAB_PANELS.keys())
    for i in range(120):
        msg_id = f"LAB{i + 1:05d}"
        patient = random.choice(patients)
        npi = random.choice(provider_npis)
        panel_name = random.choice(panel_names)
        order_number = f"ORD{random.randint(100000, 999999)}"

        days_ago = random.randint(0, 60)
        obs_dt = datetime.now().replace(
            hour=random.randint(6, 20), minute=random.randint(0, 59), second=0, microsecond=0
        ) - timedelta(days=days_ago)
        msg_dt = obs_dt

        segments = [
            _build_msh("LABSYS", "ORU^R01", msg_id, msg_dt),
            _build_pid(patient),
            _build_pv1("I", npi, fake.last_name(), fake.first_name(), f"VN{random.randint(10000, 99999)}", obs_dt),
            _build_obr(order_number, panel_name, obs_dt, npi),
        ]

        for set_id, (test_name, test_code, units, ref_range) in enumerate(LAB_PANELS[panel_name], start=1):
            value, flag = _generate_lab_value(ref_range)
            segments.append(_build_obx(set_id, test_code, test_name, value, units, ref_range, flag))

        oru_r01_messages.append("\n".join(segments))

    return adt_a01_messages, adt_a03_messages, oru_r01_messages


def messages_to_csv(
    adt_a01: list[str],
    adt_a03: list[str],
    oru_r01: list[str],
) -> tuple[str, str, str]:
    """Parse raw HL7v2 messages into CSV strings.

    Returns (admissions_csv, discharges_csv, lab_results_csv).
    """
    # Parse admissions
    admission_rows = [_parse_admission(msg) for msg in adt_a01]

    # Parse discharges
    discharge_rows = [_parse_discharge(msg) for msg in adt_a03]

    # Parse lab results (one row per OBX)
    lab_rows = []
    for msg in oru_r01:
        lab_rows.extend(_parse_lab_result(msg))

    def _to_csv(rows: list[dict]) -> str:
        if not rows:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()

    return _to_csv(admission_rows), _to_csv(discharge_rows), _to_csv(lab_rows)


def generate_hl7_csvs() -> tuple[str, str, str]:
    """Main entry point: generate HL7v2 messages and return CSV strings.

    Returns (admissions_csv, discharges_csv, lab_results_csv).
    """
    provider_npis = _replicate_provider_npis()
    patients = _replicate_patient_mrns()

    print(f"HL7: Captured {len(provider_npis)} provider NPIs and {len(patients)} patient MRNs")

    adt_a01, adt_a03, oru_r01 = generate_hl7_messages(provider_npis, patients)

    # Log sample messages for visibility
    print(f"HL7: Generated {len(adt_a01)} ADT^A01, {len(adt_a03)} ADT^A03, {len(oru_r01)} ORU^R01 messages")
    if adt_a01:
        print("--- Sample ADT^A01 ---")
        print(adt_a01[0])
        print("---")
    if oru_r01:
        print("--- Sample ORU^R01 ---")
        print(oru_r01[0])
        print("---")

    admissions_csv, discharges_csv, lab_results_csv = messages_to_csv(adt_a01, adt_a03, oru_r01)

    admission_count = len(adt_a01)
    discharge_count = len(adt_a03)
    lab_row_count = lab_results_csv.count("\n") - 1 if lab_results_csv else 0
    print(
        f"HL7: Parsed to CSV — {admission_count} admissions, {discharge_count} discharges, {lab_row_count} lab results"
    )

    return admissions_csv, discharges_csv, lab_results_csv


if __name__ == "__main__":
    admissions, discharges, labs = generate_hl7_csvs()
    print(f"\nAdmissions CSV rows: {admissions.count(chr(10)) - 1}")
    print(f"Discharges CSV rows: {discharges.count(chr(10)) - 1}")
    print(f"Lab results CSV rows: {labs.count(chr(10)) - 1}")
