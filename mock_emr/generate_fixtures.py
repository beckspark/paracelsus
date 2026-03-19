"""Generate FHIR R4-compliant fixture data for mock EMR.

NPI COORDINATION:
    This file replicates the exact seed (42) and random/Faker call sequence from
    synthetic_data/generate.py to produce matching provider NPIs. If the call
    sequence in generate.py changes, this file MUST be updated to match.

    Sequence replicated:
    1. Faker.seed(42), random.seed(42)
    2. generate_states() -> 10 states (burns 10 uuid4 calls)
    3. generate_physicians(count=10) -> 10 physicians (burns random + Faker calls; captures physician NPIs)
    4. generate_providers(count=30) -> 30 providers (captures provider NPIs we reuse here)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# FHIR value sets
APPOINTMENT_STATUSES = ["booked", "fulfilled", "cancelled", "noshow"]
APPOINTMENT_STATUS_WEIGHTS = [0.3, 0.4, 0.15, 0.15]

ENCOUNTER_STATUSES = ["finished", "in-progress", "planned"]
ENCOUNTER_STATUS_WEIGHTS = [0.6, 0.2, 0.2]

ENCOUNTER_CLASSES = [
    {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB", "display": "ambulatory"},
    {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "EMER", "display": "emergency"},
    {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "IMP", "display": "inpatient encounter"},
]
ENCOUNTER_CLASS_WEIGHTS = [0.6, 0.2, 0.2]

SERVICE_TYPES = [
    "General Practice",
    "Cardiology",
    "Pulmonology",
    "Neurology",
    "Orthopedics",
    "Mental Health",
    "Preventive Care",
    "Urgent Care",
]

ENCOUNTER_TYPES = [
    {"system": "http://snomed.info/sct", "code": "185349003", "display": "Encounter for check up"},
    {"system": "http://snomed.info/sct", "code": "390906007", "display": "Follow-up encounter"},
    {"system": "http://snomed.info/sct", "code": "50849002", "display": "Emergency room admission"},
    {"system": "http://snomed.info/sct", "code": "270427003", "display": "Patient-initiated encounter"},
]

REASON_CODES = [
    {"system": "http://snomed.info/sct", "code": "386661006", "display": "Fever"},
    {"system": "http://snomed.info/sct", "code": "49727002", "display": "Cough"},
    {"system": "http://snomed.info/sct", "code": "25064002", "display": "Headache"},
    {"system": "http://snomed.info/sct", "code": "267036007", "display": "Dyspnea"},
    {"system": "http://snomed.info/sct", "code": "29857009", "display": "Chest pain"},
    {"system": "http://snomed.info/sct", "code": "73211009", "display": "Diabetes mellitus"},
    {"system": "http://snomed.info/sct", "code": "38341003", "display": "Hypertension"},
    {"system": "http://snomed.info/sct", "code": "195967001", "display": "Asthma"},
]

DIAGNOSIS_CODES = [
    {"code": "J06.9", "display": "Acute upper respiratory infection, unspecified"},
    {"code": "I10", "display": "Essential hypertension"},
    {"code": "E11.9", "display": "Type 2 diabetes mellitus without complications"},
    {"code": "J45.909", "display": "Unspecified asthma, uncomplicated"},
    {"code": "M54.5", "display": "Low back pain"},
    {"code": "F41.1", "display": "Generalized anxiety disorder"},
    {"code": "R10.9", "display": "Unspecified abdominal pain"},
    {"code": "J20.9", "display": "Acute bronchitis, unspecified"},
    {"code": "K21.0", "display": "Gastro-esophageal reflux disease with esophagitis"},
    {"code": "G43.909", "display": "Migraine, unspecified, not intractable"},
]

INSURANCE_TYPES = ["medicare", "medicaid", "commercial", "self-pay", "tricare"]

# States data must match synthetic_data/generate.py exactly
STATES_DATA = [
    ("CA", "California", "Full practice authority after transition period", 30),
    ("TX", "Texas", "Collaborative agreement required", 14),
    ("FL", "Florida", "Physician supervision required", 7),
    ("NY", "New York", "Collaborative agreement with physician", 30),
    ("PA", "Pennsylvania", "Collaborative agreement required", 30),
    ("IL", "Illinois", "Full practice authority", 45),
    ("OH", "Ohio", "Collaborative agreement required", 14),
    ("GA", "Georgia", "Physician delegation required", 7),
    ("NC", "North Carolina", "Supervisory agreement required", 14),
    ("MI", "Michigan", "Collaborative agreement required", 30),
]

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


def _generate_npi() -> str:
    """Generate a valid-looking NPI. Must match synthetic_data/generate.py."""
    return str(random.choice([1, 2])) + "".join(str(random.randint(0, 9)) for _ in range(9))


def _replicate_oltp_seed_sequence() -> list[str]:
    """Replicate the exact call sequence from synthetic_data/generate.py to extract provider NPIs.

    This is tightly coupled to generate.py. If that file's seed sequence changes,
    this function MUST be updated to match.
    """
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    # 1. generate_states() burns 10 uuid4() calls
    for _ in range(10):
        uuid.uuid4()

    # 2. generate_physicians(count=10) — burn calls per physician:
    #    random.choice(states), fake.first_name(), fake.last_name(),
    #    generate_npi(), random.choice(SPECIALTIES), fake.phone_number()
    for _ in range(10):
        random.choice(range(10))  # random.choice(states)
        fake.first_name()
        fake.last_name()
        _generate_npi()  # burns random.choice + 9x random.randint
        random.choice(SPECIALTIES)
        uuid.uuid4()  # physician id
        fake.phone_number()

    # 3. generate_providers(count=30) — capture NPIs
    provider_npis = []
    for _ in range(30):
        random.choice(range(10))  # random.choice(physicians)
        random.choice(range(10))  # random.choice(states)
        fake.first_name()
        fake.last_name()
        npi = _generate_npi()
        provider_npis.append(npi)
        random.choice(["NP", "PA"])  # provider_type
        uuid.uuid4()  # provider id
        fake.phone_number()
        fake.date_between(start_date="-5y", end_date="-30d")

    return provider_npis


def generate_patients(fake: Faker, count: int = 200) -> list[dict]:
    """Generate FHIR R4 Patient resources."""
    patients = []
    for i in range(count):
        patient_id = f"pat-{i + 1:03d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        gender = random.choice(["male", "female"])
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat()
        mrn = f"MRN{random.randint(100000, 999999)}"
        insurance = random.choice(INSURANCE_TYPES)

        patient = {
            "resourceType": "Patient",
            "id": patient_id,
            "identifier": [
                {
                    "use": "usual",
                    "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR"}]},
                    "system": "urn:oid:1.2.3.4.5.6",
                    "value": mrn,
                }
            ],
            "active": True,
            "name": [{"use": "official", "family": last_name, "given": [first_name]}],
            "telecom": [
                {"system": "phone", "value": fake.phone_number(), "use": "home"},
                {"system": "email", "value": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"},
            ],
            "gender": gender,
            "birthDate": birth_date,
            "address": [
                {
                    "use": "home",
                    "line": [fake.street_address()],
                    "city": fake.city(),
                    "state": fake.state_abbr(),
                    "postalCode": fake.zipcode(),
                    "country": "US",
                }
            ],
            "extension": [
                {
                    "url": "http://paracelsus.local/fhir/StructureDefinition/insurance-type",
                    "valueString": insurance,
                }
            ],
        }
        patients.append(patient)
    return patients


def generate_appointments(fake: Faker, patients: list[dict], provider_npis: list[str], count: int = 500) -> list[dict]:
    """Generate FHIR R4 Appointment resources."""
    appointments = []
    for i in range(count):
        appointment_id = f"appt-{i + 1:04d}"
        patient = random.choice(patients)
        npi = random.choice(provider_npis)
        status = random.choices(APPOINTMENT_STATUSES, weights=APPOINTMENT_STATUS_WEIGHTS)[0]
        service_type = random.choice(SERVICE_TYPES)

        # Random appointment time in last 90 days
        days_ago = random.randint(0, 90)
        hour = random.randint(8, 17)
        minute = random.choice([0, 15, 30, 45])
        start_dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0) - timedelta(days=days_ago)
        duration = random.choice([15, 30, 45, 60])
        end_dt = start_dt + timedelta(minutes=duration)
        created_dt = start_dt - timedelta(days=random.randint(1, 30))

        appointment = {
            "resourceType": "Appointment",
            "id": appointment_id,
            "status": status,
            "serviceType": [
                {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "display": service_type,
                        }
                    ]
                }
            ],
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "minutesDuration": duration,
            "created": created_dt.isoformat(),
            "participant": [
                {
                    "actor": {"reference": f"Patient/{patient['id']}", "display": patient["name"][0]["given"][0]},
                    "status": "accepted",
                },
                {
                    "actor": {
                        "reference": f"Practitioner/npi-{npi}",
                        "identifier": {"system": "http://hl7.org/fhir/sid/us-npi", "value": npi},
                    },
                    "status": "accepted",
                },
            ],
        }
        appointments.append(appointment)
    return appointments


def generate_encounters(fake: Faker, patients: list[dict], provider_npis: list[str], count: int = 400) -> list[dict]:
    """Generate FHIR R4 Encounter resources."""
    encounters = []
    for i in range(count):
        encounter_id = f"enc-{i + 1:04d}"
        patient = random.choice(patients)
        npi = random.choice(provider_npis)
        status = random.choices(ENCOUNTER_STATUSES, weights=ENCOUNTER_STATUS_WEIGHTS)[0]
        enc_class = random.choices(ENCOUNTER_CLASSES, weights=ENCOUNTER_CLASS_WEIGHTS)[0]
        enc_type = random.choice(ENCOUNTER_TYPES)
        reason = random.choice(REASON_CODES)
        diagnosis = random.choice(DIAGNOSIS_CODES)

        # Random encounter time in last 90 days
        days_ago = random.randint(0, 90)
        hour = random.randint(8, 17)
        start_dt = datetime.now().replace(hour=hour, minute=0, second=0, microsecond=0) - timedelta(days=days_ago)
        length_minutes = random.choice([15, 30, 45, 60, 90, 120])
        end_dt = start_dt + timedelta(minutes=length_minutes)

        encounter = {
            "resourceType": "Encounter",
            "id": encounter_id,
            "status": status,
            "class": enc_class,
            "type": [{"coding": [enc_type]}],
            "subject": {"reference": f"Patient/{patient['id']}"},
            "participant": [
                {
                    "individual": {
                        "reference": f"Practitioner/npi-{npi}",
                        "identifier": {"system": "http://hl7.org/fhir/sid/us-npi", "value": npi},
                    }
                }
            ],
            "period": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
            },
            "reasonCode": [{"coding": [reason], "text": reason["display"]}],
            "diagnosis": [
                {
                    "condition": {
                        "display": diagnosis["display"],
                    },
                    "use": {
                        "coding": [
                            {
                                "system": "http://hl7.org/fhir/diagnosis-role",
                                "code": "AD",
                                "display": "Admission diagnosis",
                            }
                        ]
                    },
                    "rank": 1,
                }
            ],
            "length": {
                "value": length_minutes,
                "unit": "minutes",
                "system": "http://unitsofmeasure.org",
                "code": "min",
            },
        }

        # Add ICD-10 code as extension on the condition reference
        encounter["diagnosis"][0]["condition"]["extension"] = [
            {
                "url": "http://paracelsus.local/fhir/StructureDefinition/icd10-code",
                "valueString": diagnosis["code"],
            }
        ]

        encounters.append(encounter)
    return encounters


def build_bundle(resources: list[dict], resource_type: str, page_size: int = 50) -> list[dict]:
    """Build paginated FHIR Bundle responses for a list of resources."""
    bundles = []
    total = len(resources)

    for offset in range(0, total, page_size):
        page = resources[offset : offset + page_size]
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": total,
            "link": [
                {
                    "relation": "self",
                    "url": f"/fhir/{resource_type}?_count={page_size}&_offset={offset}",
                }
            ],
            "entry": [{"fullUrl": f"{resource_type}/{r['id']}", "resource": r} for r in page],
        }

        next_offset = offset + page_size
        if next_offset < total:
            bundle["link"].append(
                {
                    "relation": "next",
                    "url": f"/fhir/{resource_type}?_count={page_size}&_offset={next_offset}",
                }
            )

        bundles.append(bundle)

    return bundles


def main():
    """Generate all FHIR R4 fixtures."""
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Replicate OLTP seed sequence to get matching provider NPIs
    provider_npis = _replicate_oltp_seed_sequence()
    print(f"Captured {len(provider_npis)} provider NPIs from OLTP seed sequence")

    # Step 2: Generate FHIR resources with a separate Faker instance
    # Use a different seed range so patient data doesn't collide with OLTP data
    fake = Faker()
    Faker.seed(1042)
    random.seed(1042)

    patients = generate_patients(fake, count=200)
    appointments = generate_appointments(fake, patients, provider_npis, count=500)
    encounters = generate_encounters(fake, patients, provider_npis, count=400)

    print(f"Generated {len(patients)} patients, {len(appointments)} appointments, {len(encounters)} encounters")

    # Step 3: Build paginated bundles and save
    patient_bundles = build_bundle(patients, "Patient", page_size=50)
    appointment_bundles = build_bundle(appointments, "Appointment", page_size=50)
    encounter_bundles = build_bundle(encounters, "Encounter", page_size=50)

    with open(FIXTURES_DIR / "patients.json", "w") as f:
        json.dump(patient_bundles, f, indent=2, default=str)

    with open(FIXTURES_DIR / "appointments.json", "w") as f:
        json.dump(appointment_bundles, f, indent=2, default=str)

    with open(FIXTURES_DIR / "encounters.json", "w") as f:
        json.dump(encounter_bundles, f, indent=2, default=str)

    print(f"Saved fixtures to {FIXTURES_DIR}")


if __name__ == "__main__":
    main()
