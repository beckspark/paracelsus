"""Synthetic data generators for Paracelsus POC.

Uses Faker to generate realistic healthcare provider supervision data.
"""

import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)  # Reproducible data
random.seed(42)


# Medical specialties for supervising physicians
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

# Case types for provider supervision
CASE_TYPES = [
    "Initial Consultation",
    "Follow-up Visit",
    "Medication Management",
    "Chronic Disease Management",
    "Preventive Care",
    "Urgent Care",
    "Post-Surgical Follow-up",
    "Mental Health Assessment",
    "Lab Review",
    "Imaging Review",
]

# US States with supervision requirements
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


@dataclass
class State:
    id: str
    code: str
    name: str
    supervision_requirements: str
    review_frequency_days: int


@dataclass
class Physician:
    id: str
    npi: str
    first_name: str
    last_name: str
    specialty: str
    state_license_id: str
    email: str
    phone: str


@dataclass
class Provider:
    id: str
    npi: str
    first_name: str
    last_name: str
    provider_type: str
    supervising_physician_id: str
    state_id: str
    email: str
    phone: str
    hire_date: date


@dataclass
class Case:
    id: str
    provider_id: str
    patient_mrn: str
    case_type: str
    status: str
    priority: str
    created_at: datetime
    closed_at: datetime | None


@dataclass
class CaseReview:
    id: str
    case_id: str
    physician_id: str
    review_date: date
    review_status: str
    notes: str
    due_date: date
    completed_at: datetime | None


def generate_npi() -> str:
    """Generate a valid-looking NPI (10 digits starting with 1 or 2)."""
    return str(random.choice([1, 2])) + "".join(str(random.randint(0, 9)) for _ in range(9))


def generate_mrn() -> str:
    """Generate a medical record number."""
    return f"MRN{random.randint(100000, 999999)}"


def generate_states() -> list[State]:
    """Generate state records."""
    return [
        State(
            id=str(uuid.uuid4()),
            code=code,
            name=name,
            supervision_requirements=req,
            review_frequency_days=freq,
        )
        for code, name, req, freq in STATES_DATA
    ]


def generate_physicians(states: list[State], count: int = 10) -> list[Physician]:
    """Generate physician records."""
    physicians = []
    for _ in range(count):
        state = random.choice(states)
        first_name = fake.first_name()
        last_name = fake.last_name()
        physicians.append(
            Physician(
                id=str(uuid.uuid4()),
                npi=generate_npi(),
                first_name=first_name,
                last_name=last_name,
                specialty=random.choice(SPECIALTIES),
                state_license_id=state.id,
                email=f"{first_name.lower()}.{last_name.lower()}@hospital.org",
                phone=fake.phone_number(),
            )
        )
    return physicians


def generate_providers(physicians: list[Physician], states: list[State], count: int = 30) -> list[Provider]:
    """Generate provider (NP/PA) records."""
    providers = []
    for _ in range(count):
        physician = random.choice(physicians)
        state = random.choice(states)
        first_name = fake.first_name()
        last_name = fake.last_name()
        providers.append(
            Provider(
                id=str(uuid.uuid4()),
                npi=generate_npi(),
                first_name=first_name,
                last_name=last_name,
                provider_type=random.choice(["NP", "PA"]),
                supervising_physician_id=physician.id,
                state_id=state.id,
                email=f"{first_name.lower()}.{last_name.lower()}@clinic.org",
                phone=fake.phone_number(),
                hire_date=fake.date_between(start_date="-5y", end_date="-30d"),
            )
        )
    return providers


def generate_cases(providers: list[Provider], count: int = 100) -> list[Case]:
    """Generate patient case records."""
    cases = []
    for _ in range(count):
        provider = random.choice(providers)
        created = fake.date_time_between(start_date="-90d", end_date="-1d")

        # 70% open, 20% closed, 10% pending review
        status_roll = random.random()
        if status_roll < 0.7:
            status = "open"
            closed_at = None
        elif status_roll < 0.9:
            status = "closed"
            closed_at = created + timedelta(days=random.randint(1, 30))
        else:
            status = "pending_review"
            closed_at = None

        cases.append(
            Case(
                id=str(uuid.uuid4()),
                provider_id=provider.id,
                patient_mrn=generate_mrn(),
                case_type=random.choice(CASE_TYPES),
                status=status,
                priority=random.choices(["low", "normal", "high", "urgent"], weights=[0.1, 0.6, 0.2, 0.1])[0],
                created_at=created,
                closed_at=closed_at,
            )
        )
    return cases


def generate_case_reviews(
    cases: list[Case],
    providers: list[Provider],
    physicians: list[Physician],
    count: int = 200,
) -> list[CaseReview]:
    """Generate case review records."""
    # Build provider -> physician mapping
    provider_physician_map = {p.id: p.supervising_physician_id for p in providers}

    reviews = []
    for _ in range(count):
        case = random.choice(cases)
        physician_id = provider_physician_map.get(case.provider_id)

        if not physician_id:
            continue

        # Due date is typically 7-30 days after case creation
        due_date = case.created_at.date() + timedelta(days=random.randint(7, 30))

        # Determine review status based on due date
        today = date.today()
        if due_date < today:
            # Past due - could be completed or overdue
            if random.random() < 0.7:  # 70% completed
                status = "completed"
                completed_at = datetime.combine(
                    due_date - timedelta(days=random.randint(0, 5)),
                    datetime.min.time(),
                )
                review_date = completed_at.date()
            else:
                status = "overdue"
                completed_at = None
                review_date = due_date
        else:
            # Future due date - pending
            status = "pending"
            completed_at = None
            review_date = due_date

        reviews.append(
            CaseReview(
                id=str(uuid.uuid4()),
                case_id=case.id,
                physician_id=physician_id,
                review_date=review_date,
                review_status=status,
                notes=fake.sentence() if status == "completed" else None,
                due_date=due_date,
                completed_at=completed_at,
            )
        )

    return reviews


def generate_hubspot_contacts(count: int = 50) -> list[dict]:
    """Generate HubSpot-style contact records."""
    contacts = []
    for i in range(count):
        first_name = fake.first_name()
        last_name = fake.last_name()
        company = fake.company()

        contacts.append(
            {
                "id": str(i + 1),
                "properties": {
                    "firstname": first_name,
                    "lastname": last_name,
                    "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
                    "phone": fake.phone_number(),
                    "company": company,
                    "jobtitle": random.choice(
                        [
                            "Healthcare Administrator",
                            "Practice Manager",
                            "Medical Director",
                            "Compliance Officer",
                            "IT Director",
                            "CFO",
                            "CEO",
                        ]
                    ),
                    "lifecyclestage": random.choice(["subscriber", "lead", "marketingqualifiedlead", "customer"]),
                    "hs_lead_status": random.choice(["NEW", "OPEN", "IN_PROGRESS", "QUALIFIED", "UNQUALIFIED"]),
                    "createdate": fake.iso8601(),
                    "lastmodifieddate": fake.iso8601(),
                },
                "createdAt": fake.iso8601(),
                "updatedAt": fake.iso8601(),
                "archived": False,
            }
        )
    return contacts


def generate_hubspot_deals(contacts: list[dict], count: int = 30) -> list[dict]:
    """Generate HubSpot-style deal records."""
    deals = []
    stages = [
        "appointmentscheduled",
        "qualifiedtobuy",
        "presentationscheduled",
        "decisionmakerboughtin",
        "contractsent",
        "closedwon",
        "closedlost",
    ]

    for i in range(count):
        contact = random.choice(contacts) if contacts else None
        stage = random.choice(stages)

        deals.append(
            {
                "id": str(i + 1),
                "properties": {
                    "dealname": f"{fake.company()} - Provider Supervision Platform",
                    "amount": str(random.randint(10000, 500000)),
                    "dealstage": stage,
                    "pipeline": "default",
                    "closedate": fake.iso8601() if stage.startswith("closed") else None,
                    "hs_lastmodifieddate": fake.iso8601(),
                    "createdate": fake.iso8601(),
                },
                "createdAt": fake.iso8601(),
                "updatedAt": fake.iso8601(),
                "archived": False,
                "associations": {"contacts": {"results": [{"id": contact["id"]}]} if contact else {"results": []}},
            }
        )
    return deals


def generate_hubspot_companies(count: int = 20) -> list[dict]:
    """Generate HubSpot-style company records."""
    companies = []
    industries = [
        "Hospital & Health Care",
        "Medical Practice",
        "Health, Wellness and Fitness",
        "Pharmaceuticals",
        "Medical Devices",
        "Biotechnology",
    ]

    for i in range(count):
        companies.append(
            {
                "id": str(i + 1),
                "properties": {
                    "name": fake.company() + random.choice([" Health", " Medical", " Healthcare", ""]),
                    "domain": fake.domain_name(),
                    "industry": random.choice(industries),
                    "numberofemployees": str(random.choice([50, 100, 250, 500, 1000, 5000])),
                    "annualrevenue": str(random.randint(1000000, 100000000)),
                    "city": fake.city(),
                    "state": fake.state_abbr(),
                    "country": "United States",
                    "phone": fake.phone_number(),
                    "createdate": fake.iso8601(),
                    "hs_lastmodifieddate": fake.iso8601(),
                },
                "createdAt": fake.iso8601(),
                "updatedAt": fake.iso8601(),
                "archived": False,
            }
        )
    return companies


def generate_all_data() -> dict:
    """Generate all synthetic data."""
    states = generate_states()
    physicians = generate_physicians(states)
    providers = generate_providers(physicians, states)
    cases = generate_cases(providers)
    case_reviews = generate_case_reviews(cases, providers, physicians)

    hubspot_contacts = generate_hubspot_contacts()
    hubspot_deals = generate_hubspot_deals(hubspot_contacts)
    hubspot_companies = generate_hubspot_companies()

    return {
        "oltp": {
            "states": states,
            "physicians": physicians,
            "providers": providers,
            "cases": cases,
            "case_reviews": case_reviews,
        },
        "hubspot": {
            "contacts": hubspot_contacts,
            "deals": hubspot_deals,
            "companies": hubspot_companies,
        },
    }


if __name__ == "__main__":
    data = generate_all_data()
    print(f"Generated {len(data['oltp']['states'])} states")
    print(f"Generated {len(data['oltp']['physicians'])} physicians")
    print(f"Generated {len(data['oltp']['providers'])} providers")
    print(f"Generated {len(data['oltp']['cases'])} cases")
    print(f"Generated {len(data['oltp']['case_reviews'])} case reviews")
    print(f"Generated {len(data['hubspot']['contacts'])} HubSpot contacts")
    print(f"Generated {len(data['hubspot']['deals'])} HubSpot deals")
    print(f"Generated {len(data['hubspot']['companies'])} HubSpot companies")
