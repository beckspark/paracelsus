#!/usr/bin/env python3
"""Generate HubSpot v3 API fixtures from Faker data.

Run once to create JSON fixtures, then the mock serves them statically.
"""

import json
import random
from datetime import timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def generate_contacts(count: int = 50) -> dict:
    """Generate contacts in HubSpot v3 API format."""
    results = []
    for i in range(count):
        first_name = fake.first_name()
        last_name = fake.last_name()
        created = fake.date_time_between(start_date="-1y", end_date="now")
        updated = created + timedelta(days=random.randint(0, 30))

        results.append(
            {
                "id": str(i + 1),
                "properties": {
                    "firstname": first_name,
                    "lastname": last_name,
                    "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
                    "phone": fake.phone_number(),
                    "company": fake.company(),
                    "jobtitle": random.choice(
                        ["Healthcare Administrator", "Practice Manager", "Medical Director", "CFO", "CEO"]
                    ),
                    "lifecyclestage": random.choice(["subscriber", "lead", "marketingqualifiedlead", "customer"]),
                    "hs_lead_status": random.choice(["NEW", "OPEN", "IN_PROGRESS", "QUALIFIED"]),
                    "createdate": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_object_id": str(i + 1),
                },
                "createdAt": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "updatedAt": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "archived": False,
            }
        )
    return {"results": results}


def generate_companies(count: int = 20) -> dict:
    """Generate companies in HubSpot v3 API format."""
    results = []
    for i in range(count):
        created = fake.date_time_between(start_date="-1y", end_date="now")
        updated = created + timedelta(days=random.randint(0, 30))

        results.append(
            {
                "id": str(i + 1),
                "properties": {
                    "name": fake.company() + random.choice([" Health", " Medical", ""]),
                    "domain": fake.domain_name(),
                    "industry": random.choice(["Hospital & Health Care", "Medical Practice", "Pharmaceuticals"]),
                    "numberofemployees": str(random.choice([50, 100, 250, 500, 1000])),
                    "city": fake.city(),
                    "state": fake.state_abbr(),
                    "createdate": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_object_id": str(i + 1),
                },
                "createdAt": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "updatedAt": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "archived": False,
            }
        )
    return {"results": results}


def generate_deals(count: int = 30) -> dict:
    """Generate deals in HubSpot v3 API format."""
    results = []
    for i in range(count):
        created = fake.date_time_between(start_date="-6m", end_date="now")
        updated = created + timedelta(days=random.randint(0, 30))
        close = created + timedelta(days=90)

        results.append(
            {
                "id": str(i + 1),
                "properties": {
                    "dealname": f"{fake.company()} - Platform Deal",
                    "amount": str(random.randint(10000, 500000)),
                    "dealstage": random.choice(["appointmentscheduled", "qualifiedtobuy", "closedwon", "closedlost"]),
                    "pipeline": "default",
                    "closedate": close.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "createdate": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_lastmodifieddate": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "hs_object_id": str(i + 1),
                },
                "createdAt": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "updatedAt": updated.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "archived": False,
            }
        )
    return {"results": results}


def generate_properties() -> dict:
    """Generate property definitions for all object types."""
    return {
        "contacts": [
            {
                "name": "firstname",
                "type": "string",
                "label": "First Name",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "lastname",
                "type": "string",
                "label": "Last Name",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "email",
                "type": "string",
                "label": "Email",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "phone",
                "type": "string",
                "label": "Phone",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "company",
                "type": "string",
                "label": "Company",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "jobtitle",
                "type": "string",
                "label": "Job Title",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "lifecyclestage",
                "type": "string",
                "label": "Lifecycle Stage",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "hs_lead_status",
                "type": "string",
                "label": "Lead Status",
                "groupName": "contactinformation",
                "fieldType": "text",
            },
            {
                "name": "createdate",
                "type": "datetime",
                "label": "Create Date",
                "groupName": "contactinformation",
                "fieldType": "date",
            },
            {
                "name": "hs_lastmodifieddate",
                "type": "datetime",
                "label": "Last Modified",
                "groupName": "contactinformation",
                "fieldType": "date",
            },
        ],
        "companies": [
            {"name": "name", "type": "string", "label": "Name", "groupName": "companyinformation", "fieldType": "text"},
            {
                "name": "domain",
                "type": "string",
                "label": "Domain",
                "groupName": "companyinformation",
                "fieldType": "text",
            },
            {
                "name": "industry",
                "type": "string",
                "label": "Industry",
                "groupName": "companyinformation",
                "fieldType": "text",
            },
            {
                "name": "numberofemployees",
                "type": "number",
                "label": "Employees",
                "groupName": "companyinformation",
                "fieldType": "number",
            },
            {"name": "city", "type": "string", "label": "City", "groupName": "companyinformation", "fieldType": "text"},
            {
                "name": "state",
                "type": "string",
                "label": "State",
                "groupName": "companyinformation",
                "fieldType": "text",
            },
            {
                "name": "createdate",
                "type": "datetime",
                "label": "Create Date",
                "groupName": "companyinformation",
                "fieldType": "date",
            },
            {
                "name": "hs_lastmodifieddate",
                "type": "datetime",
                "label": "Last Modified",
                "groupName": "companyinformation",
                "fieldType": "date",
            },
        ],
        "deals": [
            {
                "name": "dealname",
                "type": "string",
                "label": "Deal Name",
                "groupName": "dealinformation",
                "fieldType": "text",
            },
            {
                "name": "amount",
                "type": "number",
                "label": "Amount",
                "groupName": "dealinformation",
                "fieldType": "number",
            },
            {
                "name": "dealstage",
                "type": "string",
                "label": "Deal Stage",
                "groupName": "dealinformation",
                "fieldType": "text",
            },
            {
                "name": "pipeline",
                "type": "string",
                "label": "Pipeline",
                "groupName": "dealinformation",
                "fieldType": "text",
            },
            {
                "name": "closedate",
                "type": "datetime",
                "label": "Close Date",
                "groupName": "dealinformation",
                "fieldType": "date",
            },
            {
                "name": "createdate",
                "type": "datetime",
                "label": "Create Date",
                "groupName": "dealinformation",
                "fieldType": "date",
            },
            {
                "name": "hs_lastmodifieddate",
                "type": "datetime",
                "label": "Last Modified",
                "groupName": "dealinformation",
                "fieldType": "date",
            },
        ],
    }


def main():
    FIXTURES_DIR.mkdir(exist_ok=True)

    # Generate and save fixtures
    fixtures = {
        "contacts.json": generate_contacts(),
        "companies.json": generate_companies(),
        "deals.json": generate_deals(),
        "properties.json": generate_properties(),
    }

    for filename, data in fixtures.items():
        path = FIXTURES_DIR / filename
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Generated {path}")

    print(f"\nFixtures generated in {FIXTURES_DIR}")


if __name__ == "__main__":
    main()
