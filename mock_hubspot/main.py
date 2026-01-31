"""Mock HubSpot API for Singer/Meltano tap-hubspot compatibility.

Implements HubSpot's CRM API v3 contract so the real tap-hubspot connector
can be used without modification. This enables demonstrating SaaS API ingestion
patterns in a local development environment.
"""

import random
from datetime import timedelta
from typing import Any

from faker import Faker
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Mock HubSpot API",
    description="HubSpot CRM API v3 mock for Singer tap-hubspot testing",
    version="1.0.0",
)

fake = Faker()
Faker.seed(42)
random.seed(42)

# Generate mock data on startup
CONTACTS: list[dict] = []
DEALS: list[dict] = []
COMPANIES: list[dict] = []


def generate_mock_data():
    """Generate mock HubSpot data on startup."""
    global CONTACTS, DEALS, COMPANIES

    # Generate contacts
    for i in range(50):
        first_name = fake.first_name()
        last_name = fake.last_name()
        created = fake.date_time_between(start_date="-1y", end_date="now")

        CONTACTS.append(
            {
                "id": str(i + 1),
                "properties": {
                    "firstname": first_name,
                    "lastname": last_name,
                    "email": f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}",
                    "phone": fake.phone_number(),
                    "company": fake.company(),
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
                    "createdate": created.isoformat() + "Z",
                    "hs_object_id": str(i + 1),
                    "lastmodifieddate": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                },
                "createdAt": created.isoformat() + "Z",
                "updatedAt": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                "archived": False,
            }
        )

    # Generate companies
    industries = [
        "Hospital & Health Care",
        "Medical Practice",
        "Health, Wellness and Fitness",
        "Pharmaceuticals",
        "Medical Devices",
        "Biotechnology",
    ]

    for i in range(20):
        created = fake.date_time_between(start_date="-1y", end_date="now")
        COMPANIES.append(
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
                    "createdate": created.isoformat() + "Z",
                    "hs_object_id": str(i + 1),
                    "hs_lastmodifieddate": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                },
                "createdAt": created.isoformat() + "Z",
                "updatedAt": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                "archived": False,
            }
        )

    # Generate deals
    stages = [
        "appointmentscheduled",
        "qualifiedtobuy",
        "presentationscheduled",
        "decisionmakerboughtin",
        "contractsent",
        "closedwon",
        "closedlost",
    ]

    for i in range(30):
        created = fake.date_time_between(start_date="-6m", end_date="now")
        stage = random.choice(stages)
        contact = random.choice(CONTACTS) if CONTACTS else None
        company = random.choice(COMPANIES) if COMPANIES else None

        DEALS.append(
            {
                "id": str(i + 1),
                "properties": {
                    "dealname": f"{fake.company()} - Provider Supervision Platform",
                    "amount": str(random.randint(10000, 500000)),
                    "dealstage": stage,
                    "pipeline": "default",
                    "closedate": (created + timedelta(days=90)).isoformat() + "Z"
                    if stage.startswith("closed")
                    else None,
                    "createdate": created.isoformat() + "Z",
                    "hs_object_id": str(i + 1),
                    "hs_lastmodifieddate": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                },
                "createdAt": created.isoformat() + "Z",
                "updatedAt": (created + timedelta(days=random.randint(0, 30))).isoformat() + "Z",
                "archived": False,
                "associations": {
                    "contacts": {"results": [{"id": contact["id"], "type": "deal_to_contact"}]}
                    if contact
                    else {"results": []},
                    "companies": {"results": [{"id": company["id"], "type": "deal_to_company"}]}
                    if company
                    else {"results": []},
                },
            }
        )


# Generate data on module load
generate_mock_data()


def verify_token(authorization: str | None = Header(None)) -> str:
    """Verify Bearer token (accepts any token for mock purposes)."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing authorization header")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header format")

    token = authorization.replace("Bearer ", "")
    if not token:
        raise HTTPException(status_code=401, detail="Empty token")

    return token


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "mock-hubspot-api"}


@app.get("/crm/v3/objects/contacts")
async def list_contacts(
    limit: int = Query(default=100, le=100),
    after: str | None = Query(default=None),
    properties: str | None = Query(default=None),
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """List contacts with pagination (HubSpot API compatible)."""
    start_idx = int(after) if after else 0
    end_idx = min(start_idx + limit, len(CONTACTS))

    results = CONTACTS[start_idx:end_idx]

    response: dict[str, Any] = {"results": results}

    # Add pagination info if there are more results
    if end_idx < len(CONTACTS):
        response["paging"] = {"next": {"after": str(end_idx), "link": f"?after={end_idx}"}}

    return response


@app.get("/crm/v3/objects/contacts/{contact_id}")
async def get_contact(
    contact_id: str,
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """Get a single contact by ID."""
    for contact in CONTACTS:
        if contact["id"] == contact_id:
            return contact

    raise HTTPException(status_code=404, detail="Contact not found")


@app.get("/crm/v3/objects/companies")
async def list_companies(
    limit: int = Query(default=100, le=100),
    after: str | None = Query(default=None),
    properties: str | None = Query(default=None),
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """List companies with pagination (HubSpot API compatible)."""
    start_idx = int(after) if after else 0
    end_idx = min(start_idx + limit, len(COMPANIES))

    results = COMPANIES[start_idx:end_idx]

    response: dict[str, Any] = {"results": results}

    if end_idx < len(COMPANIES):
        response["paging"] = {"next": {"after": str(end_idx), "link": f"?after={end_idx}"}}

    return response


@app.get("/crm/v3/objects/companies/{company_id}")
async def get_company(
    company_id: str,
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """Get a single company by ID."""
    for company in COMPANIES:
        if company["id"] == company_id:
            return company

    raise HTTPException(status_code=404, detail="Company not found")


@app.get("/crm/v3/objects/deals")
async def list_deals(
    limit: int = Query(default=100, le=100),
    after: str | None = Query(default=None),
    properties: str | None = Query(default=None),
    associations: str | None = Query(default=None),
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """List deals with pagination (HubSpot API compatible)."""
    start_idx = int(after) if after else 0
    end_idx = min(start_idx + limit, len(DEALS))

    results = DEALS[start_idx:end_idx]

    response: dict[str, Any] = {"results": results}

    if end_idx < len(DEALS):
        response["paging"] = {"next": {"after": str(end_idx), "link": f"?after={end_idx}"}}

    return response


@app.get("/crm/v3/objects/deals/{deal_id}")
async def get_deal(
    deal_id: str,
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """Get a single deal by ID."""
    for deal in DEALS:
        if deal["id"] == deal_id:
            return deal

    raise HTTPException(status_code=404, detail="Deal not found")


@app.get("/crm/v3/schemas")
async def list_schemas(
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """List object schemas (for connector discovery)."""
    return {
        "results": [
            {
                "id": "contacts",
                "name": "contacts",
                "labels": {"singular": "Contact", "plural": "Contacts"},
                "primaryDisplayProperty": "email",
                "properties": [
                    {"name": "firstname", "type": "string", "label": "First Name"},
                    {"name": "lastname", "type": "string", "label": "Last Name"},
                    {"name": "email", "type": "string", "label": "Email"},
                    {"name": "phone", "type": "string", "label": "Phone"},
                    {"name": "company", "type": "string", "label": "Company"},
                    {"name": "jobtitle", "type": "string", "label": "Job Title"},
                    {"name": "lifecyclestage", "type": "string", "label": "Lifecycle Stage"},
                    {"name": "hs_lead_status", "type": "string", "label": "Lead Status"},
                    {"name": "createdate", "type": "datetime", "label": "Create Date"},
                    {"name": "lastmodifieddate", "type": "datetime", "label": "Last Modified"},
                ],
            },
            {
                "id": "companies",
                "name": "companies",
                "labels": {"singular": "Company", "plural": "Companies"},
                "primaryDisplayProperty": "name",
                "properties": [
                    {"name": "name", "type": "string", "label": "Name"},
                    {"name": "domain", "type": "string", "label": "Domain"},
                    {"name": "industry", "type": "string", "label": "Industry"},
                    {"name": "numberofemployees", "type": "number", "label": "Employees"},
                    {"name": "annualrevenue", "type": "number", "label": "Annual Revenue"},
                    {"name": "city", "type": "string", "label": "City"},
                    {"name": "state", "type": "string", "label": "State"},
                ],
            },
            {
                "id": "deals",
                "name": "deals",
                "labels": {"singular": "Deal", "plural": "Deals"},
                "primaryDisplayProperty": "dealname",
                "properties": [
                    {"name": "dealname", "type": "string", "label": "Deal Name"},
                    {"name": "amount", "type": "number", "label": "Amount"},
                    {"name": "dealstage", "type": "string", "label": "Deal Stage"},
                    {"name": "pipeline", "type": "string", "label": "Pipeline"},
                    {"name": "closedate", "type": "datetime", "label": "Close Date"},
                ],
            },
        ]
    }


@app.get("/crm/v3/schemas/{object_type}")
async def get_schema(
    object_type: str,
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """Get schema for a specific object type."""
    schemas = (await list_schemas(_token))["results"]

    for schema in schemas:
        if schema["id"] == object_type:
            return schema

    raise HTTPException(status_code=404, detail=f"Schema not found for {object_type}")


# Search endpoint (simplified implementation)
@app.post("/crm/v3/objects/{object_type}/search")
async def search_objects(
    object_type: str,
    _token: str = Depends(verify_token),
) -> dict[str, Any]:
    """Search objects (returns all for mock purposes)."""
    data_map = {
        "contacts": CONTACTS,
        "companies": COMPANIES,
        "deals": DEALS,
    }

    if object_type not in data_map:
        raise HTTPException(status_code=404, detail=f"Unknown object type: {object_type}")

    return {"total": len(data_map[object_type]), "results": data_map[object_type]}


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions in HubSpot-compatible format."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "message": exc.detail,
            "correlationId": fake.uuid4(),
            "category": "VALIDATION_ERROR" if exc.status_code == 400 else "OBJECT_NOT_FOUND",
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
