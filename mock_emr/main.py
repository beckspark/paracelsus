"""Mock FHIR R4 EMR API for Singer/Meltano tap-rest-api-msdk compatibility.

Serves JSON fixtures in FHIR R4 Bundle format with pagination.
Fixtures are generated on startup using Faker if they don't exist.
"""

import json
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Mock FHIR R4 EMR API",
    description="FHIR R4 API mock for Meltano tap-rest-api-msdk testing",
    version="1.0.0",
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Resource type to fixture filename mapping
RESOURCE_MAP = {
    "Patient": "patients",
    "Appointment": "appointments",
    "Encounter": "encounters",
}


def generate_fixtures_if_needed():
    """Generate fixtures on startup if they don't exist."""
    if (FIXTURES_DIR / "patients.json").exists():
        return

    from generate_fixtures import main as generate_main

    print("Generating FHIR R4 fixtures...")
    generate_main()
    print("Fixtures generated.")


def load_bundles(resource_type: str) -> list[dict]:
    """Load paginated FHIR Bundle fixtures for a resource type."""
    filename = RESOURCE_MAP.get(resource_type)
    if not filename:
        return []
    path = FIXTURES_DIR / f"{filename}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


# Generate fixtures on module load (startup)
generate_fixtures_if_needed()

# Pre-load all bundles into memory
BUNDLES: dict[str, list[dict]] = {}
for resource_type in RESOURCE_MAP:
    BUNDLES[resource_type] = load_bundles(resource_type)


def get_page(resource_type: str, count: int, offset: int) -> dict:
    """Get a page of results from pre-loaded bundles, supporting arbitrary count/offset."""
    bundles = BUNDLES.get(resource_type, [])
    if not bundles:
        return {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 0,
            "entry": [],
        }

    # Flatten all entries from all bundles
    all_entries = []
    total = bundles[0].get("total", 0) if bundles else 0
    for bundle in bundles:
        all_entries.extend(bundle.get("entry", []))

    # Slice for requested page
    page_entries = all_entries[offset : offset + count]

    response = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
        "link": [
            {
                "relation": "self",
                "url": f"/fhir/{resource_type}?_count={count}&_offset={offset}",
            }
        ],
        "entry": page_entries,
    }

    # Add next link if more pages exist
    next_offset = offset + count
    if next_offset < total:
        response["link"].append(
            {
                "relation": "next",
                "url": f"/fhir/{resource_type}?_count={count}&_offset={next_offset}",
            }
        )

    return response


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "mock-fhir-emr"}


@app.get("/fhir/metadata")
async def capability_statement():
    """FHIR CapabilityStatement (minimal, for discovery)."""
    return {
        "resourceType": "CapabilityStatement",
        "status": "active",
        "kind": "instance",
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [
            {
                "mode": "server",
                "resource": [
                    {"type": rt, "interaction": [{"code": "read"}, {"code": "search-type"}]} for rt in RESOURCE_MAP
                ],
            }
        ],
    }


@app.get("/fhir/Patient")
async def search_patients(
    _count: int = Query(default=50, alias="_count"),
    _offset: int = Query(default=0, alias="_offset"),
):
    """FHIR Patient search endpoint with pagination."""
    return JSONResponse(content=get_page("Patient", _count, _offset))


@app.get("/fhir/Appointment")
async def search_appointments(
    _count: int = Query(default=50, alias="_count"),
    _offset: int = Query(default=0, alias="_offset"),
):
    """FHIR Appointment search endpoint with pagination."""
    return JSONResponse(content=get_page("Appointment", _count, _offset))


@app.get("/fhir/Encounter")
async def search_encounters(
    _count: int = Query(default=50, alias="_count"),
    _offset: int = Query(default=0, alias="_offset"),
):
    """FHIR Encounter search endpoint with pagination."""
    return JSONResponse(content=get_page("Encounter", _count, _offset))
