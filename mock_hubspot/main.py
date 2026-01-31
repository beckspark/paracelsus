"""Mock HubSpot API for Singer/Meltano tap-hubspot compatibility.

Serves JSON fixtures in HubSpot v3 API format.
Fixtures are generated on startup using Faker if they don't exist.
"""

import json
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

app = FastAPI(
    title="Mock HubSpot API",
    description="HubSpot CRM API mock for Singer tap-hubspot testing",
    version="1.0.0",
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def generate_fixtures_if_needed():
    """Generate fixtures on startup if they don't exist."""
    if (FIXTURES_DIR / "contacts.json").exists():
        return  # Already generated

    from generate_fixtures import main as generate_main

    print("Generating HubSpot fixtures...")
    generate_main()
    print("Fixtures generated.")


def load_fixture(name: str) -> dict:
    """Load a JSON fixture file."""
    path = FIXTURES_DIR / f"{name}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {"results": []}


# Generate fixtures on module load (startup)
generate_fixtures_if_needed()

CONTACTS = load_fixture("contacts")
COMPANIES = load_fixture("companies")
DEALS = load_fixture("deals")
PROPERTIES = load_fixture("properties")

# Map object type variations to canonical names
OBJECT_ALIASES = {
    "contacts": "contacts",
    "contact": "contacts",
    "companies": "companies",
    "company": "companies",
    "deals": "deals",
    "deal": "deals",
}


def verify_auth(
    authorization: str | None = Header(None),
    hapikey: str | None = Query(None),
) -> str:
    """Accept either Bearer token OR hapikey query param."""
    if hapikey:
        return hapikey
    if authorization and authorization.startswith("Bearer "):
        return authorization.replace("Bearer ", "")
    raise HTTPException(status_code=401, detail="Missing authentication")


def get_object_type(path: str) -> str | None:
    """Extract and normalize object type from any URL path."""
    for alias, canonical in OBJECT_ALIASES.items():
        if alias in path.lower():
            return canonical
    return None


def get_data_for_type(obj_type: str) -> dict:
    """Get the fixture data for an object type."""
    return {"contacts": CONTACTS, "companies": COMPANIES, "deals": DEALS}.get(obj_type, {"results": []})


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "mock-hubspot-api"}


# ============================================================================
# PROPERTIES ENDPOINTS
# ============================================================================
@app.api_route("/crm/v3/properties/{object_type}", methods=["GET"])
@app.api_route("/crm/v3/properties/{object_type}/", methods=["GET"])
async def properties_v3(object_type: str, _token: str = Depends(verify_auth)):
    """CRM v3 properties endpoint - returns {"results": [...]}."""
    canonical = OBJECT_ALIASES.get(object_type.lower())
    props = PROPERTIES.get(canonical, [])
    return {"results": props}


@app.api_route("/properties/v1/{object_type}/properties", methods=["GET"])
@app.api_route("/properties/v2/{object_type}/properties", methods=["GET"])
async def properties_legacy(object_type: str, _token: str = Depends(verify_auth)):
    """Legacy v1/v2 properties - returns list directly."""
    canonical = OBJECT_ALIASES.get(object_type.lower())
    return PROPERTIES.get(canonical, [])


@app.api_route("/companies/v2/properties", methods=["GET"])
@app.api_route("/contacts/v1/properties", methods=["GET"])
@app.api_route("/deals/v1/properties", methods=["GET"])
async def properties_object_specific(request: Request, _token: str = Depends(verify_auth)):
    """Object-specific legacy properties endpoints."""
    obj_type = get_object_type(request.url.path)
    return PROPERTIES.get(obj_type, [])


# ============================================================================
# DATA ENDPOINTS
# ============================================================================
@app.api_route("/crm/v3/objects/{object_type}", methods=["GET"])
async def objects_v3(
    object_type: str,
    limit: int = Query(default=100),
    after: str | None = Query(default=None),
    _token: str = Depends(verify_auth),
):
    """CRM v3 objects list endpoint."""
    canonical = OBJECT_ALIASES.get(object_type.lower())
    if not canonical:
        return {"results": []}

    data = get_data_for_type(canonical)
    all_results = data.get("results", [])

    start_idx = int(after) if after else 0
    end_idx = min(start_idx + limit, len(all_results))
    results = all_results[start_idx:end_idx]

    response: dict[str, Any] = {"results": results}
    if end_idx < len(all_results):
        response["paging"] = {"next": {"after": str(end_idx)}}
    return response


@app.api_route("/contacts/v1/lists/all/contacts/all", methods=["GET"])
async def contacts_legacy_all(
    count: int = Query(default=100),
    vidOffset: int | None = Query(default=None),
    _token: str = Depends(verify_auth),
):
    """Legacy contacts all endpoint."""
    all_results = CONTACTS.get("results", [])
    start_idx = vidOffset or 0
    end_idx = min(start_idx + count, len(all_results))
    results = all_results[start_idx:end_idx]

    response: dict[str, Any] = {"contacts": results, "has-more": end_idx < len(all_results)}
    if end_idx < len(all_results):
        response["vid-offset"] = end_idx
    return response


@app.api_route("/companies/v2/companies/paged", methods=["GET"])
async def companies_legacy_paged(
    limit: int = Query(default=100),
    offset: int = Query(default=0),
    _token: str = Depends(verify_auth),
):
    """Legacy companies paged endpoint."""
    all_results = COMPANIES.get("results", [])
    end_idx = min(offset + limit, len(all_results))
    return {
        "companies": all_results[offset:end_idx],
        "has-more": end_idx < len(all_results),
        "offset": end_idx if end_idx < len(all_results) else None,
    }


@app.api_route("/deals/v1/deal/paged", methods=["GET"])
async def deals_legacy_paged(
    limit: int = Query(default=100),
    offset: int = Query(default=0),
    _token: str = Depends(verify_auth),
):
    """Legacy deals paged endpoint."""
    all_results = DEALS.get("results", [])
    end_idx = min(offset + limit, len(all_results))
    return {
        "deals": all_results[offset:end_idx],
        "hasMore": end_idx < len(all_results),
        "offset": end_idx if end_idx < len(all_results) else None,
    }


# ============================================================================
# CATCH-ALL for unhandled endpoints
# ============================================================================
@app.api_route("/{path:path}", methods=["GET", "POST"])
async def catch_all(request: Request, path: str, _token: str = Depends(verify_auth)):
    """Catch-all for any unhandled endpoints. Returns sensible empty responses."""
    url = request.url.path.lower()

    # Properties endpoints
    if "properties" in url or "property" in url:
        obj_type = get_object_type(url)
        props = PROPERTIES.get(obj_type, [])
        return {"results": props} if "/v3/" in url else props

    # Object list endpoints
    if any(x in url for x in ["objects", "contacts", "companies", "deals"]):
        obj_type = get_object_type(url)
        if obj_type:
            data = get_data_for_type(obj_type)
            results = data.get("results", [])[:100]
            if "/v3/" in url:
                return {"results": results}
            if "paged" in url:
                return {"results": results, "has-more": False, "hasMore": False}
            if "/all" in url:
                return {"contacts": results, "has-more": False}
            return {"results": results}

    # Schema/search endpoints
    if "schema" in url:
        return {"results": []}
    if "search" in url:
        return {"total": 0, "results": []}

    return {"results": [], "total": 0}


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "message": exc.detail, "correlationId": "mock-error-id"},
    )
