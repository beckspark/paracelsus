# Paracelsus Work Log

## 2026-01-31: Python Version Upgrade & Dead Code Analysis

### Objective
1. Upgrade Python versions across all containers to latest supported versions
2. Add vulture for dead code analysis to pre-commit hooks

### Changes Made

#### Python Version Upgrades

| Component | Before | After | Notes |
|-----------|--------|-------|-------|
| `.python-version` | 3.13 | 3.14 | Local development |
| `pyproject.toml` | `>=3.12` | `>=3.14` | Package requirement |
| `mock_hubspot/Dockerfile` | 3.12-slim | 3.14-slim | Full 3.14 support |
| `synthetic_data/Dockerfile` | 3.12-slim | 3.14-slim | Upgraded psycopg2-binary to 2.9.11 |
| `meltano/Dockerfile` | 3.11-slim | 3.12-slim | Limited by dbt-postgres/msgspec |

**Why meltano stays at 3.12:**
- dbt-postgres doesn't support Python 3.14 ([GitHub #12098](https://github.com/dbt-labs/dbt-core/issues/12098)) - blocked by mashumaro and Pydantic V1 EOL
- msgspec (tap-s3-csv dependency) lacks 3.13 wheels ([GitHub #764](https://github.com/jcrist/msgspec/issues/764))
- Python 3.12 is the highest version with full wheel support for all Meltano plugins

#### Vulture Dead Code Analysis

Added vulture to pre-commit hooks:

**pyproject.toml:**
```toml
[tool.vulture]
min_confidence = 80
paths = ["."]
exclude = [".venv/", "mock_hubspot/", "synthetic_data/", "terraform/lambda/"]
```

**.pre-commit-config.yaml:**
```yaml
- id: vulture
  name: vulture
  entry: uv run vulture
  language: system
  types: [python]
  pass_filenames: false
```

#### dbt Model Fix

Updated `stg_hubspot__contacts.sql` to extract from JSON `properties` column instead of flattened columns:
```sql
properties->>'firstname' as first_name,  -- was: property_firstname
```

### Results
- All pre-commit hooks pass (ruff, ruff-format, basedpyright, vulture)
- Full ELT pipeline works with upgraded Python versions
- 7,620 rows in `fact_provider_case_load`

### Technical Decisions

1. **3.14 where possible, 3.12 for meltano**: Maximized Python version while respecting ecosystem constraints. Mock services and seeder get 3.14; meltano needs 3.12 for dependency compatibility.

2. **Vulture at 80% confidence**: Standard threshold balances false positive reduction with catching actual dead code.

3. **psycopg2-binary upgrade**: 2.9.11 required for Python 3.14 wheels (2.9.9 lacked them).

---

## 2026-01-31: Full ELT Pipeline Working

### Objective
Get `meltano run elt-all` working end-to-end with all three data sources (OLTP, S3, HubSpot) and dbt transforms.

### Changes Made

#### Phase 1: tap-s3-csv (Completed)
- Verified S3 CSV data exists in LocalStack
- Ran `tap-s3-csv target-postgres` successfully
- 80 records synced (contacts_csv, deals_csv)
- dbt model `stg_s3__contacts` builds successfully

#### Phase 2: HubSpot Mock Authentication (Completed)
- Updated `mock_hubspot/main.py` to accept both Bearer token AND `hapikey` query parameter
- singer-io tap-hubspot uses `?hapikey=xxx`, our mock now handles both auth methods

#### Phase 3: TLS for Mock HubSpot (Completed)
- **Problem**: tap-hubspot hardcodes `https://api.hubapi.com` - cannot override base URL
- **Solution**:
  - Added self-signed SSL certificate generation to mock-hubspot Dockerfile
  - Docker Compose links mock-hubspot as `api.hubapi.com` DNS alias
  - Meltano container patches tap-hubspot's certifi bundle with self-signed cert

**SSL Certificate Handling:**
- Removed global `REQUESTS_CA_BUNDLE`, `SSL_CERT_FILE` env vars (broke PyPI)
- System certifi patched in `entrypoint.sh` for general Python requests
- tap-hubspot virtualenv certifi patched separately after plugin install

#### Phase 4: tap-hubspot v3 API (Completed)
- **Switched from singer-io to meltanolabs tap-hubspot** (v3 API)
- Created `generate_fixtures.py` to produce HubSpot v3 format JSON at container startup
- Fixtures generated with Faker, seeded for reproducibility

**Files created/modified:**
| File | Change |
|------|--------|
| `mock_hubspot/main.py` | Serves JSON fixtures, catch-all routing |
| `mock_hubspot/generate_fixtures.py` | Generates contacts, companies, deals, properties |
| `mock_hubspot/Dockerfile` | Includes faker, generates SSL cert on build |
| `meltano/meltano.yml` | meltanolabs tap-hubspot with custom settings |
| `meltano/entrypoint.sh` | Patches system + tap certifi bundles |
| `docker-compose.yml` | Removed global SSL env vars, cert volume sharing |
| `dbt/models/staging/stg_hubspot__contacts.sql` | Updated for v3 API column names |

### Results
- **elt-all completes successfully**
- tap-postgres: 350 records (physicians, providers, cases, case_reviews, states)
- tap-s3-csv: 80 records (contacts_csv, deals_csv)
- tap-hubspot: 50 records (contacts only)
- dbt: 13 models, 48 tests - all pass
- `fact_provider_case_load`: 7,620 rows

### Known Limitations
1. **tap-hubspot syncs contacts only** - companies/deals disabled due to association schema mismatch with mock
2. **SSL cert patching required** - tap-hubspot virtualenv certifi must be patched after each `meltano install`
3. **Fixtures regenerated on container restart** - data is deterministic (seeded) but timestamps change

### Technical Decisions

1. **JSON Fixtures over Programmatic Mock**: Moved from inline Faker data generation to JSON fixtures for:
   - Explicit, inspectable data structure
   - Easier debugging of API format mismatches
   - Runtime generation keeps POC self-contained

2. **meltanolabs over singer-io tap**: The singer-io tap uses legacy v1/v2 API with complex nested `{"value": "...", "timestamp": ...}` format. meltanolabs uses v3 API with flat properties - simpler to mock.

3. **Contacts-only for POC**: Companies and deals have association schemas that require `companyId` NOT NULL. Rather than mock full association graph, limited to contacts for POC scope.

### Next Steps (Future)
- [ ] Add association mocking for companies/deals
- [ ] Automate certifi patching in Dockerfile build
- [ ] Add more HubSpot streams (owners, pipelines)
- [ ] Production-ready OAuth flow for real HubSpot

---

## Commands Reference

```bash
# Start full stack
docker-compose up -d

# Run full ELT pipeline
docker-compose exec meltano meltano run elt-all

# Individual source runs
docker-compose exec meltano meltano run tap-postgres target-postgres
docker-compose exec meltano meltano run tap-s3-csv target-postgres
docker-compose exec meltano meltano run tap-hubspot target-postgres

# dbt only
docker-compose exec meltano meltano invoke dbt-postgres run
docker-compose exec meltano meltano invoke dbt-postgres test

# Patch tap-hubspot SSL (after meltano install)
docker-compose exec meltano bash -c '
TAP_CERTIFI="/project/.meltano/extractors/tap-hubspot/venv/lib/python3.12/site-packages/certifi/cacert.pem"
cat /certs/cert.pem >> "$TAP_CERTIFI"
'

# Query results
docker-compose exec olap-db psql -U warehouse -d analytics -c "SELECT COUNT(*) FROM public_marts.fact_provider_case_load;"
```
