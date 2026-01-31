# Paracelsus Work Log

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
TAP_CERTIFI="/project/.meltano/extractors/tap-hubspot/venv/lib/python3.11/site-packages/certifi/cacert.pem"
cat /certs/cert.pem >> "$TAP_CERTIFI"
'

# Query results
docker-compose exec olap-db psql -U warehouse -d analytics -c "SELECT COUNT(*) FROM public_marts.fact_provider_case_load;"
```
