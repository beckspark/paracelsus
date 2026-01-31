# Paracelsus: AWS ELT Pipeline POC

Meltano proof-of-concept demonstrating an ephemeral GitHub Action-triggered ELT pipeline for provider supervision case load analytics.

## Quick Start

```bash
# Copy environment template (required)
cp .env.example .env

# Start all services
docker-compose up -d

# Wait for services to initialize, then run setup
./scripts/init.sh

# Run the full ELT pipeline (all 3 sources + dbt transforms)
docker-compose exec meltano meltano run elt-all

# Alternative: Run via GitHub Actions locally
./scripts/run-elt.sh elt-all

# Query the results
docker-compose exec olap-db psql -U warehouse -d analytics -c \
  "select count(*) from public_marts.fact_provider_case_load"
```

**Expected results:**
- tap-postgres: 350 records (physicians, providers, cases, case_reviews, states)
- tap-s3-csv: 80 records (contacts_csv, deals_csv)
- tap-hubspot: 50 records (contacts)
- dbt: 13 models, 48 tests passing
- fact_provider_case_load: ~7,620 rows

## Running Pipeline Locally (act)

Run the GitHub Actions workflow locally using [act](https://github.com/nektos/act):

```bash
# Prerequisites: act installed, Docker stack running
./scripts/run-elt.sh              # Default: elt-all
./scripts/run-elt.sh elt-postgres # Postgres only (EL + dbt)
./scripts/run-elt.sh el-all       # Extract/Load only (no dbt)
```

This executes the same workflow that runs in GitHub Actions, with automatic
SSL certificate patching for the mock HubSpot API.

## Architecture

```
┌──────────────────┐
│    LocalStack    │
│  ┌────────────┐  │
│  │     S3     │  │
│  │   (CSVs)   │  │
│  └─────┬──────┘  │
└────────┼─────────┘
         │
         ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                            Meltano                               │
│  tap-postgres ──► target-postgres ──► dbt-postgres:run ──► dbt-postgres:test
│  (EL)              (Load to raw)       (T: staging → marts)   (validate)   │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │
        ┌───────────────────┴───────────────────┐
        ▼                                       ▼
┌───────────────┐                    ┌───────────────────────────┐
│  OLTP (PG)    │                    │   OLAP Warehouse (PG)     │
│  - physicians │                    │   Raw → Staging → Marts   │
│  - providers  │                    └───────────────────────────┘
│  - cases      │
│  - reviews    │
└───────────────┘

Orchestration: GitHub Actions (scheduled workflows or manual triggers)
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| LocalStack | http://localhost:4566 | - |
| Mock HubSpot API | https://localhost:8443 | Bearer token (any) |
| OLTP PostgreSQL | localhost:5433 | admin / oltp_secret |
| OLAP PostgreSQL | localhost:5435 | warehouse / olap_secret |

## Project Structure

```
paracelsus/
├── docker-compose.yml          # Main compose (includes modular configs)
├── .env                        # Credentials (copy from .env.example)
├── .env.example                # Template for local development
├── compose/                    # Modular docker-compose configs
│   ├── databases.yml           # OLTP and OLAP Postgres
│   ├── infrastructure.yml      # LocalStack (AWS emulator)
│   ├── mock-services.yml       # Mock HubSpot API
│   └── meltano.yml             # Meltano ELT and seeder
├── meltano/                    # Meltano ELT project
│   ├── meltano.yml             # Single config (EL + dbt utility)
│   └── Dockerfile
├── mock_hubspot/               # Mock HubSpot API
│   ├── main.py
│   └── Dockerfile
├── synthetic_data/             # Data generators
│   ├── generate.py
│   ├── seed_oltp.py
│   ├── seed_s3.py
│   └── init_oltp.sql
├── dbt/                        # dbt transformation layer
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
└── scripts/
    └── init.sh
```

## Data Pipeline

### Unified ELT (Recommended)

Meltano handles both Extract/Load AND Transform in a single command:

```bash
# Run unified ELT pipeline
docker-compose exec meltano meltano run elt-postgres
```

This single command:
1. Extracts from OLTP database (tap-postgres)
2. Loads to warehouse (target-postgres)
3. Transforms with dbt (dbt-postgres:run)
4. Validates with dbt tests (dbt-postgres:test)

### Individual Steps

If you need to run steps separately:

```bash
# Extract & Load only (no transform)
docker-compose exec meltano meltano run el-postgres

# Transform only (run dbt manually)
docker-compose exec meltano meltano invoke dbt-postgres run
docker-compose exec meltano meltano invoke dbt-postgres test
```

### Data Sources

Three source patterns demonstrated:

1. **PostgreSQL → PostgreSQL** (database replication)
   ```bash
   docker-compose exec meltano meltano run tap-postgres target-postgres
   ```
   - Source: OLTP supervision database
   - Tables: physicians, providers, cases, case_reviews, states

2. **S3 → PostgreSQL** (file ingestion)
   ```bash
   docker-compose exec meltano meltano run tap-s3-csv target-postgres
   ```
   - Source: LocalStack S3 bucket
   - Files: contacts.csv, deals.csv

3. **HubSpot API → PostgreSQL** (SaaS integration)
   ```bash
   docker-compose exec meltano meltano run tap-hubspot target-postgres
   ```
   - Source: Mock HubSpot API (connector-compatible)
   - Objects: contacts, companies, deals

### Transform (dbt)

```
staging/           → 1:1 with source tables, type casting
intermediate/      → Business logic, joins
marts/            → Fact and dimension tables
```

**Key Models:**
- `dim_physician` - Supervising physicians dimension
- `dim_provider` - NP/PA providers dimension
- `dim_state` - State reference dimension
- `fact_provider_case_load` - Daily case load metrics (grain: physician × date)

### Orchestration

In production, GitHub Actions orchestrates the pipeline on a schedule. For local development, use `./scripts/run-elt.sh` which runs the same GitHub Actions workflow using `act`.

## Usage

### Query the warehouse

```bash
# Connect to OLAP database
docker-compose exec olap-db psql -U warehouse -d analytics

# Example query
select
    p.full_name as physician,
    f.date_key,
    f.cases_reviewed,
    f.cases_pending_review,
    f.cases_overdue
from public_marts.fact_provider_case_load f
join public_marts.dim_physician p ON f.physician_key = p.physician_key
where f.date_key = CURRENT_DATE
order by f.cases_overdue DESC;
```

### LocalStack CLI

```bash
# List S3 buckets
docker-compose exec localstack awslocal s3 ls
```

## Development

### Prerequisites

- Docker & Docker Compose
- act for local GitHub Actions
- Python 3.14+ (for local development)

## License

MIT
