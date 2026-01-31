"""Lambda function to trigger Meltano ELT jobs.

This Lambda triggers the unified Meltano ELT pipeline which includes:
1. Extract data from sources (tap-postgres, tap-s3-csv, tap-hubspot)
2. Load data to warehouse (target-postgres)
3. Transform with dbt (dbt-postgres:run, dbt-postgres:test)

In production, this would trigger an ECS task or similar.
For LocalStack POC, we simulate the execution.
"""

import os
import subprocess


def handler(event, context):
    """Trigger Meltano ELT jobs.

    Args:
        event: Lambda event containing:
            - job: Meltano job name (e.g., 'elt-postgres', 'elt-all', 'el-all')
                   Defaults to 'elt-postgres' for unified ELT
            - environment: Meltano environment (dev/prod)
            - triggered_by: Source of trigger (scheduled, manual, etc.)
        context: Lambda context

    Returns:
        Dict with job execution status and details
    """
    job = event.get("job", "elt-postgres")
    environment = event.get("environment", os.environ.get("MELTANO_ENVIRONMENT", "dev"))
    triggered_by = event.get("triggered_by", "manual")

    print(f"Triggering Meltano job: {job} (environment: {environment}, triggered_by: {triggered_by})")

    try:
        result = execute_meltano_job(job, environment)

        return {
            "status": "completed" if result["success"] else "failed",
            "job": job,
            "environment": environment,
            "triggered_by": triggered_by,
            "message": result.get("message", ""),
            "streams_synced": result.get("streams", []),
            "records_processed": result.get("records", 0),
            "models_built": result.get("models", []),
            "tests_passed": result.get("tests_passed", 0),
        }

    except Exception as e:
        print(f"Error running Meltano job {job}: {e}")
        return {
            "status": "failed",
            "job": job,
            "error": str(e),
        }


def execute_meltano_job(job: str, environment: str) -> dict:
    """Execute a Meltano job.

    In a real AWS deployment, this would:
    1. Trigger an ECS task running the Meltano container
    2. Pass the job name and environment as arguments
    3. Monitor the task execution

    For LocalStack POC, we simulate successful execution.
    """
    # Job configurations matching meltano.yml
    job_configs = {
        # EL-only jobs
        "el-postgres": {
            "streams": [
                "public-physicians",
                "public-providers",
                "public-cases",
                "public-case_reviews",
                "public-states",
            ],
            "records": 350,
            "models": [],
            "tests_passed": 0,
        },
        "el-s3": {
            "streams": ["contacts_csv", "deals_csv"],
            "records": 80,
            "models": [],
            "tests_passed": 0,
        },
        "el-hubspot": {
            "streams": ["contacts", "companies", "deals"],
            "records": 100,
            "models": [],
            "tests_passed": 0,
        },
        "el-all": {
            "streams": [
                "public-physicians",
                "public-providers",
                "public-cases",
                "public-case_reviews",
                "public-states",
                "contacts_csv",
                "deals_csv",
                "contacts",
                "companies",
                "deals",
            ],
            "records": 530,
            "models": [],
            "tests_passed": 0,
        },
        # Unified ELT jobs (includes dbt transform)
        "elt-postgres": {
            "streams": [
                "public-physicians",
                "public-providers",
                "public-cases",
                "public-case_reviews",
                "public-states",
            ],
            "records": 350,
            "models": [
                "stg_oltp__physicians",
                "stg_oltp__providers",
                "stg_oltp__cases",
                "stg_oltp__case_reviews",
                "stg_oltp__states",
                "int_case_review_status",
                "int_physician_daily_metrics",
                "dim_physician",
                "dim_provider",
                "dim_state",
                "fact_provider_case_load",
            ],
            "tests_passed": 24,
        },
        "elt-all": {
            "streams": [
                "public-physicians",
                "public-providers",
                "public-cases",
                "public-case_reviews",
                "public-states",
                "contacts_csv",
                "deals_csv",
                "contacts",
                "companies",
                "deals",
            ],
            "records": 530,
            "models": [
                "stg_oltp__physicians",
                "stg_oltp__providers",
                "stg_oltp__cases",
                "stg_oltp__case_reviews",
                "stg_oltp__states",
                "stg_s3__contacts",
                "stg_hubspot__contacts",
                "int_case_review_status",
                "int_physician_daily_metrics",
                "dim_physician",
                "dim_provider",
                "dim_state",
                "fact_provider_case_load",
            ],
            "tests_passed": 24,
        },
    }

    config = job_configs.get(job, {"streams": [], "records": 0, "models": [], "tests_passed": 0})

    # Build descriptive message
    el_part = f"{len(config['streams'])} streams, ~{config['records']} records"
    if config.get("models"):
        t_part = f", {len(config['models'])} dbt models, {config['tests_passed']} tests passed"
    else:
        t_part = ""

    return {
        "success": True,
        "streams": config["streams"],
        "records": config["records"],
        "models": config.get("models", []),
        "tests_passed": config.get("tests_passed", 0),
        "message": f"Meltano job '{job}' completed: {el_part}{t_part}",
    }


def trigger_meltano_container(job: str, environment: str) -> dict:
    """Trigger Meltano via Docker exec (for local development).

    This demonstrates how to invoke Meltano in a real deployment.
    """
    cmd = [
        "docker",
        "exec",
        "paracelsus-meltano",
        "meltano",
        "--environment",
        environment,
        "run",
        job,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        return {
            "success": result.returncode == 0,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "message": "Meltano job completed" if result.returncode == 0 else "Meltano job failed",
        }
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "message": "Meltano job timed out after 900 seconds",
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to execute Meltano: {e}",
        }
