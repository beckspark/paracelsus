"""Seed S3 bucket with CSV/Excel files."""

import csv
import io
import os
import time

import boto3
import pandas as pd
from botocore.config import Config
from generate import generate_all_data


def get_s3_client():
    """Get S3 client configured for LocalStack with retry logic."""
    localstack_host = os.environ.get("LOCALSTACK_HOST", "localhost")
    endpoint_url = f"http://{localstack_host}:4566"

    max_retries = 10
    retry_delay = 2

    config = Config(
        retries={"max_attempts": 3, "mode": "standard"},
        connect_timeout=5,
        read_timeout=10,
    )

    for attempt in range(max_retries):
        try:
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
                region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                config=config,
            )
            # Test connection
            client.list_buckets()
            return client
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"S3 connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise e


def ensure_bucket_exists(s3_client, bucket_name: str):
    """Create bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except Exception:
        print(f"Creating bucket {bucket_name}")
        s3_client.create_bucket(Bucket=bucket_name)


def upload_contacts_csv(s3_client, bucket_name: str, contacts: list[dict]):
    """Upload HubSpot contacts as CSV."""
    # Flatten the nested structure for CSV
    rows = []
    for contact in contacts:
        row = {"id": contact["id"]}
        row.update(contact["properties"])
        rows.append(row)

    # Create CSV in memory
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    s3_client.put_object(
        Bucket=bucket_name,
        Key="hubspot/contacts.csv",
        Body=output.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Uploaded {len(rows)} contacts to s3://{bucket_name}/hubspot/contacts.csv")


def upload_deals_csv(s3_client, bucket_name: str, deals: list[dict]):
    """Upload HubSpot deals as CSV."""
    rows = []
    for deal in deals:
        row = {"id": deal["id"]}
        row.update(deal["properties"])
        # Add associated contact ID if present
        assoc = deal.get("associations", {}).get("contacts", {}).get("results", [])
        row["contact_id"] = assoc[0]["id"] if assoc else None
        rows.append(row)

    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    s3_client.put_object(
        Bucket=bucket_name,
        Key="hubspot/deals.csv",
        Body=output.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    print(f"Uploaded {len(rows)} deals to s3://{bucket_name}/hubspot/deals.csv")


def upload_state_requirements_excel(s3_client, bucket_name: str):
    """Upload state supervision requirements as Excel file."""
    # More detailed state requirements for reference data
    data = {
        "state_code": ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI"],
        "state_name": [
            "California",
            "Texas",
            "Florida",
            "New York",
            "Pennsylvania",
            "Illinois",
            "Ohio",
            "Georgia",
            "North Carolina",
            "Michigan",
        ],
        "np_practice_authority": [
            "Full (after transition)",
            "Reduced",
            "Restricted",
            "Reduced",
            "Reduced",
            "Full",
            "Reduced",
            "Restricted",
            "Reduced",
            "Reduced",
        ],
        "pa_supervision_required": [
            "No",
            "Yes",
            "Yes",
            "Yes",
            "Yes",
            "No",
            "Yes",
            "Yes",
            "Yes",
            "Yes",
        ],
        "chart_review_frequency_days": [30, 14, 7, 30, 30, 45, 14, 7, 14, 30],
        "physician_patient_ratio_limit": [
            "6:1",
            "7:1",
            "4:1",
            "6:1",
            "4:1",
            "No limit",
            "5:1",
            "4:1",
            "6:1",
            "5:1",
        ],
        "telehealth_supervision_allowed": [
            "Yes",
            "Yes",
            "Limited",
            "Yes",
            "Yes",
            "Yes",
            "Yes",
            "Limited",
            "Yes",
            "Yes",
        ],
        "prescriptive_authority": [
            "Full",
            "Limited",
            "Limited",
            "Full",
            "Limited",
            "Full",
            "Limited",
            "Limited",
            "Limited",
            "Limited",
        ],
        "last_updated": [
            "2024-01-15",
            "2024-02-01",
            "2023-12-01",
            "2024-01-20",
            "2023-11-15",
            "2024-03-01",
            "2024-01-10",
            "2023-10-01",
            "2024-02-15",
            "2024-01-05",
        ],
    }

    df = pd.DataFrame(data)

    # Create Excel in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="State Requirements", index=False)

    output.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key="reference/state_requirements.xlsx",
        Body=output.getvalue(),
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    print(f"Uploaded state requirements to s3://{bucket_name}/reference/state_requirements.xlsx")


def seed_s3():
    """Main function to seed S3 with files."""
    bucket_name = "paracelsus-landing"

    print("Generating synthetic data...")
    data = generate_all_data()
    hubspot_data = data["hubspot"]

    print("Connecting to LocalStack S3...")
    s3_client = get_s3_client()

    print("Ensuring bucket exists...")
    ensure_bucket_exists(s3_client, bucket_name)

    print("Uploading files to S3...")
    upload_contacts_csv(s3_client, bucket_name, hubspot_data["contacts"])
    upload_deals_csv(s3_client, bucket_name, hubspot_data["deals"])
    upload_state_requirements_excel(s3_client, bucket_name)

    print("S3 seeding completed successfully!")


if __name__ == "__main__":
    seed_s3()
