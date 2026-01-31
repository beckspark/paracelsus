"""Seed OLTP database with synthetic data."""

import os
import time

import psycopg2
from generate import generate_all_data
from psycopg2.extras import execute_values


def get_connection():
    """Get database connection with retry logic."""
    max_retries = 10
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.environ.get("OLTP_HOST", "localhost"),
                port=int(os.environ.get("OLTP_PORT", "5433")),
                user=os.environ.get("OLTP_USER", "admin"),
                password=os.environ.get("OLTP_PASSWORD", "oltp_secret"),
                database=os.environ.get("OLTP_DB", "supervision"),
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise e


def seed_states(cursor, states):
    """Insert state records."""
    values = [
        (
            s.id,
            s.code,
            s.name,
            s.supervision_requirements,
            s.review_frequency_days,
        )
        for s in states
    ]

    execute_values(
        cursor,
        """
        INSERT INTO states (id, code, name, supervision_requirements, review_frequency_days)
        VALUES %s
        ON CONFLICT (code) DO NOTHING
        """,
        values,
    )
    print(f"Inserted {len(values)} states")


def seed_physicians(cursor, physicians):
    """Insert physician records."""
    values = [
        (
            p.id,
            p.npi,
            p.first_name,
            p.last_name,
            p.specialty,
            p.state_license_id,
            p.email,
            p.phone,
        )
        for p in physicians
    ]

    execute_values(
        cursor,
        """
        INSERT INTO physicians (id, npi, first_name, last_name, specialty,
                               state_license_id, email, phone)
        VALUES %s
        ON CONFLICT (npi) DO NOTHING
        """,
        values,
    )
    print(f"Inserted {len(values)} physicians")


def seed_providers(cursor, providers):
    """Insert provider records."""
    values = [
        (
            p.id,
            p.npi,
            p.first_name,
            p.last_name,
            p.provider_type,
            p.supervising_physician_id,
            p.state_id,
            p.email,
            p.phone,
            p.hire_date,
        )
        for p in providers
    ]

    execute_values(
        cursor,
        """
        INSERT INTO providers (id, npi, first_name, last_name, provider_type,
                              supervising_physician_id, state_id, email, phone, hire_date)
        VALUES %s
        ON CONFLICT (npi) DO NOTHING
        """,
        values,
    )
    print(f"Inserted {len(values)} providers")


def seed_cases(cursor, cases):
    """Insert case records."""
    values = [
        (
            c.id,
            c.provider_id,
            c.patient_mrn,
            c.case_type,
            c.status,
            c.priority,
            c.created_at,
            c.closed_at,
        )
        for c in cases
    ]

    execute_values(
        cursor,
        """
        INSERT INTO cases (id, provider_id, patient_mrn, case_type, status,
                          priority, created_at, closed_at)
        VALUES %s
        """,
        values,
    )
    print(f"Inserted {len(values)} cases")


def seed_case_reviews(cursor, reviews):
    """Insert case review records."""
    values = [
        (
            r.id,
            r.case_id,
            r.physician_id,
            r.review_date,
            r.review_status,
            r.notes,
            r.due_date,
            r.completed_at,
        )
        for r in reviews
    ]

    execute_values(
        cursor,
        """
        INSERT INTO case_reviews (id, case_id, physician_id, review_date,
                                  review_status, notes, due_date, completed_at)
        VALUES %s
        """,
        values,
    )
    print(f"Inserted {len(values)} case reviews")


def seed_database():
    """Main function to seed the OLTP database."""
    print("Generating synthetic data...")
    data = generate_all_data()
    oltp_data = data["oltp"]

    print("Connecting to OLTP database...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print("Seeding database...")

        # Seed in order of dependencies
        seed_states(cursor, oltp_data["states"])
        seed_physicians(cursor, oltp_data["physicians"])
        seed_providers(cursor, oltp_data["providers"])
        seed_cases(cursor, oltp_data["cases"])
        seed_case_reviews(cursor, oltp_data["case_reviews"])

        conn.commit()
        print("OLTP database seeded successfully!")

    except Exception as e:
        conn.rollback()
        print(f"Error seeding database: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    seed_database()
