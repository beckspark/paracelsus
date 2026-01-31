"""Main entry point to seed all data sources."""

import sys
import time

from seed_oltp import seed_database
from seed_s3 import seed_s3


def main():
    """Seed all data sources in sequence."""
    print("=" * 60)
    print("Paracelsus Data Seeder")
    print("=" * 60)

    # Add initial delay to ensure services are ready
    initial_delay = 5
    print(f"Waiting {initial_delay}s for services to initialize...")
    time.sleep(initial_delay)

    errors = []

    # Seed OLTP database
    print("\n" + "=" * 60)
    print("Step 1: Seeding OLTP Database")
    print("=" * 60)
    try:
        seed_database()
    except Exception as e:
        print(f"ERROR: Failed to seed OLTP database: {e}")
        errors.append(("OLTP", str(e)))

    # Seed S3 bucket
    print("\n" + "=" * 60)
    print("Step 2: Seeding S3 Bucket")
    print("=" * 60)
    try:
        seed_s3()
    except Exception as e:
        print(f"ERROR: Failed to seed S3: {e}")
        errors.append(("S3", str(e)))

    # Summary
    print("\n" + "=" * 60)
    print("Seeding Complete")
    print("=" * 60)

    if errors:
        print(f"Completed with {len(errors)} error(s):")
        for source, error in errors:
            print(f"  - {source}: {error}")
        sys.exit(1)
    else:
        print("All data sources seeded successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()
