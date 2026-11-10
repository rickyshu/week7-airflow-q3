"""List the Q8 source files, download the Netflix CSV, and count records."""

import argparse
import csv
from pathlib import Path

import boto3


PREFIX = "bronze/"
OBJECT_KEY = f"{PREFIX}netflix_titles.csv"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bucket", help="Name of the week 7 S3 bucket")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    print(f"Objects in s3://{args.bucket}/{PREFIX}")
    found = False
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=args.bucket, Prefix=PREFIX
    ):
        for item in page.get("Contents", []):
            if item["Key"] == PREFIX:
                continue
            print(f"  {item['Key']}  {item['Size']:,} bytes")
            if item["Key"] == OBJECT_KEY:
                found = True
    if not found:
        raise SystemExit(f"Missing object: s3://{args.bucket}/{OBJECT_KEY}")

    destination = Path(__file__).resolve().parent / "data" / "netflix_titles.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(args.bucket, OBJECT_KEY, str(destination))
    print(f"Downloaded to {destination.relative_to(destination.parent.parent)}")

    with destination.open(encoding="utf-8-sig", newline="") as source:
        records = csv.reader(source)
        next(records, None)  # Skip the header.
        count = sum(1 for row in records if row)
    print(f"CSV data records: {count:,}")


if __name__ == "__main__":
    main()
