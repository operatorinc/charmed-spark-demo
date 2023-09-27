from glob import glob

from botocore.client import Config
import boto3

import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--access-key","-k",  required=True)
    parser.add_argument("--secret-key","-s",  required=True)
    parser.add_argument("--endpoint-url","-e",  default="https://s3.us-east-1.amazonaws.com")
    parser.add_argument("--bucket", "-b", default="operator-day-spark-logs")
    parser.add_argument("--action",  "-a",
                        choices=["create", "delete", "setup"], nargs="+")

    args = parser.parse_args()

    session = boto3.session.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key
    )
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
    s3 = session.client("s3", endpoint_url=args.endpoint_url, config=config)

    if "create" in args.action:
        print(f"Creating bucket {args.bucket}")
        s3.create_bucket(Bucket=args.bucket)

    if "setup" in args.action:
        print(f"Setting up Spark bucket {args.bucket}")

        def write_to_s3(filename: str):
            with open(filename, "rb") as fid:
                s3.put_object(Bucket=args.bucket, Key=filename, Body=fid)

        # Create the data
        for filename in glob("data/*"):
            write_to_s3(filename)

        # Create the script
        write_to_s3("scripts/stock_country_report.py")

        # Create the folder for the logs
        s3.put_object(Bucket=args.bucket, Key=("spark-events/"))

    if "delete" in args.action:
        print(f"Delete bucket {args.bucket}")

        try:
            # Delete objects if they exists
            s3.delete_objects(Bucket=args.bucket, Delete={
                "Objects": [
                    {"Key": item["Key"]}
                    for item in s3.list_objects_v2(Bucket=args.bucket).get("Contents", [])
                ]
            })
        except Exception:
            print("Bucket possibly already empty or not existing")

        s3.delete_bucket(Bucket=args.bucket)
