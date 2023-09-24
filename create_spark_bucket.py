from botocore.client import Config
import boto3

import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--access-key", required=True)
    parser.add_argument("-s", "--access-secret", required=True)
    parser.add_argument("-e", "--endpoint-url", default="https://s3.us-east-1.amazonaws.com")
    parser.add_argument("-b", "--bucket", default="operator-day-spark-logs")

    args = parser.parse_args()
    

    session = boto3.session.Session(
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.access_secret
    )
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
    s3 = session.client("s3", endpoint_url=args.endpoint_url, config=config)

    s3.create_bucket(Bucket=args.bucket)
    s3.put_object(Bucket=args.bucket, Key=("spark-events/"))
