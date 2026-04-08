import boto3
import os
import argparse

def upload_to_s3(local_file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"{local_file_path} not found")

    print("Uploading file to S3...")
    s3.upload_file(local_file_path, bucket_name, s3_key)
    print(f"✅ Uploaded to s3://{bucket_name}/{s3_key}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True, help="Local CSV path")
    args = parser.parse_args()

    upload_to_s3(
        local_file_path=args.file_path,
        bucket_name="ecommerce",
        s3_key="raw/orders_5m/orders_5m.csv"
    )
