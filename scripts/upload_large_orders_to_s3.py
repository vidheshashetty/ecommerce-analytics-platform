import boto3
import os

LOCAL_FILE_PATH = "data/data.csv"
BUCKET_NAME = "ecommerce"
S3_KEY = "raw/data/orders_2.csv"

def upload_to_s3():
    s3 = boto3.client('s3')

    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"{LOCAL_FILE_PATH} not found")

    print("Uploading file to S3...")

    s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_KEY)

    print(f"✅ Uploaded to s3://{BUCKET_NAME}/{S3_KEY}")

if __name__ == "__main__":
    upload_to_s3()
