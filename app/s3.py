# app/s3.py
import os, mimetypes, uuid
import boto3

YC_S3_ENDPOINT = os.getenv("YC_S3_ENDPOINT", "https://storage.yandexcloud.net")
YC_S3_REGION   = os.getenv("YC_S3_REGION", "ru-central1")
YC_S3_KEY      = os.getenv("YC_S3_KEY")
YC_S3_SECRET   = os.getenv("YC_S3_SECRET")
YC_S3_BUCKET   = os.getenv("YC_S3_BUCKET")

_s3 = boto3.client(
    "s3",
    endpoint_url=YC_S3_ENDPOINT,
    aws_access_key_id=YC_S3_KEY,
    aws_secret_access_key=YC_S3_SECRET,
    region_name=YC_S3_REGION,
)

def guess_mime(filename: str, fallback="application/octet-stream"):
    return mimetypes.guess_type(filename)[0] or fallback

def put_bytes(key: str, data: bytes, content_type: str = None, metadata: dict = None) -> str:
    _s3.put_object(
        Bucket=YC_S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type or "application/octet-stream",
        Metadata=metadata or {},
    )
    return f"{YC_S3_ENDPOINT.rstrip('/')}/{YC_S3_BUCKET}/{key}"
