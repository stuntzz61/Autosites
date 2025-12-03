"""
S3 file upload utilities
"""
import logging
import uuid
from typing import Optional
from fastapi import UploadFile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import settings

log = logging.getLogger("s3")


def get_s3_client():
    """Get S3 client for Yandex Cloud Object Storage"""
    config = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    )

    return boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT or None,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
        config=config,
    )


async def upload_file_to_s3(
    file: UploadFile,
    request_id: str,
    category: str = "gallery"
) -> str:
    """
    Upload file to S3 and return public URL
    """
    # Generate unique filename
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "jpg"
    filename = f"{request_id}/{category}/{uuid.uuid4().hex}.{ext}"

    # Read file content
    content = await file.read()

    # Upload to S3
    client = get_s3_client()

    try:
        client.put_object(
            Bucket=settings.S3_BUCKET,
            Key=filename,
            Body=content,
            ContentType=file.content_type or "image/jpeg",
            ACL="public-read",
        )
    except ClientError as e:
        # If ACL fails, try without it (bucket policy might handle access)
        if "AccessControlListNotSupported" in str(e):
            log.warning("ACL not supported, uploading without ACL")
            client.put_object(
                Bucket=settings.S3_BUCKET,
                Key=filename,
                Body=content,
                ContentType=file.content_type or "image/jpeg",
            )
        else:
            log.error(f"Failed to upload to S3: {e}")
            raise

    # Construct public URL
    if settings.S3_PUBLIC_URL:
        url = f"{settings.S3_PUBLIC_URL.rstrip('/')}/{filename}"
    elif settings.S3_ENDPOINT:
        url = f"{settings.S3_ENDPOINT.rstrip('/')}/{settings.S3_BUCKET}/{filename}"
    else:
        url = f"https://{settings.S3_BUCKET}.s3.{settings.S3_REGION}.amazonaws.com/{filename}"

    log.info(f"Uploaded file: {filename} -> {url}")

    return url


async def delete_file_from_s3(url: str) -> bool:
    """
    Delete file from S3 by URL
    """
    try:
        # Extract key from URL
        if settings.S3_PUBLIC_URL:
            key = url.replace(f"{settings.S3_PUBLIC_URL.rstrip('/')}/", "")
        elif settings.S3_ENDPOINT:
            key = url.split(f"/{settings.S3_BUCKET}/")[-1]
        else:
            # Standard AWS URL
            key = url.split(f"{settings.S3_BUCKET}.s3.")[1].split("/", 1)[1] if f"{settings.S3_BUCKET}.s3." in url else url.split("/")[-3:]
            if isinstance(key, list):
                key = "/".join(key)

        client = get_s3_client()
        client.delete_object(Bucket=settings.S3_BUCKET, Key=key)

        log.info(f"Deleted file: {key}")
        return True

    except Exception as e:
        log.error(f"Failed to delete file: {e}")
        return False

