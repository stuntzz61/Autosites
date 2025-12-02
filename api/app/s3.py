"""
S3 file upload utilities
"""
import logging
import uuid
from fastapi import UploadFile
import boto3
from botocore.config import Config

from app.config import settings

log = logging.getLogger("s3")


def get_s3_client():
    """Get S3 client"""
    return boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT or None,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
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
    ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
    filename = f"{request_id}/{category}/{uuid.uuid4().hex}.{ext}"

    # Read file content
    content = await file.read()

    # Upload to S3
    client = get_s3_client()
    client.put_object(
        Bucket=settings.S3_BUCKET,
        Key=filename,
        Body=content,
        ContentType=file.content_type or "image/jpeg",
        ACL="public-read",
    )

    # Construct public URL
    if settings.S3_PUBLIC_URL:
        url = f"{settings.S3_PUBLIC_URL}/{filename}"
    else:
        url = f"{settings.S3_ENDPOINT}/{settings.S3_BUCKET}/{filename}"

    log.info(f"Uploaded file: {filename}")

    return url


async def delete_file_from_s3(url: str) -> bool:
    """
    Delete file from S3 by URL
    """
    try:
        # Extract key from URL
        if settings.S3_PUBLIC_URL:
            key = url.replace(f"{settings.S3_PUBLIC_URL}/", "")
        else:
            key = url.split(f"/{settings.S3_BUCKET}/")[-1]

        client = get_s3_client()
        client.delete_object(Bucket=settings.S3_BUCKET, Key=key)

        log.info(f"Deleted file: {key}")
        return True

    except Exception as e:
        log.error(f"Failed to delete file: {e}")
        return False

