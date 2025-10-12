import logging
from uuid import uuid4
import base64
import hashlib
import tempfile
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from botocore.exceptions import ClientError
from google.api_core.exceptions import GoogleAPIError
from .config import s3_client, gcs_bucket

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((ClientError, GoogleAPIError))
)
async def replicate_file(s3_bucket: str, s3_key: str) -> str:
    corr_id = str(uuid4())
    logger.info(f"[{corr_id}] Starting replication: S3 {s3_bucket}/{s3_key} to GCS {gcs_bucket.name}/{s3_key}")

    # check
    try:
        s3_head = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        s3_etag = s3_head['ETag'].strip('"')
        if '-' in s3_etag:
            logger.warning(f"[{corr_id}] Multi-part ETag detected; using existence check only")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise ValueError("S3 object not found")
        raise e

    gcs_blob = gcs_bucket.blob(s3_key)
    if gcs_blob.exists():
        gcs_blob.reload()
        gcs_md5_hex = base64.b64decode(gcs_blob.md5_hash).hex()
        if gcs_md5_hex == s3_etag or '-' in s3_etag:
            logger.info(f"[{corr_id}] Idempotent skip: File already exists with matching hash")
            return "File already replicated (idempotent skip)"

    
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    body = response["Body"]
    content_type = response.get("ContentType", "application/octet-stream")

    hasher = hashlib.md5()
    with tempfile.SpooledTemporaryFile(max_size=5 * 1024 * 1024) as spool:
        for chunk in iter(lambda: body.read(64 * 1024), b""):
            hasher.update(chunk)
            spool.write(chunk)
        spool.seek(0)

        try:
            gcs_blob.upload_from_file(spool, content_type=content_type)
        except Exception as e:
            logger.error(f"[{corr_id}] Upload failed: {str(e)}")
            raise ValueError(f"Upload error: {str(e)}")
        logger.info(f"[{corr_id}] Uploaded {s3_key} to GCS")

    # checksum validation
    computed_md5_hex = hasher.hexdigest()
    if computed_md5_hex != s3_etag and '-' not in s3_etag:
        logger.error(f"[{corr_id}] Checksum mismatch: Deleting corrupt GCS file")
        gcs_blob.delete()
        raise ValueError("Checksum mismatch after upload")

    logger.info(f"[{corr_id}] Replication complete")
    return f"Successfully replicated {s3_key} to GCS {gcs_bucket.name}"