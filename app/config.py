import os
from dotenv import load_dotenv
import boto3
from google.cloud import storage
from google.oauth2.service_account import Credentials

load_dotenv()

REQUIRED_VARS = [
    'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION',
    'GCP_SERVICE_ACCOUNT_JSON', 'TARGET_GCS_BUCKET'
]
for var in REQUIRED_VARS:
    if not os.getenv(var):
        raise ValueError(f"Missing required env var: {var}. Check .env file.")


s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)


gcp_creds_path = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
if not os.path.exists(gcp_creds_path):
    raise ValueError(f"GCP service account JSON not found at: {gcp_creds_path}")
credentials = Credentials.from_service_account_file(gcp_creds_path)
gcs_client = storage.Client(credentials=credentials)
gcs_bucket = gcs_client.bucket(os.getenv('TARGET_GCS_BUCKET'))