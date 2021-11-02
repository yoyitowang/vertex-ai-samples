from typing import Optional
from google.cloud import storage
from google.cloud.aiplatform import utils
from google.auth import credentials as auth_credentials


def upload_file(
    local_file_path: str,
    gcs_uri: str,
    project: Optional[str] = None,
    credentials: Optional[auth_credentials.Credentials] = None,
) -> str:
    """Copies a local file to a GCS path."""

    gcs_bucket, blob_path = utils.extract_bucket_and_prefix_from_gcs_path(gcs_uri)
    client = storage.Client(project=project, credentials=credentials)
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_file_path)

    gcs_path = "".join(["gs://", "/".join([blob.bucket.name, blob.name])])
    return gcs_path
