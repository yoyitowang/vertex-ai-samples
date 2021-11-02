from datetime import datetime
from typing import Optional
from google.cloud import storage
from google.cloud.aiplatform import utils
from google.auth import credentials as auth_credentials

import subprocess
import tarfile


def upload_file(
    local_file_path: str,
    remote_file_path: str,
    project: Optional[str] = None,
    credentials: Optional[auth_credentials.Credentials] = None,
) -> str:
    """Copies a local file to a GCS path."""

    gcs_bucket, blob_path = utils.extract_bucket_and_prefix_from_gcs_path(
        remote_file_path
    )
    client = storage.Client(project=project, credentials=credentials)
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_file_path)

    gcs_path = "".join(["gs://", "/".join([blob.bucket.name, blob.name])])
    return gcs_path


def archive_code_and_upload(staging_bucket: str):
    # Archive all source in current directory
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    source_archived_file = f"source_archived_{timestamp}.tar.gz"

    git_files = subprocess.check_output(
        ["git", "ls-tree", "-r", "HEAD", "--name-only"], encoding="UTF-8"
    ).split("\n")

    with tarfile.open(source_archived_file, "w:gz") as tar:
        for file in git_files:
            if len(file) > 0:
                tar.add(file)

    # Upload archive to GCS bucket
    source_archived_file_gcs = upload_file(
        local_file_path=f"{source_archived_file}",
        remote_file_path="/".join(
            [staging_bucket, "code_archives", source_archived_file]
        ),
    )

    print(f"Uploaded source code archive to {source_archived_file_gcs}")

    return source_archived_file_gcs