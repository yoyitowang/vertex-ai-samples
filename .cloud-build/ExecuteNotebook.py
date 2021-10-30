#!/usr/bin/env python
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import nbformat
import os
import errno
from typing import Dict, Optional, Tuple
import papermill as pm
import shutil

from google.cloud.aiplatform import utils
from google.cloud import storage
from google.auth import credentials as auth_credentials

# This script is used to execute a notebook and write out the output notebook.
# The replaces calling the nbconvert via command-line, which doesn't write the output notebook correctly when there are errors during execution.

STAGING_FOLDER = "staging"


def upload_file(
    local_file_path: str,
    gcs_dir: str,
    project: Optional[str] = None,
    credentials: Optional[auth_credentials.Credentials] = None,
) -> str:
    """Copies a local file to a GCS path."""

    # TODO(b/171202993) add user agent
    gcs_bucket, blob_path = utils.extract_bucket_and_prefix_from_gcs_path(gcs_dir)
    client = storage.Client(project=project, credentials=credentials)
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_file_path)

    gcs_path = "".join(["gs://", "/".join([blob.bucket.name, blob.name])])
    return gcs_path


def download_file(bucket_name: str, blob_name: str, destination_file: str) -> str:
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.download_to_filename(filename=destination_file)


def execute_notebook(
    notebook_source: str,
    output_file_folder: str,
    should_log_output: bool,
):
    file_name = os.path.basename(os.path.normpath(notebook_source))

    # Download notebook if it's a GCS URI
    if notebook_source.startswith("gs://"):
        # Extract uri components
        bucket_name, prefix = utils.extract_bucket_and_prefix_from_gcs_path(
            notebook_source
        )

        # Download remote notebook to local file system
        notebook_source = file_name
        download_file(
            bucket_name=bucket_name, blob_name=prefix, destination_file=notebook_source
        )

    has_error = False

    # Execute notebook
    try:
        # Execute notebook
        pm.execute_notebook(
            input_path=notebook_source,
            output_path=notebook_source,
            progress_bar=should_log_output,
            request_save_on_cell_execute=should_log_output,
            log_output=should_log_output,
            stdout_file=sys.stdout if should_log_output else None,
            stderr_file=sys.stderr if should_log_output else None,
        )
    except Exception:
        # print(f"Error executing the notebook: {notebook_file_path}.\n\n")
        has_error = True

        raise

    finally:
        # # Clear env
        # if env_name is not None:
        #     shutil.rmtree(path=env_name)

        # Copy executed notebook
        output_file_path = os.path.join(
            output_file_folder, "failure" if has_error else "success", file_name
        )

        if output_file_path.startswith("gs://"):
            # Upload to GCS path
            upload_file(notebook_source, gcs_dir=output_file_path)

            print(f"Uploaded output to: {output_file_path}")
        else:
            # Create directories if they don't exist
            if not os.path.exists(os.path.dirname(output_file_path)):
                try:
                    os.makedirs(os.path.dirname(output_file_path))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            print(f"Writing output to: {output_file_path}")
            shutil.move(notebook_source, output_file_path)


# import argparse

# parser = argparse.ArgumentParser(description="Run changed notebooks.")
# parser.add_argument(
#     "--notebook_source",
#     type=str,
#     help="Local filepath or GCS URI to notebook.",
#     required=True,
# )
# parser.add_argument(
#     "--output_folder_or_uri",
#     type=str,
#     help="Local folder or GCS URI to save executed notebook to.",
#     required=True,
# )

# args = parser.parse_args()
# execute_notebook(
#     notebook_source=args.notebook_source,
#     output_file_folder=args.output_folder_or_uri,
#     should_log_output=True,
# )
