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
import os
import errno
import papermill as pm
import shutil

from utils import util
from google.cloud.aiplatform import utils

# This script is used to execute a notebook and write out the output notebook.
# The replaces calling the nbconvert via command-line, which doesn't write the output notebook correctly when there are errors during execution.


def download_file(bucket_name: str, blob_name: str, destination_file: str) -> str:
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.download_to_filename(filename=destination_file)


def execute_notebook(
    notebook_source: str,
    output_file_or_uri: str,
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
        if output_file_or_uri.startswith("gs://"):
            # Upload to GCS path
            util.upload_file(notebook_source, remote_file_path=output_file_or_uri)

            print(f"Uploaded output to: {output_file_or_uri}")
        else:
            # Create directories if they don't exist
            if not os.path.exists(os.path.dirname(output_file_or_uri)):
                try:
                    os.makedirs(os.path.dirname(output_file_or_uri))
                except OSError as exc:  # Guard against race condition
                    if exc.errno != errno.EEXIST:
                        raise

            print(f"Writing output to: {output_file_or_uri}")
            shutil.move(notebook_source, output_file_or_uri)


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
#     output_file_or_uri=args.output_folder_or_uri,
#     should_log_output=True,
# )
