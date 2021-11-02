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

import argparse
import dataclasses
import datetime
import functools
import pathlib
import os
import nbformat
import subprocess
from pathlib import Path
from typing import List, Optional
from tabulate import tabulate
import tarfile
from utils import util

import execute_notebook_remote
from utils import NotebookProcessors


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def format_timedelta(delta: datetime.timedelta) -> str:
    """Formats a timedelta duration to [N days] %H:%M:%S format"""
    seconds = int(delta.total_seconds())

    secs_in_a_day = 86400
    secs_in_a_hour = 3600
    secs_in_a_min = 60

    days, seconds = divmod(seconds, secs_in_a_day)
    hours, seconds = divmod(seconds, secs_in_a_hour)
    minutes, seconds = divmod(seconds, secs_in_a_min)

    time_fmt = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    if days > 0:
        suffix = "s" if days > 1 else ""
        return f"{days} day{suffix} {time_fmt}"

    return time_fmt


@dataclasses.dataclass
class NotebookExecutionResult:
    notebook: str
    output_uri: str
    duration: datetime.timedelta
    is_pass: bool
    error_message: Optional[str]


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
    source_archived_file_gcs = util.upload_file(
        local_file_path=f"{source_archived_file}",
        gcs_uri="/".join([staging_bucket, "code_archives", source_archived_file]),
    )

    print(f"Uploaded source code archive to {source_archived_file_gcs}")

    return source_archived_file_gcs


def execute_notebook(
    notebook: str,
    staging_bucket: str,
    variable_project_id: str,
    variable_region: str,
) -> NotebookExecutionResult:
    print(f"Running notebook: {notebook}")

    # Read notebook
    with open(notebook) as f:
        nb = nbformat.read(f, as_version=4)

    # Create preprocessors
    remove_no_execute_cells_preprocessor = NotebookProcessors.RemoveNoExecuteCells()
    update_variables_preprocessor = NotebookProcessors.UpdateVariablesPreprocessor(
        replacement_map={
            "PROJECT_ID": variable_project_id,
            "REGION": variable_region,
        },
    )

    # Use no-execute preprocessor
    (
        nb,
        resources,
    ) = remove_no_execute_cells_preprocessor.preprocess(nb)

    (nb, resources) = update_variables_preprocessor.preprocess(nb, resources)

    with open(notebook, mode="w", encoding="utf-8") as new_file:
        nbformat.write(nb, new_file)

    result = NotebookExecutionResult(
        notebook=notebook,
        output_uri="",
        duration=datetime.timedelta(seconds=0),
        is_pass=False,
        error_message=None,
    )

    # TODO: Handle cases where multiple notebooks have the same name
    time_start = datetime.datetime.now()

    # Create paths
    artifacts_uri = "/".join(
        [
            staging_bucket,
            "executed_notebooks",
        ]
    )

    notebook_output_uri = "/".join([artifacts_uri, pathlib.Path(notebook).name])

    try:
        code_archive_uri = archive_code_and_upload(staging_bucket=staging_bucket)

        execute_notebook_remote(
            code_archive_uri=code_archive_uri,
            notebook_uri=notebook,
            notebook_output_uri=notebook_output_uri,
            container_uri="gcr.io/cloud-devrel-public-resources/python-samples-testing-docker:latest",
        )

        result.duration = datetime.datetime.now() - time_start
        result.is_pass = True
        print(f"{notebook} PASSED in {format_timedelta(result.duration)}.")
    except Exception as error:
        result.output_uri = notebook_output_uri
        result.duration = datetime.datetime.now() - time_start
        result.is_pass = False
        result.error_message = str(error)
        print(
            f"{notebook} FAILED in {format_timedelta(result.duration)}: {result.error_message}"
        )

    return result


def run_changed_notebooks(
    test_paths_file: str,
    base_branch: Optional[str],
    staging_bucket: str,
    variable_project_id: str,
    variable_region: str,
):
    """
    Run the notebooks that exist under the folders defined in the test_paths_file.
    It only runs notebooks that have differences from the Git base_branch.

    The executed notebooks are saved in the output_folder.

    Variables are also injected into the notebooks such as the variable_project_id and variable_region.

    Args:
        test_paths_file (str):
            Required. The new-line delimited file to folders and files that need checking.
            Folders are checked recursively.
        base_branch (str):
            Optional. If provided, only the files that have changed from the base_branch will be checked.
            If not provided, all files will be checked.
        staging_bucket (str):
            Required. The GCS staging bucket to write source code and output to.
        variable_project_id (str):
            Required. The value for PROJECT_ID to inject into notebooks.
        variable_region (str):
            Required. The value for REGION to inject into notebooks.
    """

    test_paths = []
    with open(test_paths_file) as file:
        lines = [line.strip() for line in file.readlines()]
        lines = [line for line in lines if len(line) > 0]
        test_paths = [line for line in lines]

    if len(test_paths) == 0:
        raise RuntimeError("No test folders found.")

    print(f"Checking folders: {test_paths}")

    # Find notebooks
    notebooks = []
    if base_branch:
        print(f"Looking for notebooks that changed from branch: {base_branch}")
        notebooks = subprocess.check_output(
            ["git", "diff", "--name-only", f"origin/{base_branch}..."] + test_paths
        )
    else:
        print(f"Looking for all notebooks.")
        notebooks = subprocess.check_output(["git", "ls-files"] + test_paths)

    notebooks = notebooks.decode("utf-8").split("\n")
    notebooks = [notebook for notebook in notebooks if notebook.endswith(".ipynb")]
    notebooks = [notebook for notebook in notebooks if len(notebook) > 0]
    notebooks = [notebook for notebook in notebooks if Path(notebook).exists()]

    notebook_execution_results: List[NotebookExecutionResult] = []

    if len(notebooks) > 0:
        print(f"Found {len(notebooks)} modified notebooks: {notebooks}")

        # if should_parallelize and len(notebooks) > 1:
        #     print(
        #         "Running notebooks in parallel, so no logs will be displayed. Please wait..."
        #     )
        #     with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        #         notebook_execution_results = list(
        #             executor.map(
        #                 functools.partial(
        #                     execute_notebook,
        #                     staging_bucket,
        #                     variable_project_id,
        #                     variable_region,
        #                     False,
        #                 ),
        #                 notebooks,
        #             )
        #         )
        # else:
        notebook_execution_results = [
            execute_notebook(
                notebook=notebook,
                staging_bucket=staging_bucket,
                variable_project_id=variable_project_id,
                variable_region=variable_region,
            )
            for notebook in notebooks
        ]
    else:
        print("No notebooks modified in this pull request.")

    print("\n=== RESULTS ===\n")

    notebooks_sorted = sorted(
        notebook_execution_results,
        key=lambda result: result.is_pass,
        reverse=True,
    )
    # Print results
    print(
        tabulate(
            [
                [
                    os.path.basename(os.path.normpath(result.notebook)),
                    "PASSED" if result.is_pass else "FAILED",
                    format_timedelta(result.duration),
                    result.error_message or "--",
                    result.output_uri,
                ]
                for result in notebooks_sorted
            ],
            headers=["file", "status", "duration", "error", "output_uri"],
        )
    )

    print("\n=== END RESULTS===\n")


parser = argparse.ArgumentParser(description="Run changed notebooks.")
parser.add_argument(
    "--test_paths_file",
    type=pathlib.Path,
    help="The path to the file that has newline-limited folders of notebooks that should be tested.",
    required=True,
)
parser.add_argument(
    "--base_branch",
    help="The base git branch to diff against to find changed files.",
    required=False,
)
parser.add_argument(
    "--output_folder",
    type=pathlib.Path,
    help="The path to the folder to store executed notebooks.",
    required=True,
)
parser.add_argument(
    "--variable_project_id",
    type=str,
    help="The GCP project id. This is used to inject a variable value into the notebook before running.",
    required=True,
)
parser.add_argument(
    "--variable_region",
    type=str,
    help="The GCP region. This is used to inject a variable value into the notebook before running.",
    required=True,
)

parser.add_argument(
    "--staging_bucket",
    type=str,
    help="The GCP directory for staging temporary files.",
    required=True,
)

args = parser.parse_args()
run_changed_notebooks(
    test_paths_file=args.test_paths_file,
    base_branch=args.base_branch,
    staging_bucket=args.staging_bucket,
    variable_project_id=args.variable_project_id,
    variable_region=args.variable_region,
)
