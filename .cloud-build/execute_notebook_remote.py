from google.cloud import aiplatform

from google.cloud.aiplatform import utils

from utils.NotebookProcessors import (
    RemoveNoExecuteCells,
    UpdateVariablesPreprocessor,
)

CONTAINER_URI = (
    "gcr.io/cloud-devrel-public-resources/python-samples-testing-docker:latest"
)
# CONTAINER_URI = "us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest"


import google.auth
from google.cloud.devtools import cloudbuild_v1

import yaml


def run_notebook_remote(
    container_uri: str,
    notebook_uri: str,
    output_uri: str,
):
    """Create and execute a simple Google Cloud Build configuration,
    print the in-progress status and print the completed status."""

    # Authorize the client with Google defaults
    credentials, project_id = google.auth.default()
    client = cloudbuild_v1.services.cloud_build.CloudBuildClient()

    build = cloudbuild_v1.Build()

    # The following build steps will output "hello world"
    # For more information on build configuration, see
    # https://cloud.google.com/build/docs/configuring-builds/create-basic-configuration
    cloudbuild_config = yaml.load(
        open(".cloud-build/notebook-execution-test-cloudbuild-single.yaml")
    )

    # build.steps = [
    #     {"name": "ubuntu", "entrypoint": "bash", "args": ["-c", "echo hello world"]}
    # ]

    build.steps = cloudbuild_config["steps"]

    build.substitutions = {
        "_PYTHON_IMAGE": container_uri,
        "_NOTEBOOK_GCS_URI": notebook_uri,
        "_OUTPUT_GCS_URI": output_uri,
        "_BASE_BRANCH": "master",
    }

    # build.timeout = "86400s"

    operation = client.create_build(project_id=project_id, build=build)
    # Print the in-progress operation
    print("IN PROGRESS:")
    print(operation.metadata)

    result = operation.result()
    # Print the completed status
    print("RESULT:", result.status)


# def run_notebook_remote(
#     package_gcs_uri: str,
#     python_module_name: str,
#     container_uri: str,
#     notebook_uri: str,
#     output_uri: str,
# ):
#     notebook_name = "notebook_execution"
#     job = aiplatform.CustomPythonPackageTrainingJob(
#         display_name=notebook_name,
#         python_package_gcs_uri=package_gcs_uri,
#         python_module_name=python_module_name,
#         container_uri=container_uri,
#     )

#     job.run(
#         args=[
#             "--notebook_source",
#             notebook_uri,
#             "--output_folder_or_uri",
#             output_uri,
#         ],
#         environment_variables={
#             "IS_TESTING": 1,
#         },
#         replica_count=1,
#         sync=True,
#     )


def process_notebook(
    notebook_path: str, destination_notebook_path: str, replacement_map: dict = {}
):
    import nbformat

    remove_no_execute_cells_preprocessor = RemoveNoExecuteCells()
    update_variables_preprocessor = UpdateVariablesPreprocessor(
        replacement_map=replacement_map
    )

    # Read notebook
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    # Use no-execute preprocessor
    (
        nb,
        resources,
    ) = remove_no_execute_cells_preprocessor.preprocess(nb)

    (nb, resources) = update_variables_preprocessor.preprocess(nb, resources)

    # print(f"Staging modified notebook to: {staging_file_path}")
    with open(destination_notebook_path, mode="w", encoding="utf-8") as f:
        nbformat.write(nb, f)


project = "python-docs-samples-tests"
staging_bucket = "gs://ivanmkc-test2/notebooks"
destination_gcs_folder = staging_bucket + "/notebooks"
output_uri = staging_bucket + "/notebooks/output"

notebook_path = "notebooks/official/custom/custom-tabular-bq-managed-dataset.ipynb"

# Preprocess notebook
destination_notebook_path = "debug.ipynb"
variable_project_id = "python-docs-sample-tests"
variable_region = "us-central1"

# Upload notebook
process_notebook(
    notebook_path=notebook_path,
    destination_notebook_path=destination_notebook_path,
    replacement_map={
        "PROJECT_ID": variable_project_id,
        "REGION": variable_region,
    },
)

# Copy notebook to GCS
notebook_uri = utils._timestamped_copy_to_gcs(
    local_file_path=destination_notebook_path, gcs_dir=destination_gcs_folder
)

run_notebook_remote(
    # package_gcs_uri=package_gcs_uri,
    # python_module_name=python_module_name,
    container_uri=CONTAINER_URI,
    notebook_uri=notebook_uri,
    output_uri=output_uri,
)
