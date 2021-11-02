from google.protobuf import duration_pb2
from yaml.loader import FullLoader

import google.auth
from google.cloud.devtools import cloudbuild_v1
from google.cloud.devtools.cloudbuild_v1.types import Source, StorageSource

import yaml

from google.cloud.aiplatform import utils
from google.api_core import operation

CLOUD_BUILD_FILEPATH = ".cloud-build/notebook-execution-test-cloudbuild-single.yaml"
TIMEOUT_IN_SECONDS = 86400


def execute_notebook_remote(
    code_archive_uri: str,
    notebook_uri: str,
    notebook_output_uri: str,
    container_uri: str,
) -> operation.Operation:
    """Create and execute a simple Google Cloud Build configuration,
    print the in-progress status and print the completed status."""

    # Authorize the client with Google defaults
    credentials, project_id = google.auth.default()
    client = cloudbuild_v1.services.cloud_build.CloudBuildClient()

    build = cloudbuild_v1.Build()

    # The following build steps will output "hello world"
    # For more information on build configuration, see
    # https://cloud.google.com/build/docs/configuring-builds/create-basic-configuration
    cloudbuild_config = yaml.load(open(CLOUD_BUILD_FILEPATH), Loader=FullLoader)

    substitutions = {
        "_PYTHON_IMAGE": container_uri,
        "_NOTEBOOK_GCS_URI": notebook_uri,
        "_NOTEBOOK_OUTPUT_GCS_URI": notebook_output_uri,
    }

    (
        source_archived_file_gcs_bucket,
        source_archived_file_gcs_object,
    ) = utils.extract_bucket_and_prefix_from_gcs_path(code_archive_uri)

    build.source = Source(
        storage_source=StorageSource(
            bucket=source_archived_file_gcs_bucket,
            object_=source_archived_file_gcs_object,
        )
    )
    build.steps = cloudbuild_config["steps"]

    build.substitutions = substitutions

    build.timeout = duration_pb2.Duration(seconds=TIMEOUT_IN_SECONDS)

    operation = client.create_build(project_id=project_id, build=build)
    # Print the in-progress operation
    # print("IN PROGRESS:")
    # print(operation.metadata)

    result = operation.result()
    # Print the completed status
    print("RESULT:", result.status)
    return operation


# # project = "python-docs-samples-tests"
# staging_bucket = "gs://ivanmkc-test2/notebooks"
# # destination_gcs_folder = staging_bucket + "/notebooks"
# output_uri = staging_bucket + "/notebooks/output/output.ipynb"
# code_gcs_uri = staging_bucket + "/code_archives"

# local_notebook_path = "notebooks/official/custom/test.ipynb"

# notebook_uri = "gs://ivanmkc-test2/cloudbuild-test/test.ipynb"

# from utils import util

# code_archive_uri = util.archive_code_and_upload(staging_bucket=staging_bucket)

# execute_notebook_remote(
#     code_archive_uri=code_archive_uri,
#     notebook_uri=local_notebook_path,
#     notebook_output_uri=output_uri,
#     container_uri="gcr.io/cloud-devrel-public-resources/python-samples-testing-docker:latest",
# )
