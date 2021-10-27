from google.cloud import aiplatform

from google.cloud.aiplatform import utils

from utils.NotebookProcessors import (
    RemoveNoExecuteCells,
    UpdateVariablesPreprocessor,
)

# CONTAINER_URI = (
#     "gcr.io/cloud-devrel-public-resources/python-samples-testing-docker:latest"
# )
CONTAINER_URI = "us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu.0-23:latest"


def run_notebook_remote(
    package_gcs_uri: str,
    python_module_name: str,
    container_uri: str,
    notebook_uri: str,
    output_uri: str,
):
    notebook_name = "notebook_execution"
    job = aiplatform.CustomPythonPackageTrainingJob(
        display_name=notebook_name,
        python_package_gcs_uri=package_gcs_uri,
        python_module_name=python_module_name,
        container_uri=container_uri,
    )

    job.run(
        args=[
            "--notebook_source",
            notebook_uri,
            "--output_folder_or_uri",
            output_uri,
        ],
        environment_variables={
            "IS_TESTING": 1,
        },
        replica_count=1,
        sync=True,
    )


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
notebook_uri = utils._timestamped_copy_to_gcs(
    local_file_path=destination_notebook_path, gcs_dir=destination_gcs_folder
)

aiplatform.init(project=project, staging_bucket=staging_bucket)

# Read requirements.txt
requirements = []
with open(".cloud-build/requirements.txt") as file:
    requirements = file.readlines()
    requirements = [line.rstrip() for line in requirements]

# Create packager
python_packager = utils.source_utils._TrainingScriptPythonPackager(
    script_path=".cloud-build",
    task_module_name="ExecuteNotebook",
    requirements=requirements,
)

# Package and upload to GCS
package_gcs_uri = python_packager.package_and_copy_to_gcs(
    gcs_staging_dir=staging_bucket,
    project=project,
)

python_module_name = python_packager.module_name

run_notebook_remote(
    package_gcs_uri=package_gcs_uri,
    python_module_name=python_module_name,
    container_uri=CONTAINER_URI,
    notebook_uri=notebook_uri,
    output_uri=output_uri,
)
