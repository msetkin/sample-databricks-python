
# Introduction

This educational repository is designed to demonstrate best engineering practices in a notebook-centric project using PySpark and Databricks. The focus is on building and managing DLT (Delta Live Tables) pipelines that process a publicly available New York taxi "data product".

## Project Structure

The repository is organized as follows:
- **.dbx**: Contains the project deployment endpoint configuration used by the Databricks deployment framework [DBX](https://dbx.readthedocs.io/en/latest/).
- **.github/workflows**: Holds GitHub Actions workflow files, namely the CI and CD pipelines.
- **conf**: Contains configuration files for the deployment of DLT pipelines used during the deployment of the pipelines by DBX (`deployment.yml`), and
and the physical parametrisation values for the tasks to be able to operate in cross-environmental deployments (`tasks/jinja-vars.yml`).

**python_helper**: This directory includes Python packages and modules used in the project.

**tests**: Contains test scripts and related files for the project. It includes:
- **integration**: Contains integration tests, including notebooks for testing.
- **unit**: Includes the unit tests for the project.
- **shared_test_instruments**: Provides shared utilities and data used in both unit and integration tests.

**workflows**: Contains deployable DLT pipelines in the form of the *.py notebooks, in particular, `NY_Taxi_DLT.py`.

**Other Files**:
- **.gitignore**: Specifies files and directories to be ignored by Git.
- **Makefile**: Contains local development process automation steps (linting, unit tests, etc).
- **pyproject.toml**: Configuration file for the Python project, specifying dependencies and project metadata.
- **README.md**: (this file) Provides an overview and documentation for the project.

## Current unit tests coverage
With the code and the unit tests provided in this repository, the current coverage is the following:
```
Name                                        Stmts   Miss Branch BrPart  Cover   Missing
---------------------------------------------------------------------------------------
python_helper/__init__.py                       0      0      0      0   100%
python_helper/autoloader.py                    22      9      4      0    50%   52-54, 57-67
python_helper/common.py                        67      5     18      3    88%   38->42, 49, 96, 106-109
python_helper/import_dlt.py                    22      0      0      0   100%
python_helper/pipelines/__init__.py             0      0      0      0   100%
python_helper/pipelines/ny_taxi.py             31      3      0      0    90%   69, 81-82
python_helper/pipelines/pipeline.py            10      2      0      0    80%   12, 18
python_helper/transformations/__init__.py       0      0      0      0   100%
python_helper/transformations/ny_taxi.py        4      0      0      0   100%
---------------------------------------------------------------------------------------
TOTAL                                         156     19     22      3    84%
```
While the test coverage ratio is rather a tool than a goal, the main point of the methodology implemented in this repository is that it provides the framework that allows to track the coverage in
an automated and transparent way for further individual decision making, balancing the required subjective coverage ratio targets and the expected bugs risks and impacts with the time available for such coverage implementation.



## Getting Started

To get started with this project, you'll need:

- A Databricks account
- Basic knowledge of PySpark and Databricks

1. Clone this repository.
2. Python 3.11 and Java>8 must be available for this project.
3. Configure your Databricks development environment by running `dbx configure`, `databricks configure`, etc according to the official documentation.
4. Edit/review the files `.dbx/project.json`, `conf/*`, `.github/workflows` and make sure the values provided there correspond to your Databricks environment.
5. When running the unit tests (either locally or as a part of CI), make sure the environment variable `LOCAL_MAVEN_REPO` is set to the URL of the local Artifactory 
(if there is no access to Maven Central).
6. When running deployments of integration tests locally using Makefile, make sure the environment variables `DATABRICKS_HOST` & `DATABRICKS_TOKEN` are set. 
In Makefile-triggered deployments, to override those variables, an `.env` file may be added to the root of the project with the same variables new values.
7. If a run of integration test is performed from the Github Actions, the secrets `DATABRICKS_HOST` & `DATABRICKS_TOKEN` must be set.
8. The CI&CD pipelines Github Actions can be triggered either upon certain actions over a corresponding request (opened, synchronize, reopened, unlocked), or manually from Github Actions page.
9. The local development automation (linting, unit-testing, deployment of integration tests) can be triggered manually using `make ...` and corresponding target specification.
10. When running the deployment of integration tests using a `make`, to avoid overwriting the Databricks workspace objects of other colleagues working at the same Databricks workspace,
make sure a local variable GITHUB_RUN_ID is set (either in bash or in `.env` file) to a certain unique value.


## Dataset

The New York taxi "data product" is a publicly available dataset that includes information about taxi trips in New York City, that is available in Databricks workspaces upon the path `/databricks-datasets/nyctaxi/sample/json/`. It is used in this project to demonstrate data processing techniques.


## Best Practices

This project utilises the following best practices:

- **Linting**: the tools `isort`, `black` and `ruff` are used to ensure code quality and consistency.
- **Static Analysis**: `mypy` is used for static type checking.
- **Unit Tests**: The `pytest` framework is used to validate individual components.
- **Integration Tests**: customly developed integration tests are used the production pipelines are deployed and run in the staging evnironment end-to-end without technical errors, including data consistency checks.

## Extrernal links

The content of this repository represents the slightly reworked compilation of the ideas derived from the following publicly available repositories:
- https://github.com/renardeinside/e2e-mlops-demo
- https://github.com/renardeinside/dbx-dlt-devops/
- https://github.com/renardeinside/databricks-repos-ci-demo
- https://github.com/alexott/dlt-files-in-repos-demo
- https://github.com/nicolattuso/DLT_Template



## Feedback

Feedback is welcome! In case you have any question, feel free to reach out the author of this project Mikhail Setkin (msetkin+data@gmail.com) or via Issues.

---

Feel free to explore the repository and experiment with the engineering best practices at Databricks projects. Happy coding!
