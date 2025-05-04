ifneq (,$(wildcard .env))
	include .env
	export
endif

unit-test:
	pip install -e .[local_dev,linting,testing,security]
	PYTHONPATH=. pytest tests/unit* --cov-report term-missing

lint:
	isort python_helper tests workflows
	black python_helper tests workflows
	ruff check python_helper tests
	mypy python_helper tests --explicit-package-bases --check-untyped-defs

deploy-int-tests:
	@if [ "$(ENV)" != "stage" ]; then \
		echo "Error: ENV may be only set to 'stage' here"; \
		exit 1; \
	fi
	@if [ -z "$(GITHUB_RUN_ID)" ]; then \
        echo "Error: GITHUB_RUN_ID must be set"; \
        exit 1; \
    fi

	pip install build
	pip install .[deploy]
	python -m build --wheel
	
	databricks fs ls dbfs:/FileStore/sample-databricks-python/conf/stage/${GITHUB_RUN_ID}\
		&& databricks fs rm -r dbfs:/FileStore/sample-databricks-python/conf/stage/${GITHUB_RUN_ID}\
		|| echo "No '${GITHUB_RUN_ID}' folder found in dbfs:/FileStore/sample-databricks-python/conf/stage/, skipping deletion."

	databricks fs ls dbfs:/FileStore/sample-databricks-python/dist/stage/${GITHUB_RUN_ID}\
		&& databricks fs rm -r dbfs:/FileStore/sample-databricks-python/dist/stage/${GITHUB_RUN_ID}\
		|| echo "No '${GITHUB_RUN_ID}' folder found in dbfs:/FileStore/sample-databricks-python/dist/stage/, skipping deletion."

	
	databricks fs mkdirs dbfs:/FileStore/sample-databricks-python/conf/stage/${GITHUB_RUN_ID}/
	databricks fs mkdirs dbfs:/FileStore/sample-databricks-python/dist/stage/${GITHUB_RUN_ID}/

	databricks fs cp --overwrite \
						conf/tasks/jinja-vars.yml \
						dbfs:/FileStore/sample-databricks-python/conf/stage/${GITHUB_RUN_ID}/
	
	databricks fs cp --overwrite \
						dist/python_helper*.whl \
						dbfs:/FileStore/sample-databricks-python/dist/stage/${GITHUB_RUN_ID}/

	@{ databricks repos get /Repos/project-workspace-stage/${GITHUB_RUN_ID}\
		&& echo "Repo 'project-workspace-stage/${GITHUB_RUN_ID}' already exists." || \
		{ echo "Repo 'project-workspace-stage/${GITHUB_RUN_ID}' is not found, creating it:" && databricks repos create \
		    https://github.com/msetkin/sample-databricks-python.git gitHub\
		    --path /Repos/project-workspace-stage/${GITHUB_RUN_ID}; }; }

	MY_BRANCH=$$(git symbolic-ref --short HEAD); \
	databricks repos update /Repos/project-workspace-stage/${GITHUB_RUN_ID} \
		--branch $$MY_BRANCH

	dbx deploy ny-taxi-dlt-workflow-int-test-${GITHUB_RUN_ID} --environment=stage --jinja-variables-file=./conf/tasks/jinja-vars.yml
	dbx deploy run-int-test-${GITHUB_RUN_ID} --environment=stage --jinja-variables-file=./conf/tasks/jinja-vars.yml