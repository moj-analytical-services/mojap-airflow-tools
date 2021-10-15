# mojap-airflow-tools

A few wrappers and tools to use Airflow on the Analytical Platform.

## How to install

To install, run:

```
pip install mojap-airflow-tools
```

## How to create a new release

To create a new release:

1.  Update the package version in `pyproject.toml`.
2.  Update dependencies by running:

        poetry update

3.  Create a release in GitHub.
4.  Checkout the release tag.
5.  Build the package by running:

        poetry build

6.  Publish the package by running:

        poetry publish

## How to update the release used by Airflow

To update the release used by Airflow, update the value of
`config.data-engineering-airflow.mwaa.requirements` in the `Pulumi.*.yaml` file
for the relevant environment in
[data-engineering-airflow](https://github.com/ministryofjustice/data-engineering-airflow).

You should update and test the requirements in the `dev` environment before
deploying to `prod`.
