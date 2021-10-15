import toml


def test_pyproject_toml_matches_version():
    import mojap_airflow_tools

    with open("pyproject.toml") as f:
        proj = toml.load(f)
    assert mojap_airflow_tools.__version__ == proj["tool"]["poetry"]["version"]
