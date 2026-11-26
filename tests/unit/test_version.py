"""The version is written in two places; they must agree."""

import tomllib
from pathlib import Path

import etchdb


def test_runtime_version_matches_package_metadata():
    pyproject = Path(__file__).parents[2] / "pyproject.toml"
    declared = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert etchdb.__version__ == declared
