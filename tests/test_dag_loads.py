import os

import pytest

from airflow.models import DagBag


def get_import_errors() -> list[tuple[None, None] | tuple[str, str]]:
    """
    Generate a tuple for import errors in the dag bag
    """

    dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path: str) -> str:
        return os.path.relpath(path, os.path.dirname(__file__))

    # prepend "(None,None)" to ensure that a test object is always created even if it's
    # a no op.
    return [(None, None)] + [
        (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
    ]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path: str | None, rv: str | None) -> None:
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")
