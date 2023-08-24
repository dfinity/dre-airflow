"""
Test operators.
"""


from airflow.models.baseoperator import BaseOperator


class TestTask(BaseOperator):
    def execute(*args, **kwargs) -> None:  # type:ignore
        print(
            "Test task successfully executed with args %s kwargs %s." % (args, kwargs)
        )
