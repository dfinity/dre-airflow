"""
Test operators.
"""


from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class TestTask(BaseOperator):
    def execute(context: Context) -> None:  # type:ignore
        print("Test task successfully executed.")


class TestTask2(BaseOperator):
    def execute(context: Context) -> None:  # type:ignore
        print("Test task 2 successfully executed.")
