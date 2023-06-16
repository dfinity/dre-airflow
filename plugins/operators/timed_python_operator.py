"""
This operator demoes a wrapper of the standard Python operator,
which can run an arbitrary Python callable.
"""

import sys
import time
from typing import Any

from airflow.operators.python import PythonOperator
from airflow.utils.context import Context


class TimedPythonOperator(PythonOperator):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        start = time.time()
        super().execute(context)
        done = time.time()
        print(
            f"Time taken for PythonOperator was {done - start} seconds.",
            file=sys.stderr,
        )
