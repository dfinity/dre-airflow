"""
This sensor implements a custom Python sensor that invokes a callable
and finishes when the callable returns a Truish PokeReturnValue.
"""

import sys
import time
from typing import Any

from airflow.sensors.base import PokeReturnValue
from airflow.sensors.python import PythonSensor
from airflow.utils.context import Context


class CustomPokeSensor(PythonSensor):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def poke(self, context: Context) -> PokeReturnValue | bool:
        start = time.time()
        ret = super().poke(context)
        done = time.time()
        print(
            f"Time taken for custom poke  was {done - start} seconds.",
            file=sys.stderr,
        )
        return ret
