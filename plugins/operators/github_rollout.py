from __future__ import annotations

import datetime
import re
from typing import Any, Sequence, cast

import requests
import yaml
from dfinity.rollout_types import Releases

from airflow.models import BaseOperator


class GetReleases(BaseOperator):
    """
    Gets all the releases from Github (or any URL).
    """

    template_fields: Sequence[str] = ["release_index_url"]

    def __init__(
        self,
        *,
        release_index_url: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.release_index_url = release_index_url

    def execute(self, context: Any) -> Releases:
        r = requests.get(self.release_index_url)
        r.raise_for_status()
        data = r.content
        releases = cast(Releases, yaml.safe_load(data)["releases"])
        for release in releases:
            prospective_date = re.search(
                "([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2})",
                release["rc_name"],
            )
            if prospective_date:
                release["rc_date"] = datetime.datetime.strptime(
                    prospective_date.group(1), "%Y-%m-%d_%H-%M"
                )
            else:
                raise ValueError(
                    f"Release item {release['rc_name']} has no valid date/time in name"
                )
        return releases


if __name__ == "__main__":
    import sys

    release_index_url = sys.argv[1]
    kn = GetReleases(
        task_id="x",
        release_index_url=release_index_url,
    )
    kn.execute({})
