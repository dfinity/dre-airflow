"""
Minimal Prometheus client.
"""

import urllib.parse
from typing import TypedDict, cast

import requests  # type:ignore


class PrometheusVectorResultEntry(TypedDict):
    metric: dict[str, str]
    value: str
    timestamp: float


def query_prometheus_servers(
    url_list: list[str], query: str
) -> list[PrometheusVectorResultEntry]:
    for n, url in enumerate(url_list):
        try:
            r = requests.get(
                url + "?" + urllib.parse.urlencode({"query": query}),
            )
            r.raise_for_status()
            resp = r.json()
            assert resp["status"] == "success", resp
            assert resp["data"]["resultType"] == "vector", resp["data"]
            res: list[PrometheusVectorResultEntry] = []
            for data in resp["data"]["result"]:
                m = cast(PrometheusVectorResultEntry, data)
                m["timestamp"], m["value"] = m["value"]  # type: ignore
                res.append(m)
            break
        except Exception:
            if n == len(url_list) - 1:
                raise
            continue
    return res
