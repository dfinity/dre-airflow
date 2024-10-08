"""
Minimal Prometheus client.
"""

import urllib.parse
from typing import TypedDict, cast

import requests


class PrometheusVectorResultEntry(TypedDict):
    metric: dict[str, str]
    value: float
    timestamp: float


def query_prometheus_servers(
    url_list: list[str], query: str
) -> list[PrometheusVectorResultEntry]:
    post_data = urllib.parse.urlencode({"query": query})
    for n, url in enumerate(url_list):
        try:
            r = requests.post(
                url,
                timeout=25,
                data=post_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            r.raise_for_status()
            resp = r.json()
            assert resp["status"] == "success", resp
            assert resp["data"]["resultType"] == "vector", resp["data"]
            res: list[PrometheusVectorResultEntry] = []
            for data in resp["data"]["result"]:
                m = cast(PrometheusVectorResultEntry, data)
                m["timestamp"], m["value"] = m["value"]  # type: ignore
                m["value"] = float(m["value"])
                res.append(m)
            break
        except Exception:
            if n == len(url_list) - 1:
                raise
            continue
    return res


if __name__ == "__main__":
    import pprint

    subnet_id = "pae4o-o6dxf-xki7q-ezclx-znyd6-fnk6w-vkv5z-5lfwh-xym2i-otrrw-fqe"
    query = "sum(ic_replica_info{" + f'ic_subnet="{subnet_id}"' + "}) by (ic_subnet)"
    res = query_prometheus_servers(
        ["https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query"],
        query,
    )
    pprint.pprint(res)
