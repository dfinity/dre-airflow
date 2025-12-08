import json
from typing import Any, Optional, TypedDict, cast

import requests


class MsdTarget(TypedDict):
    node_id: str
    ic_name: str
    targets: list[str]
    subnet_id: Optional[str]
    dc_id: str
    operator_id: str
    node_provider_id: str
    jobs: list[str]
    custom_labels: dict[str, Any]
    name: str
    is_api_bn: bool
    domain: Optional[str]


class MsdApi:
    msd_url: str
    targets: list[MsdTarget]

    def __init__(self, msd_url: str):
        self.msd_url = msd_url

        try:
            response = requests.get(msd_url, timeout=25)

            response.raise_for_status()
            data = response.json()
            self.targets = cast(list[MsdTarget], data)

        except requests.exceptions.HTTPError as http_err:
            # Captures 4xx and 5xx errors specifically
            print(
                f"HTTP error occurred: {http_err} - Status Code: {response.status_code}"
            )
            print(
                f"Response Body: {response.text}"
            )  # Log the body to see server error details
            raise

        except requests.exceptions.RequestException as err:
            print(f"Network or other error occurred: {err}")
            raise
        except Exception as e:
            print(f"Other error: {e}")
            raise

        print("Successfully synced with msd")

    def check_api_bn_health(self, node_id: str) -> bool:
        node = next(filter(lambda n: n["node_id"] == node_id, self.targets), None)

        if node is None:
            print("Node", node_id, "not found in multiservice discovery")
            return False

        print("Node entry in multiservice discovery:", json.dumps(node))

        if not node["is_api_bn"]:
            print(
                "Node",
                node_id,
                "is not an api boundary node",
            )
            return False

        domain = node["domain"]
        if domain is None:
            print(
                "Node",
                node_id,
                "Doesn't have a domain specified. Will use ipv6, which may not yield"
                " all of the errors",
            )
            url = node["targets"][0].split(":")[0]
            domain = f"{url}:443"

        full_domain = f"https://{domain}/health"
        print("Will use the following domain:", full_domain)

        try:
            response = requests.get(full_domain, timeout=25)

            response.raise_for_status()

            print("Received status:", response.status_code)
            if response.status_code != 204:
                print("Didn't receive status 204")
                return False

        except requests.exceptions.HTTPError as http_err:
            # Captures 4xx and 5xx errors specifically
            print(
                f"HTTP error occurred: {http_err} - Status Code: {response.status_code}"
            )
            print(
                f"Response Body: {response.text}"
            )  # Log the body to see server error details
            return False

        except requests.exceptions.RequestException as err:
            print(f"Network or other error occurred: {err}")
            return False
        except Exception as e:
            print(f"Other error: {e}")
            return False

        print("Node", node_id, "deemed healthy")
        return True
