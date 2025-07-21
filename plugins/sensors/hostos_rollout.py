from dfinity import dre, ic_types, rollout_types
from dfinity import prom_api as prom
from operators.hostos_rollout import DagParams

from airflow.hooks.subprocess import SubprocessHook


def has_network_adopted_hostos_revision(
    network: ic_types.ICNetwork,
    params: DagParams,
) -> bool:
    simulate = params["simulate"]
    git_revision = params["git_revision"]

    if simulate:
        print(f"Pretending that {git_revision} is elected (simulation on)")
        return True

    if dre.DRE(network, SubprocessHook()).is_hostos_version_blessed(git_revision):
        print("Revision is elected.  We can proceed.")
        return True

    print("Revision is not yet elected.  Waiting.")
    return False


def have_hostos_nodes_adopted_revision(
    nodes: list[rollout_types.NodeInfo],
    network: ic_types.ICNetwork,
    params: DagParams,
) -> bool:
    git_revision = params["git_revision"]
    simulate = params["simulate"]

    print(
        f"Waiting for specified HostOS nodes to have adopted revision {git_revision}."
    )

    joined = "|".join(n["node_id"] for n in nodes)
    query = "sum(hostos_version{" + f'ic_node=~"{joined}"' + "}) by (version)"
    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)
    if len(res) == 1 and res[0]["metric"]["version"] == git_revision:
        current_node_count = int(res[0]["value"])
        if current_node_count >= len(nodes):
            print(
                "All %s HostOS nodes have updated to revision %s."
                % (current_node_count, git_revision)
            )
            return True
        else:
            print(
                "The updated HostOS node count is %d but %d is expected; waiting."
                % (current_node_count, len(nodes))
            )
    if res:
        print(
            f"Upgrade of HostOS nodes to {git_revision}"
            " is not complete yet.  From Prometheus:"
        )
        for r in res:
            print(r)
    else:
        print(
            f"Upgrade has not begun yet -- Prometheus show no results for the"
            f" specified HostOS nodes {nodes}."
        )

    if simulate:
        print("This is a simulation, pretending everything went well.")
        return True

    return False


def are_hostos_nodes_healthy(
    nodes: list[rollout_types.NodeInfo],
    network: ic_types.ICNetwork,
    params: DagParams,
) -> bool:
    """
    Check for 15 minutes of no alerts (pending or firing) on any of the HostOS nodes.

    Return True if no alerts, False otherwise.
    """
    joined = "|".join(n["node_id"] for n in nodes)

    print("Waiting for alerts on HostOS nodes to subside.")
    query = """
        sum_over_time(
            ALERTS{
                ic_node=~"%s",
                severity=~"page|notify"
            }[15m]
        )""" % (joined,)

    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)
    if len(res) > 0:
        print("There are still Prometheus alerts on the subnet:")
        for r in res:
            print(r)

        if params["simulate"]:
            print("Returning as success anyway because we are in a simulation.")
            return True

        return False

    print("There are no more alerts on HostOS nodes.")
    return True


if __name__ == "__main__":
    network = ic_types.ICNetwork(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        [
            "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query",
        ],
        80,
        "dfinity.ic_admin.mainnet.proposer_key_file",
    )
    params: DagParams = {
        "simulate": False,
        "git_revision": "143a635e2af0f574e1ea0f795f8754dfbd86c0c0",
        "plan": "",
    }
    has_network_adopted_hostos_revision(network, params)
