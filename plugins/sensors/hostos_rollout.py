from dfinity import dre, ic_types, rollout_types
from dfinity import prom_api as prom
from operators.hostos_rollout import DagParams

from airflow.hooks.subprocess import SubprocessHook
from airflow.models.taskinstance import TaskInstance
from airflow.sensors.base import PokeReturnValue
from airflow.utils.xcom import XCOM_RETURN_KEY


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
    ti: TaskInstance,
) -> PokeReturnValue:
    git_revision = params["git_revision"]
    simulate = params["simulate"]

    print(
        f"Waiting for specified HostOS nodes to have adopted revision {git_revision}."
    )

    joined = "|".join(n["node_id"] for n in nodes)
    query = "hostos_version{" + f'ic_node=~"{joined}"' + "}"
    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)

    nodes_versions: dict[str, str | None] = {
        r["metric"]["ic_node"]: r["metric"]["version"] for r in res
    }
    for node in nodes:
        if node["node_id"] not in nodes_versions:
            nodes_versions[node["node_id"]] = None

    upgradeds: rollout_types.NodeUpgradeStatuses = {
        k: "upgraded"
        if v == params["git_revision"]
        else "AWOL"
        if v is None
        else "pending"
        for k, v in nodes_versions.items()
    }
    ti.xcom_push(XCOM_RETURN_KEY, upgradeds)

    not_done = [(k, v) for k, v in upgradeds.items() if v == "AWOL" or v == "pending"]

    if len(not_done) > 0:
        print(
            f"Upgrade of HostOS nodes to {git_revision}"
            " is not complete yet.  The following nodes are not done:"
        )
        for n, s in not_done:
            print(f"* Node {n} is {s}")
    else:
        print(
            "All %s HostOS nodes have updated to revision %s."
            % (len(upgradeds), git_revision)
        )
        return PokeReturnValue(True)

    if simulate:
        print("This is a simulation, pretending everything went well.")
        return PokeReturnValue(True)

    return PokeReturnValue(False)


def are_hostos_nodes_healthy(
    nodes: list[rollout_types.NodeInfo],
    network: ic_types.ICNetwork,
    params: DagParams,
    ti: TaskInstance,
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

    alerting: rollout_types.NodeAlertStatuses = {n["node_id"]: "unknown" for n in nodes}

    print("::group::Querying Prometheus servers")
    print(query)
    print("::endgroup::")
    res = prom.query_prometheus_servers(network.prometheus_urls, query)
    if len(res) > 0:
        print("There are still Prometheus alerts related to these HostOS nodes:")
        for r in res:
            alerting[r["metric"]["ic_node"]] = "alerting"
            print(r)

        # Save alerting status of nodes.
        ti.xcom_push(XCOM_RETURN_KEY, alerting)

        if params["simulate"]:
            print("Returning as success anyway because we are in a simulation.")
            return True
        else:
            return False

    else:
        print("There are no more alerts on HostOS nodes.")
        alerting = {k: "OK" for k in alerting}
        # Save all-OK alerting status.
        ti.xcom_push(XCOM_RETURN_KEY, alerting)
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
    # has_network_adopted_hostos_revision(network, params)
    nodes = [
        {
            "node_id": n,
            "node_provider_id": "meh",
            "subnet_id": "meh",
            "dc_id": "meh",
            "status": "Healthy",
        }
        for n in [
            "2nehn-z42p7-36s7j-qqcz6-sy5rb-euhte-necpl-egqsv-hdoph-gx7qd-6ae",
            "3jol6-6a6dv-ysedb-5qlxc-baclw-ub23p-d324m-7sfus-bkbnk-jlh6b-jqe",
            "4lqcl-tzzs3-jztya-ro7ak-ew5ue-l3tlc-5cxk3-4jt7m-z66st-ad5ld-aae",
            "4mm7j-dmeng-7ib4h-yvt3c-g5ed6-i5yar-rrc6w-rp7tk-ej3by-gilvv-kae",
            "4whgz-3ksfr-4rroh-dluex-fptua-mrxjo-uzmgg-lvuws-oe72b-aynhj-fae",
            "5ei6o-6mx3z-zag6b-ubdsz-odxr3-ytixv-rrnv3-khmkc-4tuni-ber6p-mae",
            "5irn3-rlxhd-lp3cf-caeka-xu2ph-54ceg-afnwr-atwlj-a33ip-odxsy-iae",
            "5jqmb-vz4s4-tp7g3-v22ai-lqxgz-fonyb-6mmlk-xsmfs-md3u4-ovqek-zqe",
            "5nkgj-r45dd-5qojg-ecnqn-ig7uo-hqndz-v5udk-gxygg-bfigl-r6lh3-jae",
            "5v4on-bsceg-rdgxe-zcqqf-l5wnq-fpxw7-x3ktj-3x4fs-o2cny-uzhor-vqe",
            "5wzcz-mfner-c5wor-3liei-7mpao-vkult-zdds6-6n3rg-42ksv-6zzqt-vae",
            "5zldm-3rijx-6pp6q-twp65-b34xz-6iitk-kqzov-flbdh-ekkx3-3an4a-eqe",
            "5zqhj-66qn7-he3p5-76gnj-hlsvl-ajlt5-k356i-fgm4m-5it4c-6m5ag-7qe",
            "62qwz-bp4wo-tdrcm-vincq-mdhz3-g6bfq-axwcr-vog76-fe4f3-lvepx-bae",
            "64fwz-oyjy7-3r6wn-n5bhn-534fp-bjw2l-b5gel-gdrt4-egsun-6tkiw-cae",
            "6bbgn-jvexd-25pja-q5wgj-spyfw-lfm3g-pjf2t-x3zmf-7llwq-p63wm-rqe",
            "6bt7p-6zrhd-xi3rj-jsugy-c6qzy-nvip5-7thap-gk6po-ygo5h-mh4jk-tae",
            "6fm36-gj33w-34vuw-rb6se-62vzz-he655-2vasp-jnu6w-ixzel-e6azy-eae",
            "6pt6z-rdv25-glenj-xvuq3-zdeeu-4c74r-smqeh-tgt34-exgd6-2wxvv-eqe",
            "6ssdj-55z6j-7q72p-vn255-utr5r-r2lgq-iitti-wnchn-rpwos-hjgrt-lqe",
            "6t3hc-abwek-snx4i-s57g2-w7u2g-beaec-jsjgx-g47s7-zjeb2-cb5pu-yqe",
            "6zj3x-tsbc3-jfzjr-lrnay-maejy-wwrrr-byxwk-kdzqu-nrkmz-mdcrx-aae",
            "7exbb-k4tu4-mw24y-3zylh-bopa5-fwdsc-f4a3z-45hh4-twpin-4zv4a-yae",
            "7f5c7-l7cuq-7jnfc-yexo3-bytd3-yf6xa-ctk76-wywln-jsy2n-ds2lp-tqe",
            "7h3aw-y3ygk-37mdb-cbuj7-ric2q-b7pgf-xwspg-bxzq5-cxid3-lqqwi-nae",
            "7muaz-6c5bc-6wjw2-f65xy-i7poz-jwhob-ty26j-ol5dh-hf3oo-dkgb3-6qe",
            "7pxqq-e4kmr-saxrs-jziwc-sfdft-bl2wh-k7vhl-6a35o-2stq5-v6uso-3qe",
            "7r7kx-pfeyy-apl6r-7m3mi-nhuan-zy5rd-l4bxq-6x5ak-a4632-sqoof-qqe",
            "7up6g-ihrwe-e2at5-bi6e2-6rjeu-yk5mv-wabsc-qsu7x-ve6kj-2kyq4-vae",
            "7vdar-a327j-byc4q-kacxg-ubasb-6dwe3-te272-frjgx-zeny2-xvj3j-5ae",
            "agmug-kfqq6-xoydk-yailg-mgajr-mpmxy-vrq44-ct45h-gmmzh-tkopc-7qe",
            "b37zb-apiex-wwlpd-ccbxn-yes6n-q44fy-jtz3j-mjxrs-ecw7v-lzmut-oae",
            "b44r4-u77ay-myhhv-d6d75-jusik-b7ry2-g6ms5-6okxm-tumyx-rjm4g-4ae",
            "bq47i-nl3wp-zbsv3-juxt5-ptcqw-wfm6e-eqv5e-m3y4e-w6xgk-z7rmw-eqe",
            "bs2f6-hqzox-bkqoe-aw3jj-7lso3-hyo33-tiq74-kuinv-ucdez-vtt4a-3ae",
            "c37f7-3shrz-2mk6g-e2zvp-3z6xc-uuk6r-6yegs-53uzt-c3vtf-dacsv-tae",
            "clfor-3la2h-lohby-tfriy-xfjkf-fnqpw-kkhjw-ctb2i-tscfx-ddwu7-bqe",
            "ctgsx-urhsi-pslxe-6p2vy-5dlpy-mbo7b-zdpht-zzbqt-j7hga-qsptm-4ae",
            "d6thv-qf4lw-o3n7c-rfg5q-zupjh-25a2d-kaxkp-y5wr6-nzjly-w2bcy-pqe",
            "dcbkk-imyhf-imyqt-ezzv4-g6krm-m7taq-l6mne-pqbmu-3ichw-y7x4r-2ae",
            "dd3ye-qncdq-jvghc-iymve-55lbp-hf6mq-6a3pa-ms5yj-espcn-n32jm-3qe",
            "dsnjt-rnuu4-vcgrm-wacun-da4y5-emipr-6bw7s-rxogu-7o2f3-a6zwb-yqe",
            "elru4-sjdrj-2rqqx-5lxfv-3i5dw-ktzok-7kr5v-un3y6-pubcf-7x47m-zae",
            "g5zt4-f3nhd-aquto-6vecp-tnrxe-gexie-kk7fk-znuxf-45cav-zcnlz-zqe",
            "gyty5-impo7-5uugu-2ocbi-ekcg2-eloni-cj67g-vasbq-i4z74-2bhpw-7qe",
            "haeka-mchv4-zwg3j-d3sz2-k5z2x-ulfmb-kflyu-ezzlz-ckc3d-ako5j-uqe",
            "hcjwo-3h7be-ds4ff-dasid-xbp6i-4uxxq-s4dmi-kwf5g-yzvyy-wakil-hqe",
        ]
    ]
    # have_hostos_nodes_adopted_revision(nodes, network, params)
    # are_hostos_nodes_healthy(nodes, network, params)
