"""
IC API code.

Minimal API to deal with proposals, sufficient in scope to
help with IC OS rollouts.
"""

import dfinity.ic_types as ic_types

IC_NETWORKS: dict[str, ic_types.ICNetwork] = {
    "mainnet": ic_types.ICNetwork(
        "https://ic0.app/",
        "https://dashboard.internetcomputer.org/proposal",
        "https://dashboard.internetcomputer.org/release",
        [
            "https://victoria.mainnet.dfinity.network/select/0/prometheus/api/v1/query",
        ],
        80,
        "dfinity.ic_admin.mainnet.proposer_key_file",
    ),
    # "staging": ic_types.ICNetwork("http://[2600:3004:1200:1200:5000:11ff:fe37:c55d]:8080/"),
    # FIXME we do not have a proposals URL for staging
}
