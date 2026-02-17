"""
Defaults for rollout plans.

These exist in a separate file within dags/ to ensure that Airflow
can load them when the DAGs change.  Note that making changes to
this file does not ensure that the DAGs will reload the defaults;
you might have to push the big reload button at the top left corner
of the Airflow main screen listing the DAGs.

DO NOT attempt to instantiate DAGs here, or put complicated code
that might have side effects.  This is only for defaults.
"""

# Decoded to type SubnetRolloutPlanSpec.
DEFAULT_GUESTOS_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
Monday:
  9:00:      [io67a, xok3w]
  11:00:     [shefu, fuqsr, 4utr6]
Tuesday:
  7:00:      [pjljw, qdvhd, 2fq7c]
  9:00:      [snjp4, w4asl, qxesv]
  11:00:     [4zbus, ejbmu, uzr34]
  13:00:     [re2t4, c4isl]
  15:00:     [mkbc3, vcpt7]
Wednesday:
  7:00:      [pae4o, 5kdm2, csyj4]
  9:00:      [eq6en, lhg73, brlsh]
  11:00:     [k44fs, cv73p, 4ecnw]
  13:00:     [opn46, lspz2, o3ow2]
  15:00:     [2zs4v, 6excn, kp5jj]
Thursday:
  7:00:      [w4rem, 6pbhf, e66qm]
  9:00:      [yinp6, bkfrj, jtdsg]
  11:00:     [mpubz, x33ed, gmq5v]
  13:00:     [3hhby, nl6hn, pzp6e]
  15:00:     [rtvil, xlkub, 3zsyy]
Monday next week:
  7:00:
    subnets: [tdb26]
    batch: 30""".strip()
}

# Decoded to type ApiBoundaryNodeRolloutPlanSpec.
DEFAULT_API_BOUNDARY_NODES_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
nodes:
  - 4fssn-4vi43-2qufr-hlrfz-hfohd-jgrwc-7l7ok-uatwb-ukau7-lwmoz-tae
  - 4p3lu-7sy3a-ph47t-wlvb6-naehi-oki2f-pkokv-hovam-s5iul-sxvwk-3ae
  - 5dpkp-lfhr2-j7mfz-gavpn-puej5-wdfzg-fw42o-zupnu-izvk3-ubzzi-6ae
  - atrq6-prjsa-w2b32-gyukj-ivjd6-kc4gq-ufi6v-yier6-puxtx-n2ipp-fqe
  - bcbz4-w2ogq-jt7xk-xd7b2-ylhei-ygp3n-pjpdy-253tu-fpn3s-f5asy-fqe
  - bfj6y-cmhcf-6fxs7-ku2u4-tucww-b2eej-2dmap-snurk-3yaks-ss7xe-rae
  - coqzx-lgyi2-hizw4-nax6t-mshs6-y4ml6-xon24-ioon4-rer4r-qtuan-cae
  - dl74z-6vpps-k6bpu-5hjsj-fb6lb-34tsv-ue5gt-bdkjn-35pt5-fdu2a-rae
  - ec62l-q44va-5lyw2-gbl4w-xcx55-c3qv4-q67vc-fu6s7-xpm2r-7tjrw-tae
  - ek6px-kxr47-noli7-nyjql-au5uc-tmj3k-777mf-lfla5-k4xx4-msu3j-dae
  - gvqb5-b2623-3twpd-mruy4-3grts-agp2t-wgmt4-hspvj-5oyl6-kje32-aqe
  - jlifv-mddo3-zfwdc-x2wmu-xklbj-dai3i-3pvjy-j4hqm-sarqr-fhkq6-zae
  - lwxeh-xayoz-wu5eb-hwraj-a3pew-yeioj-cnwat-5uow4-ugxkc-kx44o-4ae
  - mbsyf-dtlsd-ccjq4-63rmh-u2lv7-dwxuq-ym6bk-63i2l-5uyki-zccek-mqe
  - mj4qw-5oxqx-5jgpn-66re3-gzgjb-rphz5-hmwhc-ahaa5-vespc-s4maj-vqe
  - pxxcj-h2sa5-q2yql-4j7od-jwtvq-uisnw-w2ox6-xtfqi-noxva-nlgxx-wqe
  - q6hk7-hplmv-xsedz-5bv7n-vfkws-rjyel-ijhge-hx2zg-nk7wb-tzisp-xqe
  - ubipk-gibrt-gr23n-u4mrg-iwgaa-4jz42-y6gt6-3htxw-3mq2m-dwhv2-pqe
  - upg5h-ggk5u-6qxp7-ksz3r-osynn-z2wou-65klx-cuala-sd6y3-3lorr-dae
  - yqbqe-whgvn-teyic-zvtln-rcolf-yztin-ecal6-smlwy-6imph-6isdn-qqe
start_day: Wednesday
resume_at: 7:00
suspend_at: 15:00
minimum_minutes_per_batch: 15
""".strip()
}

# Decoded to type HostOSRolloutPlanSpec.
DEFAULT_HOSTOS_ROLLOUT_PLANS: dict[str, str] = {
    "mainnet": """
stages:
  canary:
  - selectors:
      assignment: unassigned
      owner: DFINITY
      status: Healthy
      nodes_per_group: 1
  - selectors:
      assignment: unassigned
      owner: DFINITY
      status: Healthy
      nodes_per_group: 5
  - selectors:
      intersect:
      - assignment: unassigned
        owner: others
        group_by: datacenter
        status: Healthy
        nodes_per_group: 1
      - not: # Exclude problematic datacenters.
          join:
          - datacenter: ct1
          - datacenter: jb1
          - datacenter: jb2
  - selectors:
      intersect:
      - join:
        - assignment: assigned
          owner: others
          group_by: subnet
          status: Healthy
          nodes_per_group: 1
        - assignment: API boundary
          owner: DFINITY
          status: Healthy
          nodes_per_group: 1
      - not: # Exclude problematic datacenters.
          join:
          - datacenter: ct1
          - datacenter: jb1
          - datacenter: jb2
  main:
    selectors:
      join:
      - assignment: assigned
        group_by: subnet
        status: Healthy
        nodes_per_group: 1
      - assignment: API boundary
        status: Healthy
        nodes_per_group: 1
    tolerance: 5% # Up to three bad nodes in a 47-node subnet.
  unassigned:
    selectors:
      assignment: unassigned
      status: Healthy
      nodes_per_group: 100
    tolerance: 15% # Tolerate up to 15% nodes not working after the upgrade.
  stragglers:
    selectors: []
    tolerance: 100% # All nodes are known not to be healthy, so best-effort update.
allowed_days:
- Monday
- Tuesday
- Wednesday
- Thursday
resume_at: 7:00
suspend_at: 15:00
minimum_minutes_per_batch: 60
""".strip()
}
