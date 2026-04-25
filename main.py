# import scripts
from scripts.seatable_servers.cvpz import cvpz_seatable_servers
from scripts.seatable_servers import network_capacity

from scripts.recources_usage import cpu_used, disk_usage

from scripts.soft_version.version_spo.cvpz import (
    cvpz_infrasoft_version,
    cvpz_spo_versions_csv,
    cvpz_spo_versions_seatable,
    sync_tables_spo_servers_cvpz,
)
from scripts.soft_version.version_ppo import cvpz_ppo_versions_seatable

if __name__ == "__main__":
    print("cvpz_seatable_servers")
    cvpz_seatable_servers.main()
    print("network_capacity")
    network_capacity.main()

    print("cpu_used")
    cpu_used.main()
    print("disk_usage")
    disk_usage.main()

    print("cvpz_spo_versions_seatable")
    cvpz_spo_versions_seatable.main()
    print("cvpz_infrasoft_version")
    cvpz_infrasoft_version.main()
    print("cvpz_spo_versions_csv")
    cvpz_spo_versions_csv.main()
    print("sync_tables_spo_servers_cvpz")
    sync_tables_spo_servers_cvpz.main()

    print("cvpz_ppo_versions_seatable")
    cvpz_ppo_versions_seatable.main()
