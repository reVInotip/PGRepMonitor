import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor


class PostgresNode:
    DOCKER_CMD = ["docker", "exec"]

    def __init__(self, node_config, index, port):
        self.name = None
        self.role = None
        self.sync_type = None
        self.count_sync = None
        self.sync_standby_names = None
        self.logging = True
        self.wait_for = None
        self.connect_to = None
        self.conn_type = "async"

        self.index = index
        self.children = []
        self.parent = None
        self.connection_info = None

        self._parse_cluster_config(node_config)
        self._validate_config()
        self._set_connection_info(port)

        self._connection = self._connect()

    # -------------------- CONNECTION --------------------

    def _connect(self):
        return psycopg2.connect(
            host="localhost",
            port=self.connection_info["port"],
            user="ubuntu",
            dbname="postgres"
        )

    def close(self):
        if self._connection:
            self._connection.close()

    # -------------------- HELPERS --------------------

    def _run(self, cmd, error_msg):
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"{error_msg}: {e}") from e

    def _docker_exec(self, script):
        cmd = self.DOCKER_CMD + [
            self.connection_info["host"],
            "bash", "-c", script
        ]
        self._run(cmd, f"Command failed on {self.name}")

    # -------------------- INIT --------------------

    def _set_connection_info(self, port):
        if self.role == "master":
            self.connection_info = {"host": "pgrepmonitor-master-1", "port": 5432}
        else:
            self.connection_info = {
                "host": f"pgrepmonitor-replicas-{self.index}",
                "port": port
            }

    def _validate_config(self):
        if not self.name or not self.role:
            raise ValueError("Node must have name and role")

        if all(v is None for v in [self.sync_type, self.sync_standby_names, self.wait_for]):
            self.conn_type = "async"
        elif all(v is not None for v in [self.sync_type, self.sync_standby_names, self.wait_for]):
            self.conn_type = "sync"
        else:
            raise ValueError("Invalid sync configuration")

        if self.sync_type and self.sync_type != "base":
            if not self.count_sync or self.count_sync <= 0:
                raise ValueError("Invalid count_sync")

        if self.role in ["replica", "last_replica"] and not self.connect_to:
            raise ValueError(f"{self.name} must have connect_to")

    # -------------------- INIT DATA --------------------

    def init_node_data(self, connect_to=None, port=None, slot_index=None):
        if self.role == "master":
            self._docker_exec("/scripts/master_init.sh")
            return

        primary_conninfo = (
            f"host={connect_to} port=5432 user=ubuntu "
            f"application_name={self.name} replication=true"
        )

        if self.role == "replica":
            script = f"/scripts/replica_init.sh {connect_to} {port} slot{slot_index} '{primary_conninfo}'"
        else:
            script = f"/scripts/end_replica_init.sh {connect_to} {port} slot{slot_index} '{primary_conninfo}'"

        self._docker_exec(script)

    # -------------------- CONFIG --------------------

    def init_node_config(self):
        if self.conn_type == "async":
            script = self._async_config_script()
        else:
            script = self._sync_config_script()

        self._docker_exec(script)

    def _async_config_script(self):
        if self.role == "master":
            return f"/scripts/master_config_init_async.sh {self.logging}"
        elif self.role == "replica":
            return f"/scripts/replica_config_init_async.sh {self.logging}"
        else:
            return f"/scripts/end_replica_config_init.sh {self.logging}"

    def _sync_config_script(self):
        sync_names = self._build_sync_standby_names()

        if self.role == "master":
            return f"/scripts/master_config_init_sync.sh {self.logging} '{self.wait_for}' '{sync_names}'"
        elif self.role == "replica":
            return f"/scripts/replica_config_init_sync.sh {self.logging} '{self.wait_for}' '{sync_names}'"
        else:
            return f"/scripts/end_replica_config_init.sh {self.logging}"

    def _build_sync_standby_names(self):
        names = ", ".join(self.sync_standby_names)

        if self.sync_type == "base":
            return names
        elif self.sync_type == "priority":
            return f"FIRST {self.count_sync} ({names})"
        else:
            return f"ANY {self.count_sync} ({names})"

    # -------------------- LIFECYCLE --------------------

    def start_node(self):
        self._docker_exec("/scripts/start.sh")

    def stop_node(self):
        self._docker_exec("/scripts/stop.sh")

    # -------------------- STATS --------------------

    def get_replication_data(self):
        try:
            with self._connection.cursor(cursor_factory=RealDictCursor) as cur:
                if self._is_last_replica():
                    return self._get_replica_lsn(cur)
                else:
                    return self._get_replication_info(cur)

        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to fetch replication data: {e}") from e

    def _get_replica_lsn(self, cur):
        cur.execute("""
            SELECT
                pg_last_wal_replay_lsn()::text AS replay_lsn,
                pg_last_wal_receive_lsn()::text AS receive_lsn
        """)

        row = cur.fetchone()

        return (
            f"{self.connection_info['host']}\n"
            f"-------------------\n"
            f"replay_lsn: {row['replay_lsn']}\n"
            f"receive_lsn: {row['receive_lsn']}"
        )

    def _get_replication_info(self, cur):
        cur.execute("""
            SELECT
                application_name,
                client_addr,
                state,
                sync_state,
                sent_lsn,
                write_lsn,
                flush_lsn,
                replay_lsn
            FROM pg_stat_replication
        """)

        rows = cur.fetchall()

        lines = [
            (
                f"""
                    application_name: {r['application_name']}
                    client_addr: {r['client_addr']}
                    state: {r['state']}
                    sync_state: {r['sync_state']}
                    sent_lsn: {r['sent_lsn']}
                    write_lsn: {r['write_lsn']}
                    flush_lsn: {r['flush_lsn']}
                    replay_lsn: {r['replay_lsn']}
                """
            )
            for r in rows
        ]

        return (
            f"{self.connection_info['host']}\n"
            f"-------------------\n" +
            "\n".join(lines)
        )

    # -------------------- UTILS --------------------

    def is_master(self):
        return self.role == "master"

    def _is_last_replica(self):
        return self.role == "last_replica"

    # -------------------- PARSER --------------------

    def _parse_cluster_config(self, node_config):
        for key, value in node_config.items():
            match key:
                case "name":
                    self.name = value
                case "role":
                    self._parse_role(value)
                case "sync_type":
                    self._parse_sync_type(value)
                case "count_sync":
                    self.count_sync = value
                case "sync_standby_names":
                    self.sync_standby_names = value
                case "logging":
                    self.logging = value
                case "wait_for":
                    self._parse_wait_for(value)
                case "connect_to":
                    self.connect_to = value

    def _parse_role(self, role):
        if role not in ["master", "replica", "last_replica"]:
            raise ValueError("Invalid role")
        self.role = role

    def _parse_sync_type(self, sync_type):
        if sync_type not in ["quorum", "priority", "base"]:
            raise ValueError("Invalid sync type")
        self.sync_type = sync_type

    def _parse_wait_for(self, wait_for):
        mapping = {
            "write": "remote_write",
            "flush": "on",
            "apply": "remote_apply",
            "off": "off"
        }
        self.wait_for = mapping.get(wait_for)