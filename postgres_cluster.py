import os
import subprocess
import sys
from collections import deque

from postgres_node import PostgresNode


class PostgresCluster:
    BASE_PORT = 10000

    def __init__(self, config, view, need_rebuild=False, need_reinit=False, enable_debug=False):
        self.config = config
        self.path_to_source = config["path_to_source"]
        self.nodes = self._build_nodes(config["cluster"])
        self.count_replicas = len(self.nodes) - 1

        self.master_node = self._build_tree()
        if not self.master_node:
            raise RuntimeError("Failed to determine master node")

        self.bin_dir = self._ensure_bin_dir()

        self._create_containers()

        if need_rebuild:
            self._rebuild(enable_debug)

        if need_reinit:
            self._reinit()

        self._start()
        self.view = view

    # -------------------- INIT HELPERS --------------------

    def _build_nodes(self, cluster_config):
        nodes = []
        replica_index = 1

        for node_conf in cluster_config:
            port = self.BASE_PORT + replica_index - 1
            node = PostgresNode(node_conf, replica_index, port)
            nodes.append(node)

            if not node.is_master():
                replica_index += 1

        return nodes

    def _build_tree(self):
        master = None
        name_map = {node.name: node for node in self.nodes}

        for node in self.nodes:
            if node.role == "master":
                master = node

            if node.role != "master" and node.connect_to is None:
                raise ValueError(f"Node {node.name} is not connected")

            if node.connect_to:
                parent = name_map.get(node.connect_to)
                if not parent:
                    raise ValueError(f"Parent {node.connect_to} not found")

                parent.children.append(node)
                node.parent = parent

        return master

    def _ensure_bin_dir(self):
        bin_dir = os.path.join(os.getcwd(), "cluster-bin")
        os.makedirs(bin_dir, mode=0o777, exist_ok=True)
        return bin_dir

    # -------------------- DOCKER --------------------

    def _docker_env(self):
        return {
            "PATH_TO_SOURCE": self.path_to_source,
            "END_PORT": str(self.BASE_PORT + self.count_replicas - 1),
            "START_PORT": str(self.BASE_PORT),
            "COUNT_REPLICAS": str(self.count_replicas),
        }

    def _run_command(self, cmd, env=None, error_msg="Command failed"):
        try:
            subprocess.run(cmd, env=env, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"{error_msg}: {e}") from e

    def _create_containers(self):
        print("Creating containers...")
        self._run_command(
            ["docker", "compose", "up", "-d"],
            env=self._docker_env(),
            error_msg="Failed to create containers"
        )
        print("Containers created")

    def destroy_containers(self):
        print("Destroying containers...")
        self._run_command(
            ["docker", "compose", "down", "-v"],
            env=self._docker_env(),
            error_msg="Failed to destroy containers"
        )
        print("Containers destroyed")

    def _rebuild(self, debug=False):
        print("Rebuilding...")
        cmd = ["/scripts/build.sh"]
        if debug:
            cmd = ["bash", "-c", "/scripts/build.sh -d"]

        self._run_command(
            ["docker", "exec", "pgrepmonitor-master-1"] + cmd,
            error_msg="Build failed"
        )
        print("Rebuild complete")

    # -------------------- CLUSTER OPS --------------------

    def _bfs(self):
        """Generator for BFS traversal"""
        queue = deque([self.master_node])
        while queue:
            node = queue.popleft()
            yield node
            queue.extend(node.children)

    def _start(self):
        print("Starting cluster...")
        for node in self._bfs():
            node.start_node()
        print("Cluster started")

    def stop(self):
        print("Stopping cluster...")
        for node in self._bfs():
            node.stop_node()
        print("Cluster stopped")

    def _reinit(self):
        print("Initializing cluster...")

        if not os.path.exists(self.bin_dir):
            raise RuntimeError("Sources not found")

        self.master_node.init_node_data()

        for node in self._bfs():
            node.start_node()

            for i, child in enumerate(node.children, start=1):
                child.init_node_data(
                    connect_to=node.connection_info["host"],
                    port=5432,
                    slot_index=i
                )

            node.init_node_config()
            node.stop_node()

        print("Cluster initialized")

    # -------------------- MAIN LOOP --------------------

    def main_loop(self):
        try:
            self.view.create_tmux_session_with_watch(
                5432,
                self.BASE_PORT,
                self.BASE_PORT + self.count_replicas - 1
            )
        except KeyboardInterrupt:
            self.stop()
            self.destroy_containers()
            sys.exit(0)