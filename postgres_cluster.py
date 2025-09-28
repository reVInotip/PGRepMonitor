import os
import subprocess
import sys
from view import View
from postgres_node import PostgresNode

class PostgresCluster:
    def __init__(self, config, need_rebuild, need_reinit, enable_debug):
        self.config = config
        
        self.path_to_source = self.config["path_to_source"]
        self.count_replicas = len(self.config["cluster"]) - 1

        nodes = []
        i = 1
        default_port = 10000
        for node_conf in self.config["cluster"]:
            node = PostgresNode(node_conf, i, default_port + i - 1)
            nodes.append(node)

            if node.is_master():
                continue
            i += 1
        
        self.master_node = self.try_build_tree(nodes)
        if self.master_node is None:
            print("Some error occurred while building cluster tree")

        bin_dir = os.path.join(os.getcwd(), "cluster-bin")

        if not os.path.exists(bin_dir):
            os.mkdir(bin_dir, mode=0o0777)
        
        self.create_containers(bin_dir)

        if need_rebuild:
            self.rebuild(enable_debug)

        if need_reinit:
            self.reinit(bin_dir)

        self.start()

        self.view = View()
    
    def try_build_tree(self, node_list):
        master_node = None

        for node in node_list:
            if node.role == "master":
                master_node = node
            
            if node.connect_to is None and node.role != "master":
                print("Cluster tree not linked!")
                exit(1)

            for node1 in node_list:
                if node1.connect_to == node.name:
                    node.children.append(node1)
                    node1.parent = node
        
        return master_node

    
    def create_containers(self, bin_dir):
        print("Create containers...")
        
        try:
            subprocess.run(
                ["docker-compose", "up", "-d"],
                env={
                    "PATH_TO_SOURCE": f"{self.path_to_source}",
                    "END_PORT": f"{10000 + self.count_replicas - 1}",
                    "START_PORT": "10000",
                    "COUNT_REPLICAS": f"{self.count_replicas}"
                },
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not create containers, log {e}")
            exit(1)
        
        print("Containers created successfully")
    
    def rebuild(self, is_debug):
        print("Rebuild started...")
        
        try:
            if is_debug:
                subprocess.run(
                    ["docker", "exec", "pgrepmonitor-master-1", "bash", "-c", "/scripts/build.sh -d"],
                    check=True,
                )
            else:
                subprocess.run(
                    ["docker", "exec", "pgrepmonitor-master-1", "/scripts/build.sh"],
                    check=True
                )
        except subprocess.CalledProcessError as e:
            print(f"Build postgres failed, logs {e}")
        
        
        print("Rebuild success")
    
    def start(self):
        print("Start cluster...")
        node_queue = []
        node_queue.append(self.master_node)

        while len(node_queue) > 0:
            node = node_queue[0]
            node_queue.remove(node)
            node.start_node()

            for child in node.children:
                node_queue.append(child)
        
        print("Cluster started")
    
    def destroy_containers(self):
        print("Destroy containers...")
        
        try:
            subprocess.run(
                ["docker-compose", "down", "-v"],
                env={
                    "PATH_TO_SOURCE": f"{self.path_to_source}",
                    "END_PORT": f"{10000 + self.count_replicas - 1}",
                    "START_PORT": "10000",
                    "COUNT_REPLICAS": f"{self.count_replicas}"
                },
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not create containers, log {e}")
            exit(1)
        
        print("Containers destroyed successfully")
    
    def stop(self):
        print("Stopped cluster...")
        node_queue = []
        node_queue.append(self.master_node)

        while len(node_queue) > 0:
            node = node_queue[0]
            node_queue.remove(node)
            node.stop_node()

            for child in node.children:
                node_queue.append(child)
        
        print("Cluster stopped")

    def reinit(self, bin_dir):
        print("Init started...")

        if not os.path.exists(bin_dir):
            print("Can not init instances without sources")
            sys.exit(1)
        
        self.master_node.init_node_data()
        
        node = None

        node_queue = []
        node_queue.append(self.master_node)

        while len(node_queue) > 0:
            node = node_queue[0]
            node_queue.remove(node)
            node.start_node()

            i = 1
            for child in node.children:
                node_queue.append(child)
                child.init_node_data(connect_to=node.connection_info["host"], port=5432, slot_index=i)
                i += 1
            
            node.init_node_config()
            node.stop_node()
        
        print("Cluster init successfully")
    
    def main_loop(self):
        try:
            self.view.create_tmux_session_with_watch(5432, 10000, 10000 + self.count_replicas - 1)
        except KeyboardInterrupt as i:
            self.stop()
            self.destroy_containers()
            sys.exit(0)
