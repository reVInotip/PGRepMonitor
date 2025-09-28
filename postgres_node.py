import subprocess
import psycopg2

class PostgresNode:
    def __init__(self, node_config, index, port):
        self.name = None
        self.role = None
        self.sync_type = None
        self.count_sync = None
        self.sync_stby_names = None
        self.logging = None
        self.wait_for = None
        self.connect_to = None
        self.conn_type = "async"
        self.index = index
        self.connection_info = None
        self.children = []
        self.parent = None

        for key, value in node_config.items():
            match key:
                case "name":
                    self.name = value
                case "role":
                    self.parse_role(value)
                case "sync_type":
                    self.parse_sync_type(value)
                case "count_sync":
                    self.count_sync = value
                case "sync_stby_names":
                    self.sync_stby_names = value
                case "logging":
                    self.logging = value
                case "wait_for":
                    self.parse_wait_for(value)
                case "connect_to":
                    self.connect_to = value
            
        self.check_options_combinations()
        self.set_connection_info(port)

    def is_master(self):
        return self.role == "master"
    
    def set_connection_info(self, port):
        # host - host in docker network (port in docker network always 5432)
        # port - port for outer connections (host in outer connections always localhost)
        if self.role == "master":
            self.connection_info = {"host": "pgrepmonitor-master-1", "port": 5432}
        else:
            self.connection_info = {"host": f"pgrepmonitor-replicas-{self.index}", "port": port}
    
    def init_node_data(self, connect_to = None, port = None, slot_index = None):
        if self.role == "master":
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c", "/scripts/master_init.sh"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Init master node failed, logs {e}")
            
            print("Master node inited successfully")
        
        primary_conninfo =\
            f"host={connect_to} port=5432 user=ubuntu application_name={self.name} fallback_application_name={self.name} replication=true"
        if self.role == "replica":
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash",
                    "-c", f"/scripts/replica_init.sh {connect_to} {port} slot{slot_index} '{primary_conninfo}'"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Init cascade replica node {self.name} failed, logs {e}")
            
            print(f"Cascade replica node {self.name} inited successfully")
        elif self.role == "last_replica":
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash",
                    "-c", f"/scripts/end_replica_init.sh {connect_to} {port} slot{slot_index} '{primary_conninfo}'"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Init last replica node {self.name} failed, logs {e}")
            
            print(f"Last replica node {self.name} inited successfully")
        
    def init_node_config(self):
        if self.role == "master":
            try:
                if self.conn_type == "async":
                    subprocess.run(
                        ["docker", "exec", self.connection_info["host"], "bash", "-c",
                        f"/scripts/master_config_init_async.sh {self.logging}"],
                        check=True,
                    )
                elif self.conn_type == "sync":
                    sync_stby_names_str = None

                    if self.sync_type == "base":
                        sync_stby_names_str = ", ".join(self.sync_stby_names)
                    elif self.sync_type == "priority":
                        sync_stby_names_str = "FIRST " + str(self.count_sync) + " (" + ", ".join(self.sync_stby_names) + ")"
                    else:
                        sync_stby_names_str = "ANY " + str(self.count_sync) + " (" + ", ".join(self.sync_stby_names) + ")"
                    
                    subprocess.run(
                        ["docker", "exec", self.connection_info["host"], "bash", "-c",
                        f"/scripts/master_config_init_sync.sh {self.logging} '{self.wait_for}' '{sync_stby_names_str}'"],
                        check=True,
                    )
            except subprocess.CalledProcessError as e:
                print(f"Init master node config failed, logs {e}")
            
            print("Master node config initted")
            return
        elif self.role == "replica":
            try:
                if self.conn_type == "async":
                    subprocess.run(
                        ["docker", "exec", self.connection_info["host"], "bash", "-c",
                        f"/scripts/replica_config_init_async.sh {self.logging}"],
                        check=True,
                    )
                elif self.conn_type == "sync":
                    sync_stby_names_str = None

                    if self.sync_type == "base":
                        sync_stby_names_str = ", ".join(self.sync_stby_names)
                    elif self.sync_type == "priority":
                        sync_stby_names_str = "FIRST " + str(self.count_sync) + " (" + ", ".join(self.sync_stby_names) + ")"
                    else:
                        sync_stby_names_str = "ANY " + str(self.count_sync) + " (" + ", ".join(self.sync_stby_names) + ")"
                    
                    subprocess.run(
                        ["docker", "exec", self.connection_info["host"], "bash", "-c",
                        f"/scripts/replica_config_init_sync.sh {self.logging} '{self.wait_for}' '{sync_stby_names_str}'"],
                        check=True,
                    )
            except subprocess.CalledProcessError as e:
                print(f"Init replica node config failed, logs {e}")
            
            print("Replica node config initted")
        else:
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c",
                    f"/scripts/end_replica_config_init.sh {self.logging}"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Init last replica node config failed, logs {e}")
            
            print("Last replica node config initted")
    
    def start_node(self):
        if self.role == "master":
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c", "/scripts/start.sh"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Start master node failed, logs {e}")
            
            print("Master node started")
        else:
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c", "/scripts/start.sh"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Start replica node {self.name} failed, logs {e}")
            
            print(f"Replica node {self.name} started")
    
    def stop_node(self):
        if self.role == "master":
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c", "/scripts/stop.sh"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Stop master node failed, logs {e}")
            
            print("Master node stopped")
        else:
            try:
                subprocess.run(
                    ["docker", "exec", self.connection_info["host"], "bash", "-c", "/scripts/stop.sh"],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Stop replica node {self.name} failed, logs {e}")
            
            print(f"Replica node {self.name} stopped")
    
    def get_replication_data(self):
        try:
            conn = psycopg2.connect(host="localhost", port=self.connection_info["port"], user="ubuntu", dbname="postgres")
            cursor = conn.cursor()
            
            cursor.execute("""
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
            
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return data
            
        except psycopg2.Error as e:
            print(f"Error data get: {e}")
            return []

    def check_options_combinations(self):
        # required options check
        if (self.name is None) or (self.role is None):
            print("Name and role for node is required")
            exit(1)
        
        # check combinations
        if (self.sync_type is None) and (self.sync_stby_names is None) and (self.wait_for is None):
            self.conn_type = "async"
        elif (self.sync_type is not None) and (self.sync_stby_names is not None) and (self.wait_for is not None):
            self.conn_type = "sync"
        else:
            print("Error1")
            print(str(self.sync_type) + " " + str(self.sync_stby_names) + " " + str(self.wait_for))
            exit(1)
        
        if self.sync_type is not None and self.sync_type != "base" and (self.count_sync is None or self.count_sync <= 0):
            print("Error2")
            print(str(self.sync_type) + " " + str(self.count_sync))
            exit(2)
        
        if (self.role in ["replica", "last_replica"]) and (self.connect_to is None):
            print("Error3")
            exit(3)
        
        # check unimportant options
        if self.logging is None:
            self.logging = True
    
    def parse_role(self, role):
        if role in ["master", "replica", "last_replica"]:
            self.role = role
        else:
            print("Invalid role")
            exit(1)

    def parse_sync_type(self, type):
        if type in ["quorum", "priority", "base"]:
            self.sync_type = type
        else:
            print("Invalid type")
            exit(1)
    
    def parse_wait_for(self, wait_for):
        if wait_for == "write":
            self.wait_for = "remote_write"
        elif wait_for == "flush":
            self.wait_for = "on"
        elif wait_for == "apply":
            self.wait_for = "remote_apply"
