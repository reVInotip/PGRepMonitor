import os
import subprocess
import sys
from postgres_worker import PostgresWorker, MasterWorker
import shutil

class PostgresCluster:
    def __init__(self, config, view):
        self.path_to_source = config.path_to_source
        self.out_dir = config.out_dir
        self.lines = config.lines
        self.rebuild_requested = config.need_rebuild
        self.reinit_requested = config.need_reinit
        self.view = view
        self.length = max(self.lines)
        self.wait_for = config.sync_commit

        if config.sync_commit == "write":
            self.sync_commit = "remote_write" 
        elif config.sync_commit == "flush":
            self.sync_commit = "on"
        else:
            self.sync_commit = "remote_apply"

        if self.rebuild_requested:
            self.rebuild(self.out_dir, self.path_to_source, self.lines)
        
        if self.reinit_requested:
            self.reinit(self.out_dir, self.lines)

        self.nodes = []

        self.nodes.append([MasterWorker(self.out_dir, "master", 6432)])

        j = 0
        for line in self.lines:
            sublist = []
            for i in range(line):
                sublist.append(PostgresWorker(self.out_dir, i, j, 6432 + i + (j + 1) * self.length))
            self.nodes.append(sublist)
            j += 1
        
        self.nodes[0][0].create_test_db()
    
    def rebuild(self, out_dir, path_to_source, lines):
        print("Rebuild started...")

        if not os.path.exists(out_dir):
            os.mkdir(out_dir, mode=0o777)
        
        path = os.path.join(out_dir, "master")
        if not os.path.exists(path):
            os.mkdir(path, mode=0o777)
        
        try:
            subprocess.run(
                ["./configure", f"--prefix={path}", "--without-icu"],
                cwd=path_to_source,
                check=True
            )

            subprocess.run(
                ["make", "-j4" , "world"],
                cwd=path_to_source,
                check=True
            )

            subprocess.run(
                ["make", "-j4" , "install-world"],
                cwd=path_to_source,
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Build node master failed, logs {e}")
        
        j = 0
        for line in lines:
            for i in range(line):
                path = os.path.join(out_dir, str(j))
                
                if not os.path.exists(path):
                    os.mkdir(path, mode=0o777)

                path = os.path.join(path, str(i))

                if not os.path.exists(path):
                    os.mkdir(path, mode=0o777)

                try:
                    subprocess.run(
                        ["./configure", f"--prefix={path}", "--without-icu"],
                        cwd=path_to_source,
                        check=True
                    )

                    subprocess.run(
                        ["make", "-j4" , "world"],
                        cwd=path_to_source,
                        check=True
                    )

                    subprocess.run(
                        ["make", "-j4" , "install-world"],
                        cwd=path_to_source,
                        check=True
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Build node {i} in line {j} failed, logs {e}")

                print(f"Node {i} in line {j} build successfully")
            j += 1
        
        print("Rebuild success")
        

    def reinit(self, out_dir, lines):
        print("Init started...")

        if not os.path.exists(out_dir):
            print("Can not init instances")
            sys.exit(1)

        master_data = os.path.join(out_dir, "master", "data")
        if os.path.exists(master_data):
            shutil.rmtree(master_data)
        
        try:
            subprocess.run(
                [os.path.join("master", "bin", "pg_ctl"), "init", "-D", master_data, "-s", "-o", "-g --locale=en_US.UTF-8"],
                check=True,
                cwd=out_dir
            )
        except subprocess.CalledProcessError as e:
            print(f"Init master node failed, logs {e}")

        print("Master node inited successfully")

        j = 0
        for line in lines:
            path_start = os.path.join(out_dir, "master", "data")
            path_end = os.path.join(out_dir, str(j), "0", "data")

            if not os.path.exists(path_start):
                print("Can not init instances")
                sys.exit(1)

            if os.path.exists(path_end):
                shutil.rmtree(path_end)

            try:
                subprocess.run(
                    [os.path.join("master", "bin", "pg_ctl"), "start", "-D", path_start, "-s", "-o", f"-p {6432}"],
                    check=True,
                    cwd=out_dir
                )

                subprocess.run(
                    ["pg_basebackup", "-h", "localhost", "-p", "6432", "-X", "stream" , "-P" ,"-R", "-C", "-S", f"slot{j}", "-v", "-D", path_end],
                    check=True
                )

                subprocess.run(
                    [os.path.join("master", "bin", "pg_ctl"), "stop", "-D", path_start, "-s"],
                    check=True,
                    cwd=out_dir
                )
            except subprocess.CalledProcessError as e:
                print(f"Init node 0 in line {j} failed, logs {e}")

            print(f"Node 0 in line {j} inited successfully")

            for i in range(1, line):
                path_start = os.path.join(out_dir, str(j), str(i - 1), "data")
                path_end = os.path.join(out_dir, str(j), str(i), "data")

                if not os.path.exists(path_start):
                    print("Can not init instances")
                    sys.exit(1)

                if os.path.exists(path_end):
                    shutil.rmtree(path_end)

                try:
                    subprocess.run(
                        [os.path.join(str(j), str(i), "bin", "pg_ctl"), "start", "-D", path_start, "-s", "-o", f"-p {6432 + i + (j + 1) * self.length - 1}"],
                        check=True,
                        cwd=out_dir
                    )

                    subprocess.run(
                        ["pg_basebackup", "-h", "localhost", "-p", str(6432 + i + (j + 1) * self.length - 1), "-X", "stream" , "-P" ,"-R", "-C", "-S", f"slot{j}", "-v", "-D", path_end],
                        check=True
                    )

                    subprocess.run(
                        [os.path.join(str(j), str(i), "bin", "pg_ctl"), "stop", "-D", path_start, "-s"],
                        check=True,
                        cwd=out_dir
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Init node {i} in line {j} failed, logs {e}")

                print(f"Node {i} in line {j} inited successfully")
            j += 1
        
        j = 0
        for line in lines:
            path_start = os.path.join(out_dir, str(j), "0", "data", "postgresql.auto.conf")
            with open(path_start, "w") as postgres_conf:
                postgres_conf.write(f"primary_conninfo = 'host=localhost port=6432 user=grigoriy dbname=postgres application_name=line{j} fallback_application_name=line{j} replication=true'")
            
            for i in range(line - 1):
                path_start = os.path.join(out_dir, str(j), str(i), "data", "postgresql.conf")
                with open(path_start, "a") as postgres_conf:
                    postgres_conf.write("synchronous_standby_names = 'walreceiver'\n")
                    postgres_conf.write("log_replication_commands = on\n")
                    postgres_conf.write("log_min_messages = debug2\n")
                    postgres_conf.write("logging_collector = on\n")
                    postgres_conf.write("log_destination = 'jsonlog'\n")
            
            path_start = os.path.join(out_dir, str(j), str(line - 1), "data", "postgresql.conf")
            with open(path_start, "a") as postgres_conf:
                postgres_conf.write("wal_receiver_status_interval = 1s\n")

            j += 1
        
        master_conf = os.path.join(master_data, "postgresql.conf")

        height = len(lines)
        names = f"synchronous_standby_names = 'FIRST {height} ("
        for i in range(height):
            names += f"line{i}"
            if i != height - 1:
                names += ", "
        names += ")'\n"
        print(names)

        with open(master_conf, "w") as postgres_conf:
            postgres_conf.write(names)
            postgres_conf.write("synchronous_commit=" + self.sync_commit + "\n")
            postgres_conf.write("log_replication_commands = on\n")
            postgres_conf.write("log_min_messages = debug2\n")
            postgres_conf.write("logging_collector = on\n")
            postgres_conf.write("log_destination = 'jsonlog'\n")
        
        print("Cluster init successfully")
    
    def main_loop(self):
        try:
            while(True):
                command = input("Enter command: ")
                if command == "e":
                    options = input("Enter options: ")
                    self.view.show(self.lines, self.nodes, options, self.wait_for)
        except KeyboardInterrupt as i:
            sys.exit(0)