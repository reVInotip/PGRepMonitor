import os
import sys
import psycopg2
from stats import PostgresStats
import subprocess
import threading
from time import sleep

class PostgresWorker:
    def __init__(self, out_dir, id, line, port):
        self.out_dir = out_dir
        self.is_run = False
        self.id = id
        self.line = line
        self.port = port
        self.connection = 0
        self.stats = PostgresStats()
        self.state = "apply run"

        if line >= 0:
            self.data_path = os.path.join(out_dir, str(line), str(id), "data")
        else:
            self.data_path = os.path.join(out_dir, str(id), "data")

        self.start()
        self.connect()
    
    def __del__(self):
        self.close_connection()
        self.stop()
    
    def start(self):
        try:
            subprocess.run(
                [os.path.join(str(self.line), str(self.id), "bin", "pg_ctl"), "start", "-D", self.data_path, "-o", f"-p {self.port}", "-s"],
                check=True,
                cwd=self.out_dir
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not start postgres instance {e}")
            sys.exit(1)
        
        self.is_run = True

        print(f"Node {self.id} in line {self.line} started in port {self.port}")

    def stop(self):
        if not self.is_run:
            print("Process is not run")

        if self.connection:
            self.close_connection()

        try:
            subprocess.run(
                [os.path.join(str(self.line), str(self.id), "bin", "pg_ctl"), "stop", "-D", self.data_path, "-s"],
                check=True,
                cwd=self.out_dir
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not stop postgres instance {e}")
            sys.exit(1)
        
        self.is_run = False

        print(f"Node {self.id} in line {self.line} stopped")
    
    def connect(self):
        if not self.is_run:
            print("Try connect to non-existent server")
            sys.exit(1)
        elif self.connection:
            print("Connection already exists")
            return
        
        self.connection = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)

        print(f"Connection to node {self.id} in line {self.line} open")
    
    def close_connection(self):
        if self.connection != 0:
            self.connection.close()
        self.connection = 0

        print(f"Connection to node {self.id} in line {self.line} stopped")
    
    def calc_rec_diff(self, start_lsn):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                row = curs.execute(f"SELECT pg_wal_lsn_diff(pg_last_wal_receive_lsn(), {start_lsn});")
                curs.fetchone()
                if row is not None:
                    return row[0]
        return 0
    
    def calc_rep_diff(self, start_lsn):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                row = curs.execute(f"SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), {start_lsn});")
                curs.fetchone()
                if row is not None:
                    return row[0]
        return 0
    
    def calc_stats(self):
        if not self.connection:
            return self.stats
        
        with self.connection:
            with self.connection.cursor() as curs:
                curs.execute("SELECT sent_lsn, min_write_lsn, min_flush_lsn, min_replay_lsn FROM pg_stat_replication;")
                row = curs.fetchone()
                if row is not None:
                    self.stats.sent_lsn, self.stats.min_write_lsn, self.stats.min_flush_lsn, self.stats.min_apply_lsn = row
                
                curs.execute("SELECT pg_last_wal_receive_lsn();")
                row = curs.fetchone()
                if row is not None:
                    self.stats.last_receive_lsn = row[0]

                curs.execute("SELECT pg_last_wal_replay_lsn();")
                row = curs.fetchone()
                if row is not None:
                    self.stats.last_applied_lsn = row[0]
                #print(curs.execute("SELECT sent_lsn, min_write_lsn, min_flush_lsn, min_replay_lsn FROM pg_stat_replication;"))
        
        return self.stats
    
    def resume_wal_replay(self, curs):
        curs.execute("select * from pg_wal_replay_resume();")
        curs.fetchone()
        self.state = "apply run"
    
    def stop_replay_wal_executor(self, t, callback=None):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                curs.execute("select * from pg_wal_replay_pause();")
                curs.fetchone()
                sleep(int(t))
                if callback:
                    callback(curs)
    
    def restart_node_executor(self, t):
        if t == "inf":
            return

        sleep(int(t))
        self.start()
        self.connect()
        self.state = "apply run"
    
    def stop_replay_wal(self, t):
        self.state = "apply stop"
        thread = threading.Thread(target=self.stop_replay_wal_executor, args=(t, self.resume_wal_replay,))
        thread.daemon = True
        thread.start()
    
    def kill_node(self, t):
        self.state = "killed"
        self.close_connection()
        self.stop()

        thread = threading.Thread(target=self.restart_node_executor, args=(t,))
        thread.daemon = True
        thread.start()
        

class MasterWorker(PostgresWorker):
    def __init__(self, out_dir, id, port):
        super().__init__(out_dir, id, -1, port)
        self.state = "query complete"
        self.start_lsn = 0
        self.calc_diff_connection = 0
    
    def __del__(self):
        super().__del__()
    
    def start(self):
        try:
            subprocess.run(
                [os.path.join(str(self.id), "bin", "pg_ctl"), "start", "-D", self.data_path, "-o", f"-p {self.port}", "-s"],
                check=True,
                cwd=self.out_dir
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not start postgres instance {e}")
            sys.exit(1)
        
        self.is_run = True

        print(f"Node {self.id} started in port {self.port}")

    def stop(self):
        if not self.is_run:
            print("Process is not run")

        if self.connection:
            self.close_connection()

        try:
            subprocess.run(
                [os.path.join(str(self.id), "bin", "pg_ctl"), "stop", "-D", self.data_path, "-s"],
                check=True,
                cwd=self.out_dir
            )
        except subprocess.CalledProcessError as e:
            print(f"Can not stop postgres instance {e}")
            sys.exit(1)
        
        self.is_run = False

        print(f"Node {self.id} stopped")
    
    def connect(self):
        if not self.is_run:
            print("Try connect to non-existent server")
            sys.exit(1)
        elif self.connection:
            print("Connection already exists")
            return
        
        self.connection = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)

        print(f"Connection to node {self.id} open")
    
    def close_connection(self):
        if self.connection:
            self.connection.close()
        self.connection = 0

        print(f"Connection to node {self.id} stopped")

    def create_test_db(self):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                curs.execute("CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, abacaba text);")
        local_conn.close()
    
    def query_executor(self, callback=None):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                curs.execute("INSERT INTO test (abacaba) VALUES ('assdlv');")
        local_conn.close()
        
        if callback:
            callback()
    
    def set_query_complete(self):
        self.state = "query complete"

    def calc_rec_diff(self, start_lsn):
        return 0
    
    def calc_rep_diff(self, start_lsn):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                row = curs.execute(f"SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), {start_lsn});")
                if row is not None:
                    return row[0]
        return 0
    
    def calc_diff(self, lsn1, lsn2):
        if lsn1 is None or lsn2 is None or lsn1 == 0 or lsn2 == 0:
            return 0

        if not self.calc_diff_connection:
            self.calc_diff_connection = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)

        with self.calc_diff_connection:
            with self.calc_diff_connection.cursor() as curs:
                curs.execute(f"SELECT pg_wal_lsn_diff('{lsn1}', '{lsn2}');")
                row = curs.fetchone()
                if row is not None:
                    return row[0]
        return 0
    
    def get_current_lsn(self):
        local_conn = psycopg2.connect(database="postgres", user="grigoriy", host="localhost", port=self.port)
        with local_conn:
            with local_conn.cursor() as curs:
                row = curs.execute("SELECT pg_current_wal_insert_lsn;")
                curs.fetchone()
                if row is not None:
                    return row[0]
        return 0
    
    def insert_one(self):
        with self.connection:
            with self.connection.cursor() as curs:
                curs.execute("SELECT * FROM pg_current_wal_insert_lsn();")
                row = curs.fetchone()
                if row is not None:
                    self.start_lsn = row[0]
        
        self.state = "query running"
        thread = threading.Thread(target=self.query_executor, args=(self.set_query_complete,))
        thread.daemon = True
        thread.start()
    
    def calc_stats(self, is_last):
        if not self.connection:
            print("Can not connect to server")
            return
        
        with self.connection:
            with self.connection.cursor() as curs:
                if not is_last:
                    curs.execute("SELECT sent_lsn, min_wait_write_lsn, min_wait_flush_lsn, min_wait_replay_lsn, min_write_lsn, min_flush_lsn, min_replay_lsn FROM pg_stat_replication;")
                    row = curs.fetchone()
                    if row is not None:
                        self.stats.sent_lsn, self.stats.min_wait_write_lsn, self.stats.min_wait_flush_lsn, self.stats.min_wait_apply_lsn, self.stats.min_write_lsn, self.stats.min_flush_lsn, self.stats.min_apply_lsn = row
                else:
                    curs.execute("SELECT sent_lsn, min_write_lsn, min_flush_lsn, min_replay_lsn FROM pg_stat_replication;")
                    row = curs.fetchone()
                    if row is not None:
                        self.stats.sent_lsn, self.stats.min_write_lsn, self.stats.min_flush_lsn, self.stats.min_apply_lsn = row
                
                #print(curs.execute("SELECT sent_lsn, min_write_lsn, min_flush_lsn, min_replay_lsn FROM pg_stat_replication;"))
        
        return self.stats
    
    def stop_replay_wal(self, t):
        pass
    
    def kill_node(self, t):
        pass
