import subprocess

class View:    
    def create_tmux_session_with_watch(self, count_replicas):
        session_name = "postgres_monitor"

        # Убиваем старую сессию если есть
        subprocess.run(f"tmux kill-session -t {session_name} 2>/dev/null", shell=True)

        # Создаем новую сессию с первым окном (master порт)
        subprocess.run([
            "tmux", "new-session", "-d", "-s", session_name, "-n", "PG_Monitor",
            "watch", "-d", "-n", "2",
            "docker", "container", "exec", "pgrepmonitor-master-1",
            "/var/lib/postgresql/bin/psql", "-h", "localhost", "-p", "5432", "-U", "ubuntu", "-d", "postgres", 
            "-x", "-c", "'select * from pg_stat_replication'"
        ])
        
        for i in range(1, count_replicas + 1):
            # Определяем направление разделения
            if i % 2 == 1:
                split_dir = "-v"  # Вертикальное разделение
            else:
                split_dir = "-h"  # Горизонтальное разделение
            
            # Разделяем текущую панель
            subprocess.run([
                "tmux", "split-window", split_dir, "-t", f"{session_name}:0.{i-1}",
                "watch", "-d", "-n", "2",
                "docker", "container", "exec", f"pgrepmonitor-replicas-{i}",
                "/var/lib/postgresql/bin/psql", "-h", "localhost", "-p", "5432", "-U", "ubuntu", "-d", "postgres", 
                "-x", "-c", "'select * from pg_stat_replication'"
            ])

        # Выравниваем все панели
        subprocess.run(["tmux", "select-layout", "-t", f"{session_name}:0", "tiled"])
        
        # Подключаемся к сессии
        subprocess.run(["tmux", "attach", "-t", session_name])
    