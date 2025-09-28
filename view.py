import subprocess

class View:    
    def create_tmux_session_with_watch(self, master_port, start_port, end_port):
        session_name = "postgres_monitor"

        # Убиваем старую сессию если есть
        subprocess.run(f"tmux kill-session -t {session_name} 2>/dev/null", shell=True)

        # Создаем новую сессию с первым окном (master порт)
        subprocess.run([
            "tmux", "new-session", "-d", "-s", session_name, "-n", "PG_Monitor",
            "watch", "-d", "-n", "2",
            "psql", "-h", "localhost", "-p", str(master_port), "-U", "ubuntu", "-d", "postgres", 
            "-x", "-c", "'select * from pg_stat_replication'"
        ])
        
        for i, port in enumerate(range(start_port, end_port + 1), 1):
            # Определяем направление разделения
            if i % 2 == 1:
                split_dir = "-v"  # Вертикальное разделение
            else:
                split_dir = "-h"  # Горизонтальное разделение
            
            # Разделяем текущую панель
            subprocess.run([
                "tmux", "split-window", split_dir, "-t", f"{session_name}:0.{i-1}",
                "watch", "-d", "-n", "2",
                "psql", "-h", "localhost", "-p", str(port), "-U", "ubuntu", "-d", "postgres", 
                "-x", "-c", "'select * from pg_stat_replication'"
            ])

        # Выравниваем все панели
        subprocess.run(["tmux", "select-layout", "-t", f"{session_name}:0", "tiled"])
        
        # Подключаемся к сессии
        subprocess.run(["tmux", "attach", "-t", session_name])
    