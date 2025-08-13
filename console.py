from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from view import IView
import time
import os

class ConsoleView(IView):
    def show(self, lines, nodes, options, wait_for):
        self.wait_for = wait_for
        if options:
            opt_list = options.split()
            if opt_list[0] == "apply_stop":
                for i in range(1, len(opt_list), 3):
                    nodes[int(opt_list[i]) + 1][int(opt_list[i + 1])].stop_replay_wal(int(opt_list[i + 2]))
            if opt_list[0] == "kill_node":
                for i in range(1, len(opt_list), 3):
                    nodes[int(opt_list[i]) + 1][int(opt_list[i + 1])].kill_node(opt_list[i + 2])

        start = time.perf_counter()
        nodes[0][0].insert_one()
        with Live(self.generate_layout(lines, nodes), refresh_per_second=1) as live:
            while nodes[0][0].state != "query complete":
                live.update(self.generate_layout(lines, nodes))
                time.sleep(1)
            live.update(self.generate_layout(lines, nodes, True))
        end = time.perf_counter()
        print(f"Execution time: {end - start:.6f} seconds")
    
    def generate_layout(self, lines, nodes, is_last=False):
        layout = Layout()

        master_stats = nodes[0][0].calc_stats(is_last)
        master_wait = 0
        if self.wait_for == "write":
            master_wait = self.color_chooser(nodes[0][0], master_stats.min_write_lsn, master_stats.min_wait_write_lsn) + f"min_write_lsn: {master_stats.min_write_lsn}[/]\n"
        elif self.wait_for == "flush":
            master_wait = self.color_chooser(nodes[0][0], master_stats.min_flush_lsn, master_stats.min_wait_flush_lsn) + f"min_flush_lsn: {master_stats.min_flush_lsn}[/]\n"
        elif self.wait_for == "apply":
            master_wait = self.color_chooser(nodes[0][0], master_stats.min_apply_lsn, master_stats.min_wait_apply_lsn) + f"min_apply_lsn: {master_stats.min_apply_lsn}[/]\n"

        rep_layout_list = [Layout(name=f"line{j}") for j in range(1, len(nodes))]

        j = 1
        node_views = []
        wait = 0
        receive = 0
        applied = 0
        for line in lines:
            for i in range(line):
                stats = nodes[j][i].calc_stats()

                if self.wait_for == "write":
                    wait = self.color_chooser(nodes[0][0], stats.min_write_lsn, master_stats.min_wait_write_lsn) + f"min_write_lsn: {stats.min_write_lsn}[/]\n"
                    receive = self.color_chooser(nodes[0][0], stats.last_receive_lsn, master_stats.min_wait_write_lsn, 1) + f"last_receive_lsn: {stats.last_receive_lsn}[/]\n"
                    applied = self.color_chooser(nodes[0][0], stats.last_applied_lsn, master_stats.min_wait_write_lsn, 1) + f"last_applied_lsn: {stats.last_applied_lsn}[/]\n"
                elif self.wait_for == "flush":
                    wait = self.color_chooser(nodes[0][0], stats.min_flush_lsn, master_stats.min_wait_flush_lsn) + f"min_flush_lsn: {stats.min_flush_lsn}[/]\n"
                    receive = self.color_chooser(nodes[0][0], stats.last_receive_lsn, master_stats.min_wait_flush_lsn, 1) + f"last_receive_lsn: {stats.last_receive_lsn}[/]\n"
                    applied = self.color_chooser(nodes[0][0], stats.last_applied_lsn, master_stats.min_wait_flush_lsn, 1) + f"last_applied_lsn: {stats.last_applied_lsn}[/]\n"
                elif self.wait_for == "apply":
                    wait = self.color_chooser(nodes[0][0], stats.min_apply_lsn, master_stats.min_wait_apply_lsn) + f"min_apply_lsn: {stats.min_apply_lsn}[/]\n"
                    receive = self.color_chooser(nodes[0][0], stats.last_receive_lsn, master_stats.min_wait_apply_lsn, 1) + f"last_receive_lsn: {stats.last_receive_lsn}[/]\n"
                    applied = self.color_chooser(nodes[0][0], stats.last_applied_lsn, master_stats.min_wait_apply_lsn, 1) + f"last_applied_lsn: {stats.last_applied_lsn}[/]\n"

                node_views.append(Panel(
                    f"[bold] Replica {i}[/]\n"
                    f"{wait}\n"
                    f"{receive}\n"
                    f"{applied}\n"
                    f"sent_lsn: {stats.sent_lsn}\n"
                    f"status: {nodes[j][i].state}\n"
                ))

            rep_layout_list[j - 1].split_row(*node_views)
            j += 1
            node_views.clear()
        
        rep_layout = Layout(name="replicas")
        rep_layout.split_column(*rep_layout_list)
        
        layout.split_row(
            Layout(
                Panel(
                    f"[bold] Master[/]\n"
                    f"{master_wait}\n"
                    f"min_wait_write_lsn: {master_stats.min_wait_write_lsn}\n"
                    f"min_wait_flush_lsn: {master_stats.min_wait_flush_lsn}\n"
                    f"min_wait_replay_lsn: {master_stats.min_wait_apply_lsn}\n"
                    f"sent_lsn: {master_stats.sent_lsn}\n"
                    f"status: {nodes[0][0].state}\n"
                ), name="master"
            ),
            rep_layout
        )

        return layout
    
    def color_chooser(self, master, lsn1, lsn2, mode=0):
        if master.calc_diff(lsn1, lsn2) >= 0:
            if mode == 0:
                return "[green]"
            else:
                return "[orange1]"
        else:
            if mode == 0:
                return "[red]"
            else:
                return "[gray]"
