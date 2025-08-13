class PostgresStats:
    def __init__(self):
        self.min_write_lsn = 0
        self.min_flush_lsn = 0
        self.min_apply_lsn = 0
        self.sent_lsn = 0
        self.min_wait_write_lsn = 0 
        self.min_wait_flush_lsn = 0 
        self.min_wait_apply_lsn = 0
        self.last_receive_lsn = 0
        self.last_applied_lsn = 0
