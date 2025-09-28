#!/bin/bash

cd /var/lib/postgres-data

echo "wal_level = replica" >> ./postgresql.conf
echo "max_wal_senders = 10" >> ./postgresql.conf

if [[ "$1" = "True" ]]; then
    echo "log_replication_commands = on" >> ./postgresql.conf
    echo "log_min_messages = debug2" >> ./postgresql.conf
    echo "logging_collector = on" >> ./postgresql.conf
    echo "log_destination = 'stderr'" >> ./postgresql.conf
fi