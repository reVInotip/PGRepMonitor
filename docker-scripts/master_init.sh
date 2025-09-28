#!/bin/bash

export LD_LIBRARY_PATH=/var/lib/postgresql/lib/

cd /var/lib/postgresql

./bin/initdb -D /var/lib/postgres-data

echo "host all all all trust" >> /var/lib/postgres-data/pg_hba.conf
echo "host replication all all trust" >> /var/lib/postgres-data/pg_hba.conf
echo "listen_addresses = '*'" >> /var/lib/postgres-data/postgresql.conf