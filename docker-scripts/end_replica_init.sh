#!/bin/bash

export LD_LIBRARY_PATH=/var/lib/postgresql/lib/

cd /var/lib/postgresql

./bin/pg_basebackup -h $1 -p $2 -P -C -S $3 -v -D /var/lib/postgres-data -R

echo -n "primary_conninfo = '" >> /var/lib/postgres-data/postgresql.auto.conf
echo "$4'" >> /var/lib/postgres-data/postgresql.auto.conf