#!bin/bash

export LD_LIBRARY_PATH=/var/lib/postgresql/lib/

cd /var/lib/postgresql

./bin/pg_ctl stop -D /var/lib/postgres-data -o '-d 2'