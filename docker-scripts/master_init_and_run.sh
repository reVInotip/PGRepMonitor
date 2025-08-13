#!/bin/bash

/var/lib/postgresql/bin/initdb -D /var/lib/postgresql/data

/var/lib/postgresql/bin/postgres -D /var/lib/postgresql/data -d 2