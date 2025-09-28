#!/bin/bash

cd /var/lib/postgres_src

if [ $# -eq 0 ]; then
    echo "Default building"
    ./configure --prefix=/var/lib/postgresql --without-icu
elif [[ $# -eq 1 && "$1" = "-d" ]]; then
    echo "Build in debug mode"
    ./configure --prefix=/var/lib/postgresql --without-icu --enable-cassert --enable-debug CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"
else
    echo "Invlid args (should be empty or -d)"
    exit 1
fi

make -j4 world
make -j4 install-world