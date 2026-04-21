#!/bin/bash

calculate_stats() {
    local values=("$@")
    local count=${#values[@]}
    
    if [[ $count -eq 0 ]]; then
        echo "0,0,0"
        return
    fi
    
    # Суммируем
    local sum=0
    for val in "${values[@]}"; do
        sum=$(echo "$sum + $val" | bc)
    done

    local max=${values[0]}
    for val in "${values[@]}"; do
        if (( $(echo "$val > $max" | bc -l) )); then
            max=$val
        fi
    done

    local avg=$(echo "scale=2; $sum / $count" | bc)
    
    echo "$max,$avg"
}


host=localhost

if [[ $# -eq 1 ]]; then
    host=$1
fi

psql -h $host -p 5432 -U ubuntu -d postgres -f $PWD/tests/sql/create_test_db.sql

tps_values=()
latency_values=()

for i in {1..10}; do
    result=$(pgbench -h $host -U ubuntu -d postgres -p 5432 -c 100 -j 2 \
        -t 1 -f $PWD/tests/sql/large_insert.sql 2>&1)
    tps=$(echo "$result" | grep -oP 'tps = \K[0-9.]+' | head -1)
    latency=$(echo "$result" | grep -oP 'latency average = \K[0-9.]+')

    if [[ -n "$tps" && -n "$latency" ]]; then
        tps_values+=($tps)
        latency_values+=($latency)
        echo "  TPS: $tps, Latency: ${latency}ms"
    else
        echo "  Error in parsing"
        echo "$result"
    fi
    echo ""
done

tps_stats=($(calculate_stats "${tps_values[@]}" | tr ',' ' '))
max_tps=${tps_stats[0]}
avg_tps=${tps_stats[1]}

latency_stats=($(calculate_stats "${latency_values[@]}" | tr ',' ' '))
max_latency=${latency_stats[0]}
avg_latency=${latency_stats[1]}

echo "=========================================="
echo "RESULTS (10 runs):"
echo "------------------------------------------"
echo "TPS:"
echo "  Max:          $max_tps"
echo "  Average:      $avg_tps"
echo "------------------------------------------"
echo "Latency (ms):"
echo "  Max:          $max_latency"
echo "  Average:      $avg_latency"
echo "=========================================="