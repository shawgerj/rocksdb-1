#!/bin/bash

blvd=$1
num=$2
valuesz=$3
threads=$4
batchsz=$5

rm /mydata2/wal/vlog.txt; rm -rf /mydata2/bench/*
sleep 2

./build/db_bench --db=/mydata2/bench --wal_dir=/mydata2/wal --benchmarks=fillrandom,stats --use_boulevardier=$blvd --num=$num --value_size=$valuesz --compression_type=none --write_buffer_size=134217728 --max_write_buffer_number=16 --target_file_size_base=33554432 --max_bytes_for_level_base=536870912 --max_bytes_for_level_multiplier=8 --stats_interval_seconds=60 --histogram=1 --level0_file_num_compaction_trigger=4 --level0_slowdown_writes_trigger=12 --level0_stop_writes_trigger=20 --max_background_compactions=16 --max_background_flushes=7 --threads=$threads --batch_size=$batchsz --disable_wal=$blvd --perf_level=3 --max_background_jobs=30 --writes=$(($num / $threads))
