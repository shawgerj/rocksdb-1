#!/bin/bash

blvd=$1
num=$2
valuesz=$3
threads=$4
batchsz=$5

rm /tmp/vlog.txt; rm -rf /tmp/rocksdb*

# from OptimizeLevelStyleCompaction
memtable_memory_budget=$((512*1024*1024))
./build/db_bench --db=/nobackup/db --benchmarks=fillrandom,stats --use_boulevardier=$blvd --num=$num --value_size=$valuesz --compression_type=none --write_buffer_size=$(($memtable_memory_budget/4)) --max_write_buffer_number=6 --target_file_size_base=$(($memtable_memory_budget/8)) --max_bytes_for_level_base=$memtable_memory_budget --stats_interval_seconds=60 --histogram=1 --level0_file_num_compaction_trigger=2 --max_background_compactions=2 --max_background_flushes=2 --threads=$threads --batch_size=$batchsz --disable_wal=$blvd --perf_level=3
