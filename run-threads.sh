#!/bin/bash

dbsz=20*1024*1024*1024 # 20GB

dir="results/threads"
mkdir -p $dir
for b in 0 1
do
         for t in {0..4}
         do
             tr=$((2**t))
             num=$((dbsz/1024))
             ./benches-fillrandom.sh $b $num 1024 $tr 1 >& $dir/r.b${b}.tr${tr}
	     sleep 2
         done
done

