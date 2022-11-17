#!/bin/bash

dir="results/batches"
dbsz=20*1024*1024*1024 # 20GB
for b in 0 1
do
         for v in {0..4}
         do
             bsz=$((2**v))
             num=$((dbsz/1024))
             ./benches-fillrandom.sh $b $num 1024 1 $bsz >& $dir/r.b${b}.batch${bsz}
	     sleep 2
         done
done

