#!/bin/bash

dbsz=5*1024*1024*1024 # 5GB
for b in 0 1
do
         for v in {7..8}
         do
             vsz=$((2**v))
             num=$((dbsz/vsz))
             ./benches-fillrandom.sh $b $num $vsz 1 1 >& r.b${b}.v${vsz}
         done
done

