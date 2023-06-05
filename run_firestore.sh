#!/bin/bash

for run_workload in {a..f}
do
  bin/ycsb load googlefirestore -P workloads/workload${run_workload} -P conf/googlefirestore.properties -p recordcount=1000000 -p core_workload_insertion_retry_limit=10 -threads 32 -s > workload${run_workload}-load.dat
  bin/ycsb run googlefirestore -P workloads/workload${run_workload} -P conf/googlefirestore.properties -p recordcount=1000000 -p operationcount=10000000 -p maxexecutiontime=180 -threads 256 -s > workload${run_workload}-run.dat
done
