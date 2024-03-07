#!/bin/bash

for run_workload in {a..f}
do
  java -cp jdbc-binding/lib/jdbc-binding-2.0.0-SNAPSHOT.jar:lib/mssql-jdbc-12.4.0.jre11.jar site.ycsb.db.JdbcDBCreateTable -P conf/db.properties -n usertable
  bin/ycsb load jdbc -P workloads/workload${run_workload} -P conf/db.properties -p recordcount=1000000 -p core_workload_insertion_retry_limit=10 -threads 32 -s > workload${run_workload}-load.dat
  bin/ycsb run jdbc -P workloads/workload${run_workload} -P conf/db.properties -p recordcount=1000000 -p operationcount=10000000 -p maxexecutiontime=300 -threads 256 -s > workload${run_workload}-run.dat
done
