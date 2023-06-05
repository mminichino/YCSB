#!/bin/bash
INSTANCE="perf-test-1"
DATABASE="testdb"
PRINT_USAGE="Usage: $0 [ -i instance -d database ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "i:d:" opt
do
  case $opt in
    i)
      INSTANCE=$OPTARG
      ;;
    d)
      DATABASE=$OPTARG
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

for run_workload in {a..f}
do
  gcloud spanner databases ddl update "$DATABASE" --instance="$INSTANCE" --ddl-file=conf/create.sql
  ./bin/ycsb load cloudspanner -P conf/cloudspanner.properties -P "workloads/workload${run_workload}" -p recordcount=1000000 -p cloudspanner.batchinserts=1000 -threads 32 -s > "${run_workload}-load.dat"
  ./bin/ycsb run cloudspanner -P conf/cloudspanner.properties -P "workloads/workload${run_workload}" -p recordcount=1000000 -p operationcount=10000000 -threads 256 -p maxexecutiontime=180 -s > "${run_workload}-run.dat"
  gcloud spanner databases ddl update "$DATABASE" --instance="$INSTANCE" --ddl-file=conf/drop.sql
done
