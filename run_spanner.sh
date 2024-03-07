#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
INSTANCE="perf-test-1"
DATABASE="testdb"
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/cloudspanner-binding/lib/*"
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RECORDCOUNT=1000000
OPCOUNT=10000000
RUNTIME=180
PRINT_USAGE="Usage: $0 [ -i instance -d database ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "i:d:R:O:T:" opt
do
  case $opt in
    i)
      INSTANCE=$OPTARG
      ;;
    d)
      DATABASE=$OPTARG
      ;;
    R)
      RECORDCOUNT=$OPTARG
      ;;
    O)
      OPCOUNT=$OPTARG
      ;;
    T)
      RUNTIME=$OPTARG
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
  workload="workloads/workload${run_workload}"
  LOAD_OPTS="-db site.ycsb.db.cloudspanner.CloudSpannerClient -P conf/cloudspanner.properties -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -p cloudspanner.batchinserts=1000 -s -load"
  RUN_OPTS="-db site.ycsb.db.cloudspanner.CloudSpannerClient -P conf/cloudspanner.properties -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -s -t"

  gcloud spanner databases ddl update "$DATABASE" --instance="$INSTANCE" --ddl-file=conf/create.sql
  java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
  java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS
  gcloud spanner databases ddl update "$DATABASE" --instance="$INSTANCE" --ddl-file=conf/drop.sql
done
