#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/jdbc-binding/lib/*"
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RECORDCOUNT=1000000
OPCOUNT=10000000
RUNTIME=180
PRINT_USAGE="Usage: $0 [ -T run_time -O operations -R record_count ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "R:O:T:" opt
do
  case $opt in
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
  LOAD_OPTS="-db site.ycsb.db.jdbc.JdbcDBClient -P conf/db.properties -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -p core_workload_insertion_retry_limit=10 -s -load"
  RUN_OPTS="-db site.ycsb.db.jdbc.JdbcDBClient -P conf/db.properties -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -s -t"

  java -cp "$CLASSPATH" site.ycsb.db.jdbc.JdbcDBCreateTable -P conf/db.properties -n usertable
  java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
  java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS
done
