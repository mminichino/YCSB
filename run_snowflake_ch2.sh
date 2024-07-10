#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/snowflake-binding/lib/*"
THREADCOUNT_LOAD=1
THREADCOUNT_RUN=10
RUNTIME=600
RUN_MODE=0
LOAD_MODE=0
LOAD_DRIVER="site.ycsb.db.snowflake.SnowflakeTPCLoad"
RUN_DRIVER="site.ycsb.db.snowflake.SnowflakeTPCRun"

while getopts "T:lr" opt
do
  case $opt in
    l)
      LOAD_MODE=1
      ;;
    r)
      RUN_MODE=1
      ;;
    T)
      RUNTIME=$OPTARG
      ;;
    \?)
      ;;
    esac
done

for workload in 16 32 64 128
do
  if [ -f "workloads/workload_ch2_${workload}" ]; then
    WORKLOAD_LIST="$WORKLOAD_LIST workloads/workload_ch2_${workload}"
  fi
done

if [ $LOAD_MODE -eq 1 ]; then
  LOAD_OPTS="-db $LOAD_DRIVER -P workloads/workload_ch2 -threads $THREADCOUNT_LOAD -s -load"
  java -cp "$CLASSPATH" site.ycsb.BenchClient $LOAD_OPTS
fi

if [ $RUN_MODE -eq 1 ]; then
  for workload in $WORKLOAD_LIST; do
    RUN_OPTS="-db $RUN_DRIVER -P $workload -p operationcount=0 -p maxexecutiontime=$RUNTIME -manual -s -t"
    java -cp "$CLASSPATH" site.ycsb.BenchClient $RUN_OPTS
  done
fi
