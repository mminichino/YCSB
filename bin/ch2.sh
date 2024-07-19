#!/bin/sh
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*"
RUNTIME=600
RUN_MODE=0
LOAD_MODE=0
TEST_MODE=0
QUERY_PRINT="false"
CBS_DRIVER_LOAD="site.ycsb.db.couchbase3.CouchbaseTPCLoad"
COLUMNAR_DRIVER_LOAD="site.ycsb.db.couchbase3.AnalyticsTPCLoad"
CBS_DRIVER_RUN="site.ycsb.db.couchbase3.CouchbaseTPCRun"
COLUMNAR_DRIVER_RUN="site.ycsb.db.couchbase3.AnalyticsTPCRun"
LOAD_DRIVER=$COLUMNAR_DRIVER_LOAD
RUN_DRIVER=$COLUMNAR_DRIVER_RUN

while getopts "T:lprtS" opt
do
  case $opt in
    l)
      LOAD_MODE=1
      ;;
    p)
      QUERY_PRINT="true"
      ;;
    r)
      RUN_MODE=1
      ;;
    t)
      TEST_MODE=1
      ;;
    T)
      RUNTIME=$OPTARG
      ;;
    S)
      LOAD_DRIVER=$CBS_DRIVER_LOAD
      RUN_DRIVER=$CBS_DRIVER_RUN
      ;;
    \?)
      ;;
    esac
done

if [ $LOAD_MODE -eq 1 ]; then
  LOAD_OPTS="-db $LOAD_DRIVER -P workloads/workload_ch2 -threads 1 -load"
  java -cp "$CLASSPATH" site.ycsb.BenchClient $LOAD_OPTS
fi

if [ $TEST_MODE -eq 1 ]; then
  RUN_OPTS="-db $RUN_DRIVER -P workloads/workload_ch2 -threads 1 -p operationcount=1 -p maxexecutiontime=0 -p benchmark.queryPrint=$QUERY_PRINT -test -manual -t"
  java -cp "$CLASSPATH" site.ycsb.BenchClient $RUN_OPTS
fi

if [ $RUN_MODE -eq 1 ]; then
  for THREADCOUNT_RUN in 1 3 8; do
    RUN_OPTS="-db $RUN_DRIVER -P workloads/workload_ch2_${THREADCOUNT_RUN} -threads $THREADCOUNT_RUN -p operationcount=0 -p maxexecutiontime=$RUNTIME -manual -s -t"
    java -cp "$CLASSPATH" site.ycsb.BenchClient $RUN_OPTS
  done
fi
