#!/bin/sh
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH=${SCRIPT_ROOT}/conf
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RECORDCOUNT=1000000
OPCOUNT=10000000
RUNTIME=180
RUN_MODE=0
LOAD_MODE=0
WORKLOAD=""

NEW_PATH=$(find "$SCRIPT_ROOT" -name \*.jar -type f |
{
while IFS= read -r line; do
    export CLASSPATH=${CLASSPATH}:$line
done
echo "$CLASSPATH"
})

CLASSPATH=$NEW_PATH

while getopts "w:R:O:T:lr" opt
do
  case $opt in
    w)
      if [ -f "$OPTARG" ]; then
        WORKLOAD=$OPTARG
      else
        echo "File $OPTARG does not exist"
      fi
      ;;
    l)
      LOAD_MODE=1
      ;;
    r)
      RUN_MODE=1
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
      ;;
    esac
done

if [ -z "$WORKLOAD" ]; then
  for workload in a b c d e f
  do
    if [ -f "workloads/workload${workload}" ]; then
      WORKLOAD_LIST="$WORKLOAD_LIST workloads/workload${workload}"
    fi
  done
else
  WORKLOAD_LIST=$WORKLOAD
fi

for workload in $WORKLOAD_LIST; do
  LOAD_OPTS="-db site.ycsb.db.couchbase3.Couchbase3Client -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -s -load"
  RUN_OPTS="-db site.ycsb.db.couchbase3.Couchbase3Client -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -s -t"
  [ $RUN_MODE -eq 0 ] && java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
  [ $LOAD_MODE -eq 0 ] && java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS
done
