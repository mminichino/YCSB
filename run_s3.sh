#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/s3-binding/lib/*"
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RECORDCOUNT=100000
OPCOUNT=1000000
RUNTIME=180
FIELDLENGTH=16
FIELDCOUNT=32
run_workload="a"
PRINT_USAGE="Usage: $0 [ -T run_time -O operations -R record_count ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "R:O:T:w:sml" opt
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
    w)
      run_workload=$OPTARG
      ;;
    s)
      FIELDLENGTH=16
      FIELDCOUNT=32
      ;;
    m)
      FIELDLENGTH=512
      FIELDCOUNT=256
      ;;
    l)
      FIELDLENGTH=1024
      FIELDCOUNT=1024
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

workload="workloads/workload${run_workload}"
LOAD_OPTS="-db site.ycsb.db.s3.S3Client -P conf/s3.properties -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -p fieldlength=$FIELDLENGTH -p fieldcount=$FIELDCOUNT -s -load"
RUN_OPTS="-db site.ycsb.db.s3.S3Client -P conf/s3.properties -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -p fieldlength=$FIELDLENGTH -p fieldcount=$FIELDCOUNT -s -t"

java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS
