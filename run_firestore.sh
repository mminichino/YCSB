#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/googlefirestore-binding/lib/*"
DATABASE="testdb"
REGION="us-central1"
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RECORDCOUNT=1000000
OPCOUNT=10000000
RUNTIME=180
PRINT_USAGE="Usage: $0 [ -d database -r region -T run_time -O operations -R record_count ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "d:r:R:O:T:" opt
do
  case $opt in
    d)
      DATABASE=$OPTARG
      ;;
    r)
      REGION=$OPTARG
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

gcloud config set compute/region "$REGION"

for run_workload in {a..f}
do
  workload="workloads/workload${run_workload}"
  LOAD_OPTS="-db site.ycsb.db.firestore.GoogleFirestoreClient -P conf/googlefirestore.properties -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -p core_workload_insertion_retry_limit=10 -s -load"
  RUN_OPTS="-db site.ycsb.db.firestore.GoogleFirestoreClient -P conf/googlefirestore.properties -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -s -t"

  echo "Creating Firestore database ${DATABASE}"
  gcloud firestore databases create --location="${REGION}" --database="${DATABASE}"
  sleep 1
  java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
  java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS
  echo "Deleting Firestore database ${DATABASE}"
  gcloud firestore databases delete --database="${DATABASE}" -q
  sleep 360
done
