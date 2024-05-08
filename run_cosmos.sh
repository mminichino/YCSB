#!/bin/bash
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*:${SCRIPT_ROOT}/azurecosmos-binding/lib/*"
RESOURCE_GROUP=""
RECORDCOUNT=1000000
OPCOUNT=10000000
RUNTIME=180
THROUGHPUT=48000
PRINT_USAGE="Usage: $0 [ -r resource_group | -c record_count | -w workload ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

err_exit() {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   fi
   exit 1
}

while getopts "r:t:R:O:T:" opt
do
  case $opt in
    r)
      RESOURCE_GROUP=$OPTARG
      ;;
    t)
      THROUGHPUT=$OPTARG
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

RUN_THREADS=$(("$THROUGHPUT" / 1000))
echo "Run threads: $RUN_THREADS"

for run_workload in {a..f}
do
  echo "Creating database"
  az cosmosdb sql database create \
  --account-name ycsb \
  --resource-group "$RESOURCE_GROUP" \
  --name ycsb >/var/tmp/cosmos.log 2>&1
  [ $? -ne 0 ] && err_exit "Error creating database"

  echo "Creating container"
  az cosmosdb sql container create \
  --account-name ycsb \
  --resource-group "$RESOURCE_GROUP" \
  --database-name ycsb \
  --name usertable \
  --partition-key-path "/id" \
  --throughput "$THROUGHPUT" >/var/tmp/cosmos.log 2>&1
  [ $? -ne 0 ] && err_exit "Error creating container"

  workload="workloads/workload${run_workload}"
  LOAD_OPTS="-db site.ycsb.db.azurecosmos.AzureCosmosClient -P conf/db.properties -P $workload -threads $THREADCOUNT_LOAD -p recordcount=$RECORDCOUNT -s -load"
  RUN_OPTS="-db site.ycsb.db.azurecosmos.AzureCosmosClient -P conf/db.properties -P $workload -threads $THREADCOUNT_RUN -p recordcount=$RECORDCOUNT -p operationcount=$OPCOUNT -p maxexecutiontime=$RUNTIME -s -t"

  java -cp "$CLASSPATH" site.ycsb.Client $LOAD_OPTS
  java -cp "$CLASSPATH" site.ycsb.Client $RUN_OPTS

  echo "Deleting container"
  az cosmosdb sql container delete \
  --account-name ycsb \
  --resource-group "$RESOURCE_GROUP" \
  --database-name ycsb \
  --name usertable --yes >/var/tmp/cosmos.log 2>&1
  [ $? -ne 0 ] && err_exit "Error deleting container"

  echo "Deleting database"
  az cosmosdb sql database delete \
  --account-name ycsb \
  --resource-group "$RESOURCE_GROUP" \
  --name ycsb --yes >/var/tmp/cosmos.log 2>&1
  [ $? -ne 0 ] && err_exit "Error deleting database"
done
