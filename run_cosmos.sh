#!/bin/bash
RESOURCE_GROUP=""
RECORD_COUNT=1000000
WORKLOAD_START="a"
WORKLOAD_END="f"
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

while getopts "r:c:w:t:" opt
do
  case $opt in
    r)
      RESOURCE_GROUP=$OPTARG
      ;;
    c)
      RECORD_COUNT=$OPTARG
      ;;
    w)
      WORKLOAD_START=$OPTARG
      WORKLOAD_END=$OPTARG
      ;;
    t)
      THROUGHPUT=$OPTARG
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

for run_workload in $(eval echo "{$WORKLOAD_START..$WORKLOAD_END}")
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

  ./bin/ycsb load azurecosmos -P conf/azurecosmos.properties \
  -P "workloads/workload${run_workload}" \
  -p recordcount="$RECORD_COUNT" \
  -p core_workload_insertion_retry_limit=10 \
  -threads $RUN_THREADS \
  -s > "workload${run_workload}-load.dat"
  ./bin/ycsb run azurecosmos -P conf/azurecosmos.properties \
  -P "workloads/workload${run_workload}" \
  -p recordcount="$RECORD_COUNT" \
  -p operationcount=10000000 \
  -threads $RUN_THREADS \
  -p maxexecutiontime=180 \
  -s > "workload${run_workload}-run.dat"

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
