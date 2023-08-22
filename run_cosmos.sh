#!/bin/bash
RESOURCE_GROUP=""
RECORD_COUNT=1000000
WORKLOAD_START="a"
WORKLOAD_END="f"
PRINT_USAGE="Usage: $0 [ -r resource_group | -c record_count | -w workload ]"

function print_usage {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

while getopts "r:c:w:" opt
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
    \?)
      print_usage
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

for run_workload in $(eval echo "{$WORKLOAD_START..$WORKLOAD_END}")
do
  az cosmosdb sql database create --account-name ycsb --resource-group "$RESOURCE_GROUP" --name ycsb
  az cosmosdb sql container create --account-name ycsb --resource-group "$RESOURCE_GROUP" --database-name ycsb --name usertable --partition-key-path "/id" --throughput 4000
  ./bin/ycsb load azurecosmos -P conf/azurecosmos.properties -P "workloads/workload${run_workload}" -p recordcount="$RECORD_COUNT" -p core_workload_insertion_retry_limit=10 -threads 16 -s > "${run_workload}-load.dat"
  ./bin/ycsb run azurecosmos -P conf/azurecosmos.properties -P "workloads/workload${run_workload}" -p recordcount="$RECORD_COUNT" -p operationcount=10000000 -threads 256 -p maxexecutiontime=180 -s > "${run_workload}-run.dat"
  az cosmosdb sql container delete --account-name ycsb --resource-group "$RESOURCE_GROUP" --database-name ycsb --name usertable --yes
  az cosmosdb sql database delete --account-name ycsb --resource-group "$RESOURCE_GROUP" --name ycsb --yes
done
