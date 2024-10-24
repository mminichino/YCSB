#!/bin/bash
#
RESOURCE_GROUP="ycsb-rg"
THROUGHPUT=48000

err_exit() {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   fi
   echo "Operation exited with non-zero value"
   exit 1
}

az config param-persist off

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

exit 0
#
##
