#!/bin/bash
#
RESOURCE_GROUP="ycsb-rg"

err_exit() {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   fi
   echo "Operation exited with non-zero value"
   exit 1
}

az config param-persist off

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

exit 0
#
##
