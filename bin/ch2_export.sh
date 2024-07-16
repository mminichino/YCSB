#!/bin/sh
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*"
COLLECTION=""

while getopts "c:" opt
do
  case $opt in
    c)
      COLLECTION=$OPTARG
      ;;
    \?)
      ;;
    esac
done

for collection in "item" "warehouse" "stock" "district" "customer" "history" "orders" "new_orders" "order_line" "supplier" "nation" "region"
do
  if [ -n "$COLLECTION" ] && [ "$collection" != "$COLLECTION" ]; then
    continue
  fi
  java -Xmx16g -cp "$CLASSPATH" site.ycsb.db.couchbase3.CouchbaseExport -p conf/db.properties -c $collection
done
