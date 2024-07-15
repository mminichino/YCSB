#!/bin/sh
#
SCRIPT_PATH=$(dirname "$0")
SCRIPT_ROOT=$(cd "$SCRIPT_PATH/.." && pwd)
CLASSPATH="${SCRIPT_ROOT}/conf:${SCRIPT_ROOT}/lib/*"

for collection in "item" "warehouse" "stock" "district" "customer" "history" "orders" "new_orders" "order_line" "supplier" "nation" "region"
do
  java -cp "$CLASSPATH" site.ycsb.db.couchbase3.CouchbaseS3Export -p conf/db.properties -c $collection
done
