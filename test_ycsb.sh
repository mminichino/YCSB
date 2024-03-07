#!/usr/bin/env bash

PRINT_USAGE="Usage: $0 -h host_name [ -h | -u | -p | -b | -c | -s | -S ]
              -h Connect host name
              -u User name
              -p Password
              -b Bucket name
              -c Collection name
              -s Scope name
              -S Do not use SSL"
USERNAME="Administrator"
PASSWORD="password"
BUCKET="ycsb"
SCOPE="_default"
COLLECTION="_default"
SCENARIO=""
CURRENT_SCENARIO=""
LOAD=1
RUN=1
RECORDCOUNT=1000
OPCOUNT=10000000
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RUNTIME=30
MAXPARALLELISM=1
MEMOPT=768
INDEX_WORKLOAD="e"
TMP_OUTPUT=$(mktemp)
REPL_NUM=1
MANUALMODE=0
SSLMODE="true"
CONTYPE="couchbases"
CONOPTIONS="?ssl=no_verify"
BYPASS=0

print_usage() {
if [ -n "$PRINT_USAGE" ]; then
   echo "$PRINT_USAGE"
fi
}

err_exit() {
   if [ -n "$1" ]; then
      echo "[!] Error: $1"
   else
      print_usage
   fi
   exit 1
}

function create_bucket {
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -ne 0 ]; then
    if [ "$MEMOPT" -eq 0 ]; then
      MEMQUOTA=$(cbc admin -U ${CONTYPE}://${HOST}${CONOPTIONS} -u $USERNAME -P $PASSWORD /pools/default 2>/dev/null | jq -r '.memoryQuota')
    else
      MEMQUOTA=$MEMOPT
    fi
    if [ -z "$MEMQUOTA" ]; then
      echo "Can not get cluster statistics. Is the cluster available?"
      exit 1
    fi
    cbc bucket-create -U ${CONTYPE}://${HOST}${CONOPTIONS} -u $USERNAME -P $PASSWORD --ram-quota $MEMQUOTA --num-replicas $REPL_NUM $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not create $BUCKET bucket."
       echo "Memory Quota: $MEMQUOTA"
       cat $TMP_OUTPUT
       exit 1
    fi
    if [ -n "$(grep ^4 $TMP_OUTPUT)" ]; then
      echo "Insufficient permission to create bucket $BUCKET please create it manually."
      exit 1
    fi
fi

sleep 1
}

function create_scope() {
local QUERY_TEXT="CREATE SCOPE ${BUCKET}.${SCOPE} IF NOT EXISTS;"

if [ "$SCOPE" = "_default" ]; then
  return
fi

cbc query -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi
}

function create_collection() {
local QUERY_TEXT="CREATE COLLECTION ${BUCKET}.${SCOPE}.${COLLECTION} IF NOT EXISTS;"

if [ "$COLLECTION" = "_default" ]; then
  return
fi

cbc query -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi
}

function delete_bucket {
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -eq 0 ]; then
    cbc bucket-delete -U ${CONTYPE}://${HOST}${CONOPTIONS} -u $USERNAME -P $PASSWORD $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not delete ycsb bucket."
       cat $TMP_OUTPUT
       exit 1
    fi
fi

sleep 1
}

function create_index {
local QUERY_TEXT="CREATE INDEX record_id_${BUCKET} ON \`${BUCKET}\`.${SCOPE}.${COLLECTION}(\`record_id\`) WITH {\"num_replica\": 1};"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function drop_index {
local QUERY_TEXT="DROP INDEX record_id_${BUCKET} ON \`${BUCKET}\`.${SCOPE}.${COLLECTION} USING GSI;"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U ${CONTYPE}://${HOST}/${BUCKET}${CONOPTIONS} -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function run_load {
[ "$MANUALMODE" -eq 0 ] && create_bucket && create_scope && create_collection
[ "$CURRENT_SCENARIO" = "$INDEX_WORKLOAD" ] && [ "$MANUALMODE" -eq 0 ] && create_index
bin/ycsb load couchbase3 \
	-P $WORKLOAD \
	-threads $THREADCOUNT_LOAD \
	-p couchbase.host=$HOST \
	-p couchbase.bucket=$BUCKET \
	-p couchbase.scope=$SCOPE \
	-p couchbase.collection=$COLLECTION \
	-p couchbase.upsert=true \
	-p couchbase.kvEndpoints=4 \
	-p couchbase.sslMode=$SSLMODE \
	-p couchbase.username=$USERNAME \
	-p couchbase.password=$PASSWORD \
	-p recordcount=$RECORDCOUNT \
	-s > ${WORKLOAD}-load.dat
}

function run_workload {
bin/ycsb run couchbase3 \
	-P $WORKLOAD \
	-threads $THREADCOUNT_RUN \
	-p couchbase.host=$HOST \
	-p couchbase.bucket=$BUCKET \
	-p couchbase.scope=$SCOPE \
  -p couchbase.collection=$COLLECTION \
	-p couchbase.upsert=true \
	-p couchbase.kvEndpoints=4 \
	-p couchbase.sslMode=$SSLMODE \
	-p couchbase.maxParallelism=$MAXPARALLELISM \
	-p couchbase.username=$USERNAME \
	-p couchbase.password=$PASSWORD \
	-p recordcount=$RECORDCOUNT \
  -p operationcount=$OPCOUNT \
  -p maxexecutiontime=$RUNTIME \
	-s > ${WORKLOAD}-run.dat
[ "$CURRENT_SCENARIO" = "$INDEX_WORKLOAD" ] && [ "$MANUALMODE" -eq 0 ] && drop_index
[ "$MANUALMODE" -eq 0 ] && delete_bucket
}

while getopts "h:u:p:b:c:s:S" opt
do
  case $opt in
    h)
      HOST=$OPTARG
      ;;
    u)
      USERNAME=$OPTARG
      ;;
    p)
      PASSWORD=$OPTARG
      ;;
    b)
      BUCKET=$OPTARG
      ;;
    c)
      COLLECTION=$OPTARG
      ;;
    s)
      SCOPE=$OPTARG
      ;;
    S)
      SSLMODE="none"
      CONTYPE="couchbase"
      CONOPTIONS=""
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done

which jq >/dev/null 2>&1
[ $? -ne 0 ] && err_exit "This utility requires jq."

which cbc >/dev/null 2>&1
[ $? -ne 0 ] && err_exit "This utility requires cbc from libcouchbase."

[ -z "$HOST" ] && err_exit

[ -z "$PASSWORD" ] && get_password

ping -c 1 $HOST >/dev/null 2>&1
[ $? -ne 0 ] && warn_msg "$HOST is not pingable."

nslookup -type=SRV _${CONTYPE}._tcp.${HOST} >/dev/null 2>&1
if [ $? -eq 0 ]; then
  info_msg "SRV records found:"
  nslookup -type=SRV _${CONTYPE}._tcp.${HOST} | grep service | awk '{print $NF}' | sed -e 's/\.$//'
fi

echo "Testing against cluster node $HOST"
if [ "$BYPASS" -eq 0 ]; then
  CLUSTER_VERSION=$(cbc admin -U ${CONTYPE}://${HOST}${CONOPTIONS} -u $USERNAME -P $PASSWORD /pools 2>/dev/null | jq -r '.componentsVersion.ns_server')
  if [ -z "$CLUSTER_VERSION" ]; then
    err_exit "Can not connect to Couchbase cluster at ${CONTYPE}://$HOST"
  fi
  echo "Cluster version $CLUSTER_VERSION"
  echo ""
else
  if [ "$MANUALMODE" -ne 1 ]; then
    err_exit "Automation bypass is only supported in Manual mode."
  fi
  echo "Automation bypassed: buckets and required indexes will need to be manually created, and no checks are performed."
fi

if [ -z "$SCENARIO" ]; then
  for ycsb_workload in {a..f}
  do
    SCENARIO="$SCENARIO $ycsb_workload"
  done
fi

for run_workload in $SCENARIO
do
  WORKLOAD="workloads/workload${run_workload}"
  CURRENT_SCENARIO=${run_workload}
  [ ! -f "$WORKLOAD" ] && err_exit "Workload file $WORKLOAD not found."
  echo "Running workload scenario YCSB-${run_workload} file $WORKLOAD"
  run_load
  run_workload
done

rm $TMP_OUTPUT
##
