#!/bin/sh
SCRIPTDIR=$(cd $(dirname $0) && pwd)
source $SCRIPTDIR/libcommon.sh
PRINT_USAGE="Usage: $0 -h host_name -w workload_file [ -u user_name -p password -b bucket_name ]"
USERNAME="Administrator"
PASSWORD="password"
BUCKET="ycsb"
SCENARIO=""
LOAD=0
RUN=0
RECORDCOUNT=1000000
OPCOUNT=10000000
THREADCOUNT_LOAD=32
THREADCOUNT_RUN=256
RUNTIME=180
MAXPARALLELISM=1
MEMOPT=0
INDEX_WORKLOAD="e"
TMP_OUTPUT=$(mktemp)

function create_bucket {
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -ne 0 ]; then
    if [ "$MEMOPT" -eq 0 ]; then
      MEMQUOTA=$(cbc admin -U couchbase://$HOST -u $USERNAME -P $PASSWORD /pools/default 2>/dev/null | jq -r '.memoryQuota')
    else
      MEMQUOTA=$MEMOPT
    fi
    cbc bucket-create -U couchbase://$HOST -u $USERNAME -P $PASSWORD --ram-quota $MEMQUOTA --num-replicas $REPL_NUM $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not create $BUCKET bucket."
       echo "Memory Quota: $MEMQUOTA"
       cat $TMP_OUTPUT
       exit 1
    fi
fi

sleep 1
}

function delete_bucket {
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1

if [ $? -eq 0 ]; then
    cbc bucket-delete -U couchbase://$HOST -u $USERNAME -P $PASSWORD $BUCKET >$TMP_OUTPUT 2>&1
    if [ $? -ne 0 ]; then
       echo "Can not delete ycsb bucket."
       cat $TMP_OUTPUT
       exit 1
    fi
fi

sleep 1
}

function create_index {
local QUERY_TEXT="CREATE INDEX record_id_index ON \`ycsb\`(\`record_id\`) WITH {\"num_replica\": 2};"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function drop_index {
local QUERY_TEXT="DROP INDEX record_id_index ON \`ycsb\` USING GSI;"
local retry_count=1

while [ "$retry_count" -le 3 ]; do
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -eq 0 ]; then
   break
else
   retry_count=$((retry_count + 1))
   sleep 2
fi
done
cbc stats -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD >/dev/null 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed. Bucket $BUCKET does not exist."
   exit 1
fi

cbc query -U couchbase://$HOST/$BUCKET -u $USERNAME -P $PASSWORD "$QUERY_TEXT" >$TMP_OUTPUT 2>&1
if [ $? -ne 0 ]; then
   echo "Query failed."
   echo "$QUERY_TEXT"
   cat $TMP_OUTPUT
   exit 1
fi

sleep 1
}

function run_load {
create_bucket
[ "$SCENARIO" = "$INDEX_WORKLOAD" ] && create_index
python2 bin/ycsb load couchbase3 \
	-P $WORKLOAD \
	-threads $THREADCOUNT_LOAD \
	-p couchbase.host=$HOST \
	-p couchbase.bucket=$BUCKET \
	-p couchbase.upsert=true \
	-p couchbase.kvEndpoints=4 \
	-p couchbase.epoll=true \
	-p couchbase.sslMode=none \
	-p couchbase.username=$USERNAME \
	-p couchbase.password=$PASSWORD \
	-p recordcount=$RECORDCOUNT \
	-s > ${WORKLOAD}-load.dat
}

function run_workload {
python2 bin/ycsb run couchbase3 \
	-P $WORKLOAD \
	-threads $THREADCOUNT_RUN \
	-p couchbase.host=$HOST \
	-p couchbase.bucket=$BUCKET \
	-p couchbase.upsert=true \
	-p couchbase.kvEndpoints=4 \
	-p couchbase.epoll=true \
	-p couchbase.sslMode=none \
	-p couchbase.maxParallelism=$MAXPARALLELISM \
	-p couchbase.username=$USERNAME \
	-p couchbase.password=$PASSWORD \
	-p recordcount=$RECORDCOUNT \
  -p operationcount=$OPCOUNT \
  -p maxexecutiontime=$RUNTIME \
	-s > ${WORKLOAD}-run.dat
[ "$SCENARIO" = "$INDEX_WORKLOAD" ] && drop_index
delete_bucket
}

while getopts "h:w:o:p:u:b:m:C:O:T:R:P:lr" opt
do
  case $opt in
    h)
      HOST=$OPTARG
      ;;
    w)
      SCENARIO=$OPTARG
      ;;
    o)
      SCENARIO=$OPTARG
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
    m)
      MEMOPT=$OPTARG
      ;;
    C)
      RECORDCOUNT=$OPTARG
      ;;
    O)
      OPCOUNT=$OPTARG
      ;;
    T)
      THREADCOUNT_RUN=$OPTARG
      THREADCOUNT_LOAD=$OPTARG
      ;;
    R)
      RUNTIME=$OPTARG
      ;;
    P)
      MAXPARALLELISM=$OPTARG
      ;;
    l)
      LOAD=1
      ;;
    r)
      RUN=1
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done

cd $SCRIPTDIR

[ -z "$HOST" ] && err_exit

[ -z "$PASSWORD" ] && get_password

echo "Testing against cluster node $HOST"
CLUSTER_VERSION=$(cbc admin -U couchbase://$HOST -u $USERNAME -P $PASSWORD /pools 2>/dev/null | sed -n -e 's/^.*ns_server":"\(.*\)","a.*$/\1/p')
if [ -z "$CLUSTER_VERSION" ]; then
  err_exit "Can not connect to Couchbase cluster at couchbase://$HOST"
fi
echo "Cluster version $CLUSTER_VERSION"
echo ""

if [ -z "$SCENARIO" ]; then
  for ycsb_workload in {a..f}
  do
    SCENARIO=$ycsb_workload
    WORKLOAD="workloads/workload${SCENARIO}"
    [ ! -f "$WORKLOAD" ] && err_exit "Workload file $WORKLOAD not found."
    echo "Running workload scenario YCSB-${SCENARIO}"
    run_load
    run_workload
  done
else
  WORKLOAD="workloads/workload${SCENARIO}"
  [ ! -f "$WORKLOAD" ] && err_exit "Workload file $WORKLOAD not found."
  echo "Running single workload scenario YCSB-${SCENARIO}"
  run_load
  run_workload
fi

if [ -d /output ]; then
   cp $SCRIPTDIR/workloads/*.dat /output
fi

rm $TMP_OUTPUT
##