#!/bin/sh
SCRIPTDIR=$(cd $(dirname $0) && pwd)
source $SCRIPTDIR/libcommon.sh
PRINT_USAGE="Usage: $0 -h host_name -w workload_file [ -u user_name -p password -b bucket_name ]"
USERNAME="Administrator"
PASSWORD="password"
BUCKET="ycsb"
WORKLOAD="workloads/workloada"
LOAD=1
RUN=0
RECORDCOUNT=1000000
OPCOUNT=10000000
THREADCOUNT=32
RUNTIME=180
MAXPARALLELISM=8

function run_load {
python2 bin/ycsb load couchbase3 \
	-P $WORKLOAD \
	-threads $THREADCOUNT \
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
	-threads $THREADCOUNT \
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
}

while getopts "h:w:p:u:b:C:O:T:R:P:lr" opt
do
  case $opt in
    h)
      HOST=$OPTARG
      ;;
    w)
      WORKLOAD=$OPTARG
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
    C)
      RECORDCOUNT=$OPTARG
      ;;
    O)
      OPCOUNT=$OPTARG
      ;;
    T)
      THREADCOUNT=$OPTARG
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
      LOAD=0
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done

[ -z "$HOST" -o -z "$WORKLOAD" ] && err_exit
[ ! -f "$WORKLOAD" ] && err_exit "Workload file $WORKLOAD not found."

[ -z "$PASSWORD" ] && get_password

if [ "$LOAD" -eq 1 ]; then
  run_load
else
  run_workload
fi
