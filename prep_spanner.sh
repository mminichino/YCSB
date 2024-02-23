#!/bin/bash
INSTANCE="perf-test-1"
DATABASE="testdb"
REGION="us-central1"
NODES=3
CREATE=1
PRINT_USAGE="Usage: $0 [ -x ] | [ -i instance -d database -r region -n nodes ]"

function print_usage {
  if [ -n "$PRINT_USAGE" ]; then
     echo "$PRINT_USAGE"
  fi
}

function create_instance {
  echo "Creating Spanner instance ${INSTANCE}"
  gcloud spanner instances create "${INSTANCE}" --config=regional-"${REGION}" --description="${INSTANCE}" --nodes="${NODES}"

  echo "Creating Spanner database ${DATABASE} in instance ${INSTANCE}"
  gcloud spanner databases create "${DATABASE}" --instance="${INSTANCE}"
}

function remove_instance {
  echo "Deleting Spanner database ${DATABASE} in instance ${INSTANCE}"
  gcloud spanner databases delete "${DATABASE}" --instance="${INSTANCE}" -q

  echo "Deleting Spanner instance ${INSTANCE}"
  gcloud spanner instances delete "${INSTANCE}" -q
}

while getopts "i:d:r:n:x" opt
do
  case $opt in
    i)
      INSTANCE=$OPTARG
      ;;
    d)
      DATABASE=$OPTARG
      ;;
    r)
      REGION=$OPTARG
      ;;
    n)
      NODES=$OPTARG
      ;;
    x)
      CREATE=0
      ;;
    \?)
      print_usage
      exit 1
      ;;
  esac
done
shift $((OPTIND -1))

if [ "${CREATE}" -eq 1 ]; then
  create_instance
else
  remove_instance
fi
