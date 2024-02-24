#!/bin/bash
#
DATABASE="testdb"
REGION="us-central1"
CREATE=1
PRINT_USAGE="Usage: $0 [ -x ] | [ -d database -r region ]"

function print_usage {
  if [ -n "$PRINT_USAGE" ]; then
     echo "$PRINT_USAGE"
  fi
}

function create_instance {
  echo "Creating Firestore database ${DATABASE}"
  gcloud firestore databases create --location="${REGION}" --database="${DATABASE}"
}

function remove_instance {
  echo "Deleting Firestore database ${DATABASE}"
  gcloud firestore databases delete --database="${DATABASE}" -q
}

while getopts "d:r:x" opt
do
  case $opt in
    d)
      DATABASE=$OPTARG
      ;;
    r)
      REGION=$OPTARG
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
