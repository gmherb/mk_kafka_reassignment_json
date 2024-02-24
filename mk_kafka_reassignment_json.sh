#!/usr/bin/env bash

set -euo pipefail

# Generate a JSON document for the kafka-reassign-partitions tool
#
# Currently only supports 2 sites for a 2.5 cluster setup.
# min.insync.replicas=2 and acks=all is recommended for 2.5 clusters.

readonly PREFERENCE=${1:-"dist"}
readonly NAME=${2:-"test"}
readonly PARTITIONS=${3:-"9"}
readonly REPLICA_FACTOR=${4:-"4"}
readonly SITE1_BROKERS=${5:-"0 1 2"}
readonly SITE2_BROKERS=${6:-"10 11 12"}

readonly SITES="2"
half_repl=$(echo $(( ${REPLICA_FACTOR} / $SITES )))
readonly HALF_REPL=$half_repl
readonly half_repl
half_part=$(echo $(( ${PARTITIONS} / $SITES )))
readonly half_part
readonly HALF_PART=$half_part

function usage {
  echo "Usage: $0 <preferred_site> <topic_name> <partitions> <replica_factor> <site1_brokers> <site2_brokers>"
  echo "  preferred_site:  site1, site2, or dist for distributed"
  echo "  topic_name:      Name of the topic"
  echo "  partitions:      Number of partitions"
  echo "  replica_factor:  Number of replicas"
  echo "  site1_brokers:   Space separated list of brokers for site 1"
  echo "  site2_brokers:   Space separated list of brokers for site 2"
  exit 1
}

function mk_document_header {
  cat <<EOF
{
  "version": 1,
  "partitions": [
EOF
}

function mk_partition_replica {
  local topic_name=$1
  local partition=$2
  local replicas=$3
  local comma=${4:-''}
  cat <<EOF
    {
      "topic": "$topic_name",
      "partition": "$partition",
      "replicas": [${replicas}]
    }$comma
EOF
}

function mk_document_footer {
  cat <<EOF
  ]
}
EOF
}

function shuffle {
  local -r count=$1
  local -r broker_ids=$2

  shuf -n "$count" -e $broker_ids \
    | tr "\n" ','
}

function shuffle_site1 {
  local -r count=$1
  shuffle "$count" "$SITE1_BROKERS"
}

function shuffle_site2 {
  local -r count=$1
  shuffle "$count" "$SITE2_BROKERS"
}

function gen_replica_assignment_prefer_site1 {
  shuffle_site1 "$HALF_REPL" "$SITE1_BROKERS"
  shuffle_site2 "$HALF_REPL" "$SITE2_BROKERS" \
    | sed 's/,$/\n/' 
}

function gen_replica_assignment_prefer_site2 {
  shuffle_site2 "$HALF_REPL" "$SITE2_BROKERS"
  shuffle_site1 "$HALF_REPL" "$SITE1_BROKERS" \
    | sed 's/,$/\n/' 
}

function mk_distributed_reassignment_json {
  SITE1_PARTITIONS=$(seq 0 $(( ${half_part} - 1 )))
  for p in $SITE1_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_site1)
    mk_partition_replica $NAME $p $replica_assignment ","
  done

  SITE2_PARTITIONS=$(seq ${half_part} $(( ${PARTITIONS} - 2 )))
  for p in $SITE2_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_site2)
    mk_partition_replica $NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_site2)
  mk_partition_replica $NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}

function mk_site1_preferred_reassignment_json {
  SITE1_PARTITIONS=$(seq 0 $(( ${PARTITIONS} - 2 )))
  for p in $SITE1_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_site1)
    mk_partition_replica $NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_site1)
  mk_partition_replica $NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}

function mk_site2_preferred_reassignment_json {
  SITE2_PARTITIONS=$(seq 0 $(( ${PARTITIONS} - 2 )))
  for p in $SITE2_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_site2)
    mk_partition_replica $NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_site2)
  mk_partition_replica $NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}


# Main
mk_document_header
case $PREFERENCE in
  "site1")
    mk_site1_preferred_reassignment_json
    ;;
  "site2")
    mk_site2_preferred_reassignment_json
    ;;
  "dist")
    mk_distributed_reassignment_json
    ;;
  *)
    usage
    ;;
esac
mk_document_footer