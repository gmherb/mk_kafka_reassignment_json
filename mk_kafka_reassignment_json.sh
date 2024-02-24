#!/usr/bin/env bash

set -euo pipefail

readonly RACK_COUNT="2"
readonly SCRIPT_DESCRIPTION="Generate a JSON document for the \
kafka-reassign-partitions tool. Currently supports only [$RACK_COUNT] racks \
for a 2.5 cluster setup."

readonly DEFAULT_TOPIC_NAME="test-topic.v1"
readonly TOPIC_NAME=${1:-"$DEFAULT_TOPIC_NAME"}

readonly DEFAULT_RACK_PREFERENCE="distributed"
readonly RACK_PREFERENCE=${2:-"$DEFAULT_RACK_PREFERENCE"}

readonly DEFAULT_PARTITIONS="12"
readonly PARTITIONS=${3:-"$DEFAULT_PARTITIONS"}

readonly DEFAULT_REPLICATION_FACTOR="4"
readonly REPLICATION_FACTOR=${4:-"$DEFAULT_REPLICATION_FACTOR"}

readonly DEFAULT_RACK1_BROKERS="10 11 12"
readonly RACK1_BROKERS=${5:-"$DEFAULT_RACK1_BROKERS"}

readonly DEFAULT_RACK2_BROKERS="20 21 22"
readonly RACK2_BROKERS=${6:-"$DEFAULT_RACK2_BROKERS"}

function usage {
  echo "$SCRIPT_DESCRIPTION"
  echo "Usage: $0 <topic_name> <rack_preference> <partitions> \
<replication_factor> <rack1_name> <rack1_brokers> <rack2_name> \
<rack2_brokers>"
  echo "  topic_name:       Name of the topic to reassign \
partitions for (default: $DEFAULT_TOPIC_NAME)"
  echo "  rack_preference:  Rack to list first for leadership \
preference; rack1, rack2, or distributed (default: \
$DEFAULT_RACK_PREFERENCE)"
  echo "  partitions:       Number of partitions (default: \
$DEFAULT_PARTITIONS)"
  echo "  replica_factor:   Number of replicas (default: \
$DEFAULT_REPLICATION_FACTOR)"
  echo "  rack1_brokers:    Space separated list of brokers for rack 1 \
(default: $DEFAULT_RACK1_BROKERS)"
  echo "  rack2_brokers:    Space separated list of brokers for rack 2 \
(default: $DEFAULT_RACK2_BROKERS)"
  exit 1
}


HALF_REPL=$(echo $(( ${REPLICATION_FACTOR} / $RACK_COUNT )))
readonly HALF_REPL
HALF_PART=$(echo $(( ${PARTITIONS} / $RACK_COUNT )))
readonly HALF_PART

function main {
  case $RACK_PREFERENCE in
    "rack1")
      mk_document_header
      mk_rack1_preferred_reassignment_json
      ;;
    "rack2")
      mk_document_header
      mk_rack2_preferred_reassignment_json
      ;;
    "distributed")
      mk_document_header
      mk_distributed_reassignment_json
      ;;
    *)
      usage
      ;;
  esac
  mk_document_footer
}



function mk_document_header {
  cat <<EOF
{
  "version": 1,
  "partitions": [
EOF
}

function mk_partition_replica {
  local -r topic_name=$1
  local -r partition=$2
  local -r replicas=$3
  local -r comma=${4:-''}
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

function shuffle_rack1 {
  local -r count=$1
  shuffle "$count" "$RACK1_BROKERS"
}

function shuffle_rack2 {
  local -r count=$1
  shuffle "$count" "$RACK2_BROKERS"
}

function gen_replica_assignment_prefer_rack1 {
  shuffle_rack1 "$HALF_REPL" "$RACK1_BROKERS"
  shuffle_rack2 "$HALF_REPL" "$RACK2_BROKERS" \
    | sed 's/,$/\n/' 
}

function gen_replica_assignment_prefer_rack2 {
  shuffle_rack2 "$HALF_REPL" "$RACK2_BROKERS"
  shuffle_rack1 "$HALF_REPL" "$RACK1_BROKERS" \
    | sed 's/,$/\n/' 
}

function mk_distributed_reassignment_json {
  RACK1_PARTITIONS=$(seq 0 $(( ${HALF_PART} - 1 )))
  for p in $RACK1_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_rack1)
    mk_partition_replica $TOPIC_NAME $p $replica_assignment ","
  done

  RACK2_PARTITIONS=$(seq ${HALF_PART} $(( ${PARTITIONS} - 2 )))
  for p in $RACK2_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_rack2)
    mk_partition_replica $TOPIC_NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_rack2)
  mk_partition_replica $TOPIC_NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}

function mk_rack1_preferred_reassignment_json {
  RACK1_PARTITIONS=$(seq 0 $(( ${PARTITIONS} - 2 )))
  for p in $RACK1_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_rack1)
    mk_partition_replica $TOPIC_NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_rack1)
  mk_partition_replica $TOPIC_NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}

function mk_rack2_preferred_reassignment_json {
  RACK2_PARTITIONS=$(seq 0 $(( ${PARTITIONS} - 2 )))
  for p in $RACK2_PARTITIONS; do
    replica_assignment=$(gen_replica_assignment_prefer_rack2)
    mk_partition_replica $TOPIC_NAME $p $replica_assignment ","
  done

  # Last partition without comma
  replica_assignment=$(gen_replica_assignment_prefer_rack2)
  mk_partition_replica $TOPIC_NAME $(( ${PARTITIONS} - 1 )) $replica_assignment
}


main
