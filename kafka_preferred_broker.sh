#!/usr/bin/env bash

set -euo pipefail

# Generate a JSON document for the kafka-reassign-partitions tool
#
# Currently only supports 2 sites for a 2.5 cluster setup.
# min.insync.replicas=2 and acks=all is recommended for 2.5 clusters.

readonly NAME=${1:-"test"}
readonly PARTITIONS=${2:-"9"}
readonly REPLICA_FACTOR=${3:-"4"}
readonly SITE1_BROKERS=${4:-"0 1 2"}
readonly SITE2_BROKERS=${5:-"10 11 12"}

readonly SITES="2"
half_repl=$(echo $(( ${REPLICA_FACTOR} / $SITES )))
readonly half_repl
half_part=$(echo $(( ${PARTITIONS} / $SITES )))
readonly half_part

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

mk_document_header $NAME

# Site 1 leaders
seq=$(seq 0 $(( ${half_part} - 1 )))
for p in $seq; do
  #echo "Partition: ${p}"

  replica_assignment=$(\
    shuf -n "${half_repl}" -e ${SITE1_BROKERS} | tr "\n" ','; \
    shuf -n "${half_repl}" -e ${SITE2_BROKERS} | tr "\n" ',' | sed 's/,$/\n/' \
  )
  mk_partition_replica $NAME $p $replica_assignment ","

done # Loop through each partition

# Site 2 leaders
seq=$(seq ${half_part} $(( ${PARTITIONS} - 2 )))
for p in $seq; do
  #echo "Partition: ${p}"

  replica_assignment=$(\
    shuf -n "${half_repl}" -e ${SITE2_BROKERS} | tr "\n" ','; \
    shuf -n "${half_repl}" -e ${SITE1_BROKERS} | tr "\n" ',' | sed 's/,$/\n/' \
  )
  mk_partition_replica $NAME $p $replica_assignment ","

done # Loop through each partition

replica_assignment=$(\
  shuf -n "${half_repl}" -e ${SITE2_BROKERS} | tr "\n" ','; \
  shuf -n "${half_repl}" -e ${SITE1_BROKERS} | tr "\n" ',' | sed 's/,$/\n/' \
)
mk_partition_replica $NAME $(( ${PARTITIONS} -1 )) $replica_assignment

mk_document_footer
