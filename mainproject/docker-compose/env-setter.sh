#!/bin/bash
# Run me as env-setter.sh (partitionManager|router|seedNode|clusterUp|updater)

echo "Looking for replica id"

MYIP=$(ip addr show eth0 | grep inet[^6] | sed 's/.*inet \(.*\)\/[0-9]* \(.* \)*scope.*/\1/')
REPLICA_ID=1
PREFIX=raphtory_$1
# Loop until DNS resolution of $PREFIX_$REPLICA_ID return $MYIP
while [ "$(host -4 -t a ${PREFIX}_${REPLICA_ID} | cut -d " " -f 4)" != "$MYIP" ]
do
    ((REPLICA_ID++))
done

HOST_IP=${PREFIX}_${REPLICA_ID}
echo "I am ${PREFIX}_${REPLICA_ID}."
export HOST_IP
export REPLICA_ID

echo "ZOOKEEPER    = $ZOOKEEPER"
echo "REPLICA_ID   = $REPLICA_ID"
echo "N_PARTITIONS = $NUMBER_OF_PARTITIONS"
echo "HOST_IP      = $HOST_IP"

[ $1 = "seedNode"         ] && cluster seed raphtory_seedNode_1:9101 $ZOOKEEPER
sleep 5
[ $1 = "partitionManager" ] && cluster partitionManager $((REPLICA_ID-1)) $NUMBER_OF_PARTITIONS $ZOOKEEPER
sleep 5
[ $1 = "router"           ] && cluster router    $NUMBER_OF_PARTITIONS $ZOOKEEPER
sleep 5
[ $1 = "updater"          ] && cluster updateGen $NUMBER_OF_PARTITIONS $ZOOKEEPER
[ $1 = "clusterUp"        ] && cluster ClusterUp $NUMBER_OF_PARTITIONS $ZOOKEEPER

