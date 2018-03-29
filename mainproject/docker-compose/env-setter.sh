#!/bin/bash
# Run me as env-setter.sh (partitionManager|router|seedNode|clusterUp|updater)

echo "Looking for replica id"

HOST_IP=$(ip addr show eth0 | grep inet[^6] | sed 's/.*inet \(.*\)\/[0-9]* \(.* \)*scope.*/\1/')
REPLICA_ID=1
PREFIX=raphtory_$1
# Loop until DNS resolution of $PREFIX_$REPLICA_ID return $MYIP

while [ "$(host -4 -t a ${PREFIX}_${REPLICA_ID} | cut -d " " -f 4)" != "$HOST_IP" ]
do
    ((REPLICA_ID++))
    [ REPLICA_ID -gt $(($NUMBER_OF_PARTITIONS + $NUMBER_OF_ROUTERS)) ] && break
done

HOSTNAME=${PREFIX}_${REPLICA_ID}
echo "I am ${PREFIX}_${REPLICA_ID}."

export HOST_IP
export REPLICA_ID

echo "/////  ENV SET //////"
echo "ZOOKEEPER    = $ZOOKEEPER"
echo "REPLICA_ID   = $REPLICA_ID"
echo "N_PARTITIONS = $NUMBER_OF_PARTITIONS"
echo "HOST_IP      = $HOST_IP"
echo "HOSTNAME     = $HOSTNAME"
echo "/////////////////////"

[ $1 = "seedNode" ] || sleep 5
[ $1 = "LiveAnalysisManager" ] && echo TODO
cluster $1
