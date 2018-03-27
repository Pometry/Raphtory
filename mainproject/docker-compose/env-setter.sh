#!/bin/bash
# Run me as env-setter.sh partitionManager 'partitionManager $REPLICA_ID $NUMBER_OF_PARTITIONS $ZOOKEEPER' or
# Run me as env-setter.sh router 'router $NUMBER_OF_PARTITIONS $ZOOKEEPER'

echo "Looking for replica id"

MYIP=$(ip addr show eth0 | grep inet[^6] | sed 's/.*inet \(.*\)\/[0-9]* \(.* \)*scope.*/\1/')

REPLICA_ID=1
PREFIX=raphtory_$1
# Loop until DNS resolution of common_name_$REPLICA_ID return $MYIP
while [ "$(host -4 -t a ${PREFIX}_${REPLICA_ID} | cut -d " " -f 4)" != "$MYIP" ]
do
    ((REPLICA_ID++))
done

HOST_IP=${PREFIX}_${REPLICA_ID}
echo "I am ${PREFIX}_${REPLICA_ID}."

echo "${REPLICA_ID}" > /var/run/replica_id
echo "${HOSTNAME}"   > /var/run/replica_hostname

export HOST_IP
export REPLICA_ID

eval $2

