#!/bin/bash
# Run me as env-setter.sh (partitionManager|router|seedNode|clusterUp|updater)
echo "Starting up..."

export HOST_IP=$(ip addr show eth0 | grep inet[^6] | sed 's/.*inet \(.*\)\/[0-9]* \(.* \)*scope.*/\1/')
export HOSTNAME=$(hostname)

echo "/////  ENV SET //////"
echo "HOST_IP      = $HOST_IP"
echo "HOSTNAME     = $HOSTNAME"
echo "/////////////////////"

#cd /node_exporter-1.0.1.linux-386
#./node_exporter >> /dev/null &

cd /opt/docker/bin
RaphtoryServer $1
