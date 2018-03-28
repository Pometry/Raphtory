#!/bin/bash
# Run me as env-setter.sh (partitionManager|router|seedNode|clusterUp|updater)

NUMBER_OF_PARTITIONS=100
NUMBER_OF_ROUTERS=50

# - targets: ['raphtory_partitionManager_1:19200', 'raphtory_router_1:19300', 'raphtory_updater_1:19400']

TARGETS="        - targets: ["
PREFIX=raphtory
PORT=11600

CONF_FILE=/etc/prometheus/prometheus.yml
# TODO delete old
for i in `seq 1 $NUMBER_OF_PARTITIONS`; do
    TARGETS="${TARGETS}'${PREFIX}_partitionManager_${i}:${PORT}', "
done

for i in `seq 1 $NUMBER_OF_ROUTERS`; do
    TARGETS="${TARGETS}'${PREFIX}_router_${i}:${PORT}', "
done

TARGETS="$(echo $TARGETS | sed 's/, $//')"

echo ${TARGETS}
#| tee -a /etc/prometheus/prometheus.yml

#/bin/prometheus #-config.file=/etc/prometheus/prometheus.yml -storage.local.path=/prometheus -web.console.libraries=/etc/prometheus/console_libraries web.console.templates=/etc/prometheus/consoles

