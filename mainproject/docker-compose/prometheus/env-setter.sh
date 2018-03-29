#!/bin/bash
# - targets: ['raphtory_partitionManager_1:19200', 'raphtory_router_1:19300', 'raphtory_updater_1:19400']

TARGETS="        - targets: ["
PREFIX=10.0.0
PORT=11600

for i in `seq 0 254`; do
    TARGETS="${TARGETS}'${PREFIX}.${i}:${PORT}', "
done

echo ${TARGETS}

