#!/bin/bash
set -x

CORE_CP="$(raphtory-classpath)"

java -cp "$CORE_CP" $RAPHTORY_JAVA_RUN_ARGS $RAPHTORY_JAVA_RUN_CLASS $RAPHTORY_JAVA_COMPONENT_NAME
