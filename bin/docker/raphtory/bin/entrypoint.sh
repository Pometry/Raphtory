#!/bin/bash
set -euf -o pipefail
RAPHTORY_JAVA_RUN_ARGS="${RAPHTORY_JAVA_RUN_ARGS:-}"
CORE_CP="/opt/venv/lib/python3.10/site-packages/pyraphtory/lib/*"
java -cp "$CORE_CP:/raphtory/jars/*" $RAPHTORY_JAVA_RUN_ARGS $RAPHTORY_JAVA_RUN_CLASS
