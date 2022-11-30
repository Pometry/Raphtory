#!/bin/bash
set -x

DEP_CP=$(python3 -c "from pyraphtory_jvm.jre import get_local_ivy_loc; print(get_local_ivy_loc())")
CORE_CP=$(python3 -c "import site; print(site.getsitepackages()[0] + '/lib/')")

java -cp "$DEP_CP/compile/*:${CORE_CP}/*" $RAPHTORY_JAVA_RUN_ARGS $RAPHTORY_JAVA_RUN_CLASS $RAPHTORY_JAVA_COMPONENT_NAME
