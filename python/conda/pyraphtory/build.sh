# Build raphtory from source
#echo "BUILDING SBT"
#set -e && (cd $SRC_DIR && sbt "core/assembly")
#echo "Make a directory for the lib file"
#set -e && mkdir -p $SRC_DIR/python/pyraphtory/lib/
#echo "Copy the raphtory jar to the pyraphtory lib folder"
#set -e && cp $SRC_DIR/core/target/scala-2.13/core-assembly-0.1.0.jar $SRC_DIR/python/pyraphtory/lib/
echo "build"
set -e && (cd $SRC_DIR/python/pyraphtory/ && $PYTHON -m build)
echo "install"
set e && (cd $SRC_DIR/python/pyraphtory/ && $PYTHON -m pip install dist/pyraphtory-0.0.1.tar.gz)   # Python command to install the script.
