echo "Building Raphtory from source via SBT..."
set -e && (cd $SRC_DIR && sbt "core/assembly")
echo "Creating a directory for the jar..."
set -e && mkdir -p $SRC_DIR/python/pyraphtory/lib/
echo "Moving the raphtory jar to the pyraphtory lib folder..."
set -e && mv $SRC_DIR/core/target/scala-2.13/core-assembly-0.1.0.jar $SRC_DIR/python/pyraphtory/lib/
echo "Building pyraphtory..."
set -e && (cd $SRC_DIR/python/pyraphtory/ && $PYTHON -m build --no-isolation)
echo "Installing pyraphtory..."
set e && (cd $SRC_DIR/python/pyraphtory/ && $PYTHON -m pip install dist/pyraphtory-0.0.1.tar.gz)