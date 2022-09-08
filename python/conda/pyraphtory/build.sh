echo "Building Raphtory from source via SBT..."
set -e && (cd $SRC_DIR && sbt "core/assembly")

echo "Getting Raphtory version"
set -e && (cd $SRC_DIR && sbt -Dsbt.supershell=false -error "print core/version" > $SRC_DIR/raphtory_version)

echo "Creating a directory for the jar..."
set -e && mkdir -p $SRC_DIR/python/pyraphtory/lib/

echo "Moving raphtory core jar to the pyraphtory lib folder..."
set -e && mv $SRC_DIR/core/target/scala-2.13/core-assembly-$(cat $SRC_DIR/raphtory_version).jar $SRC_DIR/python/pyraphtory/lib/

echo "Building pyraphtory via poetry..."
set -e && (cd $SRC_DIR/python/pyraphtory/ && poetry build)

echo "Installing via poetry..."
set -e && (cd $SRC_DIR/python/pyraphtory/ && poetry install)

echo "Installing package via pip..."
set -e && (pip install $SRC_DIR/python/pyraphtory/dist/pyraphtory-$(cat $SRC_DIR/raphtory_version).tar.gz)

echo "Clean-up"
rm -r $SRC_DIR/raphtory_version
