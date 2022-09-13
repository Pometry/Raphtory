echo "Running make python-build..."
set -e && (cd $SRC_DIR && make python-build)

echo "Running make clean"
set -e && (cd $SRC_DIR && make clean)
