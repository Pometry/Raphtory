# Build raphtory from source
export RECIPE_DIR="/Users/haaroony/Documents/Raphtory-pyraphtory3" && set -e && (cd $RECIPE_DIR && sbt "core/assembly")
# Make a directory for the lib file
set -e && mkdir -p $RECIPE_DIR/python/pyraphtory/lib/
# Copy the raphtory jar to the pyraphtory lib folder
set -e && cp $RECIPE_DIR/core/target/scala-2.13/core-assembly-0.1.0.jar $RECIPE_DIR/python/pyraphtory/lib/
# Install PyRaphtory
export RECIPE_DIR="/Users/haaroony/Documents/Raphtory-pyraphtory3" && export PYTHON="/Users/haaroony/opt/anaconda3/envs/pyraphtorytest/bin/python" && set -e && (cd $RECIPE_DIR/python/pyraphtory/ && $PYTHON setup.py install)     # Python command to install the script.
