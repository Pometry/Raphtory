@echo off

echo "Building Raphtory from source via SBT..."
cd "%SRC_DIR%" && sbt "core\assembly"

echo "Getting Raphtory version"
cd "%SRC_DIR%" && sbt "-Dsbt.supershell=false" "-error" "print core\version" > "%SRC_DIR%\raphtory_version"
set /p RAPHTORY_VERSION=<"%SRC_DIR%\raphtory_version"


echo "Creating a directory for the jar..."
mkdir "-p" "%SRC_DIR%\python\pyraphtory\lib\"

echo "Moving raphtory core jar to the pyraphtory lib folder..."
mv "%SRC_DIR%\core\target\scala-2.13\core-assembly-%RAPHTORY_VERSION%.jar" "%SRC_DIR%\python\pyraphtory\lib\"

echo "Building pyraphtory via poetry..."
cd "%SRC_DIR%\python\pyraphtory\" && poetry "build"

echo "Installing via poetry..."
cd "%SRC_DIR%\python\pyraphtory\" && poetry "install"

echo "Installing package via pip..."
pip "install" "%SRC_DIR%\python\pyraphtory\dist\pyraphtory-%RAPHTORY_VERSION%.tar.gz"

echo "Clean-up"
DEL /S "%SRC_DIR%\raphtory_version"