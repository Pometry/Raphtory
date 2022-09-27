import os
from pathlib import Path
import pyraphtory.fileutils


env_jar_location = os.environ.get("PYRAPTHORY_JAR_LOCATION", "")
env_jar_glob_lookup = os.environ.get("PYRAPTHORY_JAR_GLOB_LOOKUP", '*.jar')
if env_jar_location != "":
    jar_location = Path(env_jar_location)
else:
    jar_location = Path(__file__).parent.parent / 'lib'
jars = ":".join([str(jar) for jar in jar_location.glob(env_jar_glob_lookup)])

# if jars is empty, then download matching version from github
jar_download_loc = str(Path(__file__).parent.parent / 'lib')
jars = pyraphtory.fileutils.check_download_update_jar(jar_download_loc, jars)
java_args = os.environ.get("PYRAPTHORY_JVM_ARGS", "")
