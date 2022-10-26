import os
from pathlib import Path


env_jar_location = os.environ.get("PYRAPTHORY_JAR_LOCATION", "")
custom_jar_path = os.environ.get("PYRAPHTORYPATH", "")
env_jar_glob_lookup = os.environ.get("PYRAPTHORY_JAR_GLOB_LOOKUP", '*.jar')
if env_jar_location != "":
    jar_location = Path(env_jar_location)
else:
    jar_location = Path(__file__).parent.parent / 'lib'
jars = ":".join([str(jar) for jar in jar_location.glob(env_jar_glob_lookup)])
if custom_jar_path:
    jars += ":" + custom_jar_path

java_args = os.environ.get("PYRAPTHORY_JVM_ARGS", "")
