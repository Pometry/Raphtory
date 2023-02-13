from pyraphtory._config import jars, java, java_args, get_local_lib, jre
import os
import sys
from pyraphtory import __version__
import subprocess
import sys


def standalone():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Standalone"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Standalone"])


def clustermanager():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.ClusterManager"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.ClusterManager"])


def ingestion():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Ingestion"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Ingestion"])


def partition():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Partition"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Partition"])


def query():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Query"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Query"])


def classpath():
    sys.stdout.write(jars)


def version():
    sys.stdout.write(__version__)


def include():
    sys.stdout.write(str(get_local_lib()))


if __name__ == "__main__":
    standalone()
