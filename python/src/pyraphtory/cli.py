from pyraphtory._config import jars, java, java_args
from pyraphtory import __version__
import subprocess
import sys


def standalone():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Standalone"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Standalone"])


def classpath():
    sys.stdout.write(jars)


def version():
    sys.stdout.write(__version__)


if __name__ == "__main__":
    standalone()
