from pyraphtory._config import jars, java, java_args
import subprocess


def standalone():
    if java_args:
        subprocess.run([java, java_args, "-cp", jars, "com.raphtory.service.Standalone"])
    else:
        subprocess.run([java, "-cp", jars, "com.raphtory.service.Standalone"])


if __name__ == "__main__":
    standalone()
