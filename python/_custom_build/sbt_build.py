import subprocess


def make_python_build():
    subprocess.check_call(["make", "sbt-build"])
