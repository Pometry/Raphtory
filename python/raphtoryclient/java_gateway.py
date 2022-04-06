import os
import platform
import py4j
from py4j import protocol as proto
from py4j.protocol import (get_return_value, escape_new_line)
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
import time

from raphtoryclient.Serializer import read_int, UTF8Deserializer


def launch_gateway():
    """
    launch jvm gateway

    Returns
    -------
    JavaGateway
    """
    # Launch a Py4J Gateway via a script
    # TODO CHECK OPERATING SYSTEM IS WINDOWS
    on_windows = platform.system() == "Windows"
    script = "./bin/run-py4j.cmd" if on_windows else "./bin/run-py4j"
    command = [script]
    # '/opt/anaconda3/envs/pyraphtory/lib/python3.8/site-packages/pyspark/./bin/spark-submit'
    # Create a temporary directory where the gateway server should write the connection
    # information.
    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)
        env = dict(os.environ)
        env["_RAPHTORY_DRIVER_CONN_INFO_PATH"] = conn_info_file
        # Launch the Java gateway.
        popen_kwargs = {'stdin': PIPE, 'env': env}
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        # We always set the necessary environment variables.
        if not on_windows:
            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            popen_kwargs['preexec_fn'] = preexec_func
            proc = Popen(command, **popen_kwargs)
        else:
            # preexec_fn not supported on Windows
            # proc = Popen(command, **popen_kwargs)
            # TODO WINDOWS IMPLEMENTATION
            pass

        # Wait for the file to appear, or for the process to exit, whichever happens first.
        while not proc.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            raise Exception("Java gateway process exited before sending its port number")

        with open(conn_info_file, "rb") as info:
            gateway_port = read_int(info)
            gateway_secret = UTF8Deserializer().loads(info)
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway (or client server to pin the thread between JVM and Python)
    gateway = py4j.java_gateway.JavaGateway(
        gateway_parameters=py4j.java_gateway.GatewayParameters(
            port=gateway_port,
            auth_token=gateway_secret,
            auto_convert=True))

    # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
    gateway.proc = proc

    # Import the classes used by Raphtory
    java_import(gateway.jvm, "com.raphtory.deployment.Raphtory")
    java_import(gateway.jvm, "com.raphtory.algorithms")
    java_import(gateway.jvm, "com.raphtory.client.GraphDeployment")
    java_import(gateway.jvm, "com.raphtory.output.FileOutputFormat")
    java_import(gateway.jvm, "com.raphtory.algorithms.api")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway

def java_import(jvm_view, import_str):
    """Imports the package or class specified by `import_str` in the
    jvm view namespace.

    :param jvm_view: The jvm_view in which to import a class/package.
    :import_str: The class (e.g., java.util.List) or the package
                 (e.g., java.io.*) to import
    """
    gateway_client = jvm_view._gateway_client
    command = proto.JVMVIEW_COMMAND_NAME + proto.JVM_IMPORT_SUB_COMMAND_NAME +\
        jvm_view._id + "\n" + escape_new_line(import_str) + "\n" +\
        proto.END_COMMAND_PART
    answer = gateway_client.send_command(command)
    return_value = get_return_value(answer, gateway_client, None, None)
    return return_value

