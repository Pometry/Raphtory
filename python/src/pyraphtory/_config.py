import os
from pathlib import Path
from importlib.resources import files
import pyraphtory
import logging
import shutil
import atexit
import sys
from jpype import JClass


extra_jars = []
_jpype_running = False


class ExitHooks(object):
    """Control python exit while preserving exit code.

    see: https://stackoverflow.com/a/73277719
    """
    def __init__(self):
        self.exit_code = None
        self.exception = None

    def hook(self):
        """
        hook into system exit to trap exit code and exceptions
        """
        self._orig_exit = sys.exit
        self._orig_exc_handler = sys.excepthook
        sys.exit = self.exit
        sys.excepthook = self.exc_handler

    def exit(self, code=0):
        """
        exit hook that traps the exit code and stores it
        :param code: exit code
        """
        self.exit_code = code
        self._orig_exit(code)

    def exc_handler(self, exc_type, exc, *args):
        """
        excepthook that traps the exception and stores it
        """
        self.exception = exc
        self._orig_exc_handler(exc_type, exc, *args)

def exit_handler():
    """Forwards exit code to java and calls java exit if jvm was started"""
    try:
        if _jpype_running:
            System = JClass("java.lang.System")
            if hooks.exit_code is not None:
                System.exit(hooks.exit_code)
            elif hooks.exception is not None:
                System.exit(1)
            else:
                System.exit(0)
    except Exception as e:
        # Catch any error, otherwise we have an infinite recursion
        print(f"Java exit handler raised exception {e!r} which cannot be thrown")


hooks = ExitHooks()
hooks.hook()
atexit.register(exit_handler)


def get_java_home() -> Path:
    logging.info("Getting JAVA_HOME")
    home = os.getenv('JAVA_HOME')
    if home is not None:
        return Path(home)
    elif shutil.which('java') is not None:
        logging.info(f'JAVA_HOME not found. But java found. Detecting home...')
        # resolve JAVA_HOME in case it is a symlink
        home = Path(shutil.which('java')).resolve().parents[1]
        os.environ["JAVA_HOME"] = str(home)
        return home
    else:
        raise FileNotFoundError("JAVA_HOME has not been set, java was also not found")


def get_local_jre_loc() -> Path:
    if os.environ.get("PYRAPHTORY_USE_SYSTEM_JAVA", ""):
        return get_java_home()
    else:
        jre = files(pyraphtory) / "jre"
        if not isinstance(jre, Path):
            raise RuntimeError("Pyraphtory is not installed correctly, are you trying to import from a compressed file?")

        os.environ["JAVA_HOME"] = str(jre)
        return jre


def get_local_lib() -> Path:
    lib = files(pyraphtory) / "lib"
    if not isinstance(lib, Path):
        raise RuntimeError("Pyraphtory is not installed correctly, are you trying to import from a compressed file?")
    return lib


def get_local_jar_path():
    lib = get_local_lib()
    return str(lib) + "/*"


def join_jar_path(path: str, *new_paths: str) -> str:
    return ":".join((path, ":".join(new_paths)))


def setup_raphtory_jars():
    custom_jar_path = os.environ.get("PYRAPHTORYPATH", "")
    java_args_env = os.environ.get("PYRAPTHORY_JVM_ARGS", "")
    path = get_local_jar_path()
    if custom_jar_path:
        path = join_jar_path(path, custom_jar_path)
    return path, java_args_env


jre = get_local_jre_loc()
jars, java_args = setup_raphtory_jars()
java = str(jre / "bin" / "java")


def add_classpath(path: str):
    """
    Add custom jars or java classes.

    Note that this function only has an effect before importing the `algorithms`,
    `api`, or `interop` modules or calling the `local` or `remote` function to start a Raphtory context.

    :param path: Path to add to the jvm classpath. This supports the wildcard '*' to add all files in a folder. Use ':'
                 to separate multiple files/folders.
    """
    global jars
    jars = join_jar_path(jars, path)


def get_java_args():
    """
    Return the arguments passed to the jvm at startup
    """
    return java_args


def set_java_args(args: str):
    """
    Set the arguments passed to the jvm at startup. 

    :param args: New arguments to pass

    Note this function overrides any existing settings passed in via the environment. 
    These arguments have no effect after the jvm is started (i.e., after importing the `algorithms`,
    `api`, or `interop` modules or calling the `local` or `remote` function to start a Raphtory context).
    """
    global java_args
    java_args = args
