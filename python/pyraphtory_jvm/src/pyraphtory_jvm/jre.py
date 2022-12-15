import inspect
import shutil
from pathlib import Path
import logging
import subprocess
import os
import site



#logging.basicConfig(level=logging.INFO)

IVY_LIB = 'lib'
PYRAPHTORY_DATA = '/pyraphtory_jvm/data'

package_folder = Path(__file__).resolve().parent

data_folder = package_folder / "data"
lib_folder = package_folder / "lib"


def has_java():
    try:
        res = subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        if res.returncode == 0:
            logging.info("Java found!")
            return True
    except FileNotFoundError:
        return False


def get_java_home() -> Path:
    logging.info("Getting JAVA_HOME")
    home = os.getenv('JAVA_HOME')
    if home is not None:
        logging.info(f"JAVA_HOME found = {home}/bin/java")
        return Path(home) / 'bin' / 'java'
    elif shutil.which('java') is not None:
        logging.info(f'JAVA_HOME not found. But java found. Detecting home...')
        # Check if JAVA_HOME is a symlink
        java_home = str(Path(shutil.which('java')).parents[1])
        if os.path.islink(java_home):
            # If it is, resolve the symlink to the real path
            java_home = os.path.realpath(java_home)
        os.environ["JAVA_HOME"] = java_home
        return Path(shutil.which('java'))
    else:
        raise FileNotFoundError("JAVA_HOME has not been set, java was also not found")


def get_local_java_loc():
    if has_java():
        return get_java_home()
    else:
        java_loc = site.getsitepackages()[0] + PYRAPHTORY_DATA + '/jre/bin/java'
        java_home = site.getsitepackages()[0] + PYRAPHTORY_DATA + '/jre/'
        if os.path.islink(java_home):
            # If it is, resolve the symlink to the real path
            java_home = os.path.realpath(java_home)
        os.environ["JAVA_HOME"] = java_home
    if os.path.isfile(java_loc):
        return java_loc
    raise Exception("JAVA not home.")
