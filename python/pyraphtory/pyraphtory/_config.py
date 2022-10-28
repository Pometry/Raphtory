import os
import pathlib
from pathlib import Path
from pyraphtory_bootstrap import ivy_file
from bs4 import BeautifulSoup


def get_ivy_jars():
    with open(os.path.dirname(os.path.realpath(__file__)) + '/ivy.xml', 'r') as f:
        raw_xml = f.read()
    data = BeautifulSoup(raw_xml, "xml")
    ivy_base_dir = os.path.expanduser("~/.ivy2/cache")
    jars_to_get = []
    for item in data.find_all('dependency'):
        org, name, rev = item.get('org'), item.get('name'), item.get('rev')
        jars_found = ivy_base_dir + '/' + org + '/' + name
        for file in pathlib.Path(jars_found).rglob("*-" + rev + ".jar"):
            jars_to_get.append(str(file))
    jars_to_get = ':'.join(set(jars_to_get))
    return jars_to_get


def setup_raphtory_jars():
    env_jar_location = os.environ.get("PYRAPTHORY_JAR_LOCATION", "")
    custom_jar_path = os.environ.get("PYRAPHTORYPATH", "")
    env_jar_glob_lookup = os.environ.get("PYRAPTHORY_JAR_GLOB_LOOKUP", '*.jar')
    java_args_env = os.environ.get("PYRAPTHORY_JVM_ARGS", "")
    if env_jar_location != "":
        jar_location = Path(env_jar_location)
    else:
        jar_location = Path(__file__).parent.parent / 'lib'
    jars_found = ":".join([str(jar) for jar in jar_location.glob(env_jar_glob_lookup)])
    if custom_jar_path:
        jars_found += ":" + custom_jar_path
    jars_found =  get_ivy_jars() + ':' + jars_found
    return jars_found, java_args_env


jars, java_args = setup_raphtory_jars()