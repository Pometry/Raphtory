import os
from pathlib import Path
from bs4 import BeautifulSoup
from pyraphtory_jvm import ivy_file


def get_ivy_jars_from_cache():
    with open(ivy_file, 'r') as f:
        raw_xml = f.read()
    data = BeautifulSoup(raw_xml, "xml")
    ivy_base_dir = os.path.expanduser("~/.ivy2/cache")
    jars_to_get = []
    for item in data.find_all('dependency'):
        org, name, rev = item.get('org'), item.get('name'), item.get('rev')
        jars_found = ivy_base_dir + '/' + org + '/' + name
        for file in Path(jars_found).rglob("*-" + rev + ".jar"):
            jars_to_get.append(str(file))
    jars_to_get = ':'.join(set(jars_to_get))
    return jars_to_get


def get_ivy_jars_from_local_lib():
    ivy_lib_dir = os.path.dirname(os.path.realpath(ivy_file))+'/lib/compile'
    jars_to_get = []
    for file in Path(ivy_lib_dir).rglob("*.jar"):
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
    jars_found = get_ivy_jars_from_local_lib() + ':' + jars_found
    print(jars_found)
    return jars_found, java_args_env


jars, java_args = setup_raphtory_jars()
