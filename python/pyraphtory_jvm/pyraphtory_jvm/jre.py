import inspect

import requests
from requests.adapters import HTTPAdapter, Retry
import platform
import shutil
import os.path
import pathlib
import hashlib
import tempfile
import subprocess
import os
import site
import logging
from pathlib import Path

#logging.basicConfig(level=logging.INFO)

IVY_LIB = '/lib'
PYRAPHTORY_DATA = '/pyraphtory_jvm/data'

ivy_folder = os.path.dirname(os.path.realpath(__file__)) + '/data/ivys/'
build_file = os.path.dirname(os.path.realpath(__file__)) + '/build.xml'

LINK = 'link'
CHECKSUM_SHA256 = 'checksum'

OS_MAC = 'Darwin'
OS_LINUX = 'Linux'
OS_X64 = 'x64'
OS_AARCH64 = 'aarch64'

SOURCES = {
    OS_MAC: {
        OS_X64:
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_x64_mac_hotspot_11.0.16.1_1.tar.gz',
                CHECKSUM_SHA256: '10be61a8dd3766f7c12e2e823a6eca48cc6361d97e1b76310c752bd39770c7fe'
            },
        OS_AARCH64:
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_aarch64_mac_hotspot_11.0.16.1_1.tar.gz',
                CHECKSUM_SHA256: 'c84f38a7d87d50649ffc1f625facb4398fa54885371336a2cbf6ae2b435cbd10'
            }
    },
    OS_LINUX: {
        OS_X64:
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_linux_hotspot_11.0.17_8.tar.gz',
                CHECKSUM_SHA256: '752616097e09d7f60a3ad8bd312f90eaf50ac72577e55df229fe6e8091148f79'
            },
        OS_AARCH64:
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_aarch64_linux_hotspot_11.0.17_8.tar.gz',
                CHECKSUM_SHA256: 'bd6efe3290c8b5a42f695a55a26f3e3c9c284288574879d4b7089f31f5114177'
            }
    },
    # 'Windows': {
    #     OS_X64:
    #         {
    #             LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_windows_hotspot_11.0.17_8.zip',
    #             CHECKSUM_SHA256: '814a731f92dd67ad6cfb11a8b06dfad5f629f67be88ae5ae37d34e6eea6be6f4'
    #         }
    # }
}

IVY_BIN = {
    LINK: 'https://github.com/Raphtory/Data/raw/main/apache-ivy-2.5.0-bin.zip',
    CHECKSUM_SHA256: '7c6f467e33c28d82f4f8c3c10575bb461498ad8dcabf57770f481bfea59b1e59'
}


def getOS():
    platform_system = platform.system()
    if platform_system in (OS_MAC, OS_LINUX):
        return platform_system
    else:
        raise Exception("Unsupported OS. Cannot install.")


def getArch():
    if platform.machine() == "x86_64":
        return OS_X64
    elif platform.machine() == 'arm64':
        return OS_AARCH64
    else:
        raise Exception("Unsupported Architecture. Cannot install.")


def delete_source(filename):
    logging.info(f"Deleting source file {filename}...")
    if os.path.exists(filename):
        try:
            os.remove(filename)
        except OSError:
            pass


def checksum(filepath, expected_sha_hash):
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    if sha256_hash.hexdigest() == expected_sha_hash:
        logging.info(f"{filepath} hash correct")
        return True
    else:
        logging.info(f"{filepath} hash invalid, got {sha256_hash.hexdigest()} expected {expected_sha_hash}")
        return False


def download_java(system_os, architecture, download_dir):
    # get the version and sha which is stored locally
    if system_os not in SOURCES:
        msg = f'Error: System OS {system_os} not supported.\nNot downloading.'
        logging.info(msg)
        raise SystemExit(msg)
    url = SOURCES[system_os][architecture][LINK]
    expected_sha = SOURCES[system_os][architecture][CHECKSUM_SHA256]
    # open a session and download
    logging.info(f"Downloading JAVA JRE {system_os}-{architecture} from {url}. Please wait...")
    file_location = safe_download_file(download_dir, expected_sha, url)
    return file_location


def safe_download_file(download_dir, expected_sha, url):
    logging.info(f"Downloading {url} to {download_dir}")
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    filename = url.split('/')[-1]
    file_location = str(download_dir + '/' + filename)
    # stream file to reduce mem footprint
    try:
        r = req_session.get(url, stream=True)
        # raise an exception if http issue occurs
        r.raise_for_status()
        if not os.path.exists(download_dir):
            os.mkdir(download_dir)
        with open(file_location, 'wb') as f:
            shutil.copyfileobj(r.raw, f)
        status = checksum(file_location, expected_sha)
        r.close()
        if not status:
            delete_source(file_location)
            logging.info(f"Downloaded Jar {file_location} has incorrect checksum")
            raise SystemExit()
    except requests.exceptions.RequestException as err:
        logging.info(f"Major exception: {err}")
        delete_source(file_location)
        raise SystemExit(err)
    return file_location


def get_and_run_ivy(JAVA_BIN, download_dir=os.path.dirname(os.path.realpath(__file__))):
    file_location = safe_download_file(str(download_dir), IVY_BIN[CHECKSUM_SHA256], IVY_BIN[LINK])
    shutil.unpack_archive(file_location, extract_dir=download_dir)
    working_dir = os.getcwd()
    logging.info(
        f"IVY working dir {working_dir}, dl dir: {download_dir} REAL PATH {str(os.path.dirname(os.path.realpath(__file__)))}")
    os.chdir(download_dir)
    logging.info(os.listdir('.'))
    files = os.listdir(ivy_folder)
    for fname in files:
        if fname.endswith('.xml'):
            subprocess.call(
                [JAVA_BIN, "-jar", download_dir + "/apache-ivy-2.5.0/ivy-2.5.0.jar", "-ivy",
                 str(ivy_folder) + "/" + fname, "-retrieve", "."])
    os.chdir(working_dir)
    # Clean up and delete downloaded ivy files
    shutil.rmtree(download_dir + "/apache-ivy-2.5.0", ignore_errors=True)
    delete_source(download_dir + "/apache-ivy-2.5.0-bin.zip")
    # Keep only compile directory
    rm_dirs = os.listdir(download_dir + IVY_LIB)
    try:
        rm_dirs.remove('compile')
    except ValueError:
        pass
    for d in rm_dirs:
        shutil.rmtree(download_dir + IVY_LIB + '/' + d, ignore_errors=True)


def empty_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path, ignore_errors=True)
        except Exception:
            # silent continue
            continue


def unpack_jre(filename, jre_loc):
    unpack_dir = tempfile.mkdtemp()
    system_os = getOS()
    logging.info(f'Unpacking JRE for {system_os}...')
    shutil.unpack_archive(filename, unpack_dir)
    if os.listdir(unpack_dir):
        result_dir = unpack_dir + '/' + os.listdir(unpack_dir)[0]
        # Empty jre folder
        empty_folder(jre_loc)
        if system_os == OS_MAC:
            move_dir = '/Contents/Home'
        else:  # if system_os == OS_LINUX:
            move_dir = ''
        file_names = os.listdir(result_dir + move_dir)
        for file_name in file_names:
            shutil.move(os.path.join(result_dir + move_dir, file_name), jre_loc)
        logging.info('Cleaning up...')
        shutil.rmtree(unpack_dir, ignore_errors=True)
        try:
            os.remove(filename)
        except OSError:
            pass
    else:
        raise Exception('Error: JRE unpacking failed.')


def check_system_dl_java(download_dir=str(os.path.dirname(os.path.realpath(__file__)))):
    logging.info("Downloading java...")
    system_os = getOS()
    logging.info(f"- Operating system: {system_os} ")
    architecture = getArch()
    logging.info(f"- Architecture: {architecture}")
    logging.info(f"Downloading to {download_dir}")
    jre_loc = download_dir + '/jre'
    if not os.path.exists(jre_loc):
        os.makedirs(jre_loc)
    download_loc = download_java(system_os, architecture, download_dir)
    unpack_jre(download_loc, jre_loc)
    return jre_loc + '/bin/java'


def has_java():
    try:
        res = subprocess.run(["java", "-version"],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        if res.returncode == 0:
            logging.info("Java found!")
            return True
    except FileNotFoundError:
        return False


def get_java_home():
    logging.info("Getting JAVA_HOME")
    home = os.getenv('JAVA_HOME')
    if home is not None:
        logging.info(f"JAVA_HOME found = {home}/bin/java")
        return home + '/bin/java'
    elif shutil.which('java') is not None:
        logging.info(f'JAVA_HOME not found. But java found. Detecting home...')
        # Check if JAVA_HOME is a symlink
        java_home = str(Path(shutil.which('java')).parents[1])
        if os.path.islink(java_home):
            # If it is, resolve the symlink to the real path
            java_home = os.path.realpath(java_home)
        os.environ["JAVA_HOME"] = java_home
        return shutil.which('java')
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


def get_local_ivy_loc():
    return site.getsitepackages()[0] + PYRAPHTORY_DATA + '/lib/'


def check_dl_java_ivy(download_dir=site.getsitepackages()[0] + PYRAPHTORY_DATA):
    if has_java():
        java_bin = get_java_home()
    else:
        java_bin = check_system_dl_java(download_dir)
    get_and_run_ivy(java_bin, download_dir)
