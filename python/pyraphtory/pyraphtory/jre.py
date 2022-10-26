import requests
import requests
from requests.adapters import HTTPAdapter, Retry
import platform
import os
import shutil
import os.path
import pathlib
import hashlib
import tempfile
import subprocess
import os

LINK = 'link'
CHECKSUM_SHA256 = 'checksum'

OS_MAC = 'Darwin'
OS_LINUX = 'Linux'

SOURCES = {
    OS_MAC: {
        'x64':
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_x64_mac_hotspot_11.0.16.1_1.tar.gz',
                CHECKSUM_SHA256: '10be61a8dd3766f7c12e2e823a6eca48cc6361d97e1b76310c752bd39770c7fe'
            },
        'aarch64':
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jre_aarch64_mac_hotspot_11.0.16.1_1.tar.gz',
                CHECKSUM_SHA256: 'c84f38a7d87d50649ffc1f625facb4398fa54885371336a2cbf6ae2b435cbd10'
            }
    },
    OS_LINUX: {
        'x64':
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_linux_hotspot_11.0.17_8.tar.gz',
                CHECKSUM_SHA256: '752616097e09d7f60a3ad8bd312f90eaf50ac72577e55df229fe6e8091148f79'
            },
        'aarch64':
            {
                LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_aarch64_linux_hotspot_11.0.17_8.tar.gz',
                CHECKSUM_SHA256: 'bd6efe3290c8b5a42f695a55a26f3e3c9c284288574879d4b7089f31f5114177'
            }
    },
    # 'Windows': {
    #     'x64':
    #         {
    #             LINK: 'https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.17%2B8/OpenJDK11U-jre_x64_windows_hotspot_11.0.17_8.zip',
    #             CHECKSUM_SHA256: '814a731f92dd67ad6cfb11a8b06dfad5f629f67be88ae5ae37d34e6eea6be6f4'
    #         }
    # }
}

IVY_BIN = {
    LINK: 'https://dlcdn.apache.org//ant/ivy/2.5.0/apache-ivy-2.5.0-bin.zip',
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
        return 'x64'
    elif platform.machine() == 'arm64':
        return 'aarch64'
    else:
        raise Exception("Unsupported Architecture. Cannot install.")


def delete_source(filename):
    print(f"Deleting source file {filename}...")
    if os.path.exists(filename):
        os.remove(filename)


def checksum(filepath, expected_sha_hash):
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    if sha256_hash.hexdigest() == expected_sha_hash:
        print(f"{filepath} hash correct")
        return True
    else:
        print(f"{filepath} hash invalid, got {sha256_hash.hexdigest()} expected {expected_sha_hash}")
        return False


def download_java(system_os, architecture, download_dir):
    # get the version and sha which is stored locally
    if system_os not in SOURCES:
        msg = f'Error: System OS {system_os} not supported.\nNot downloading.'
        print(msg)
        raise SystemExit(msg)
    url = SOURCES[system_os][architecture][LINK]
    expected_sha = SOURCES[system_os][architecture][CHECKSUM_SHA256]
    # open a session and download
    print(f"Downloading JAVA JRE {system_os}-{architecture} from {url}. Please wait...")
    file_location = safe_download_file(download_dir, expected_sha, url)
    return file_location


def safe_download_file(download_dir, expected_sha, url):
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    filename = url.split('/')[-1]
    file_location = str(download_dir + '/' + filename)
    # stream file to reduce mem footprint
    try:
        with req_session.get(url, stream=True) as r:
            # raise an exception if http issue occurs
            r.raise_for_status()
            if not os.path.exists(download_dir):
                os.mkdir(download_dir)
            with open(file_location, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        status = checksum(file_location, expected_sha)
        if not status:
            delete_source(file_location)
            raise SystemExit(f"Downloaded Jar {file_location} has incorrect checksum")
    except requests.exceptions.TooManyRedirects as err:
        print(f"Bad URL, Too Many Redirects: {err}")
        delete_source(file_location)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        delete_source(file_location)
        raise SystemExit(err)
    except requests.exceptions.RequestException as err:
        print(f"Major exception: {err}")
        delete_source(file_location)
        raise SystemExit(err)
    return file_location


def get_and_run_ivy(JAVA_BIN, download_dir='./'):
    file_location = safe_download_file(download_dir, IVY_BIN[CHECKSUM_SHA256], IVY_BIN[LINK])
    shutil.unpack_archive(file_location)
    subprocess.call([JAVA_BIN, "-jar", "./apache-ivy-2.5.0/ivy-2.5.0.jar", "-ivy", "./ivy.xml"])
    # Clean up and delete downloaded ivy files
    shutil.rmtree("apache-ivy-2.5.0")
    delete_source("apache-ivy-2.5.0-bin.zip")


def unpack_jre(filename, jre_loc):
    unpack_dir = tempfile.mkdtemp(dir=pathlib.Path().resolve())
    system_os = getOS()
    print(f'Unpacking JRE for {system_os}...')
    shutil.unpack_archive(filename, unpack_dir)
    result_dir = unpack_dir + '/' + os.listdir(unpack_dir)[0]
    if system_os == OS_MAC:
        move_dir = '/Contents/Home'
    else:  # if system_os == OS_LINUX:
        move_dir = ''
    file_names = os.listdir(result_dir + move_dir)
    for file_name in file_names:
        shutil.move(os.path.join(result_dir + move_dir, file_name), jre_loc)
    print('Cleaning up...')
    shutil.rmtree(unpack_dir)
    os.remove(filename)


def check_system_dl_java():
    print("Downloading java...")
    system_os = getOS()
    print(f"- Operating system: {system_os} ")
    architecture = getArch()
    print(f"- Architecture: {architecture}")
    download_dir = str(pathlib.Path().resolve())
    jre_loc = download_dir + '/jre'
    if not os.path.exists(jre_loc):
        os.makedirs(jre_loc)
    download_loc = download_java(system_os, architecture, download_dir)
    unpack_jre(download_loc, jre_loc)
    return jre_loc + '/bin/java'


def has_java():
    print("Checking if java is installed...")
    try:
        res = subprocess.run(["java", "-version"], stdout=subprocess.PIPE)
        if res.returncode == 0:
            print("Java found!")
            return True
    except FileNotFoundError:
        print("Java not found!")
        return False


def get_java_home():
    print("Getting JAVA_HOME")
    home = os.getenv('JAVA_HOME')
    if home is not None:
        print(f"JAVA_HOME found = {home}/bin/java")
        return home + '/bin/java'
    else:
        raise Exception("JAVA_HOME not found...")


def check_dl_java_ivy():
    if has_java():
        java_bin = get_java_home()
    else:
        java_bin = check_system_dl_java()
    get_and_run_ivy(java_bin)


