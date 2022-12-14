import inspect

import requests
from requests.adapters import HTTPAdapter, Retry
import platform
import shutil
import os.path
import hashlib
import tempfile
import subprocess
import os
import site
import logging
from pathlib import Path

#logging.basicConfig(level=logging.INFO)

IVY_LIB = 'lib'
PYRAPHTORY_DATA = '/pyraphtory_jvm/data'

build_folder = Path(__file__).resolve().parent

data_folder = build_folder / "data"

ivy_folder = data_folder / 'ivys'
build_file = build_folder / 'build.xml'

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


def getOS() -> str:
    platform_system = platform.system()
    if platform_system in (OS_MAC, OS_LINUX):
        return platform_system
    else:
        raise Exception("Unsupported OS. Cannot install.")


def getArch() -> str:
    if platform.machine() == "x86_64":
        return OS_X64
    elif platform.machine() == 'arm64':
        return OS_AARCH64
    else:
        raise Exception("Unsupported Architecture. Cannot install.")


def delete_source(filename: Path | str) -> None:
    logging.info(f"Deleting source file {filename}...")
    filename = Path(filename)
    filename.unlink(missing_ok=True)


def checksum(filepath: Path | str, expected_sha_hash: str) -> bool:
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


def download_java(system_os: str, architecture: str, download_dir: str | Path):
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


def safe_download_file(download_dir: Path | str, expected_sha, url: str) -> Path:
    download_dir = Path(download_dir)
    logging.info(f"Downloading {url} to {download_dir}")
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    filename = url.split('/')[-1]
    file_location = download_dir / filename
    # stream file to reduce mem footprint
    try:
        r = req_session.get(url, stream=True)
        # raise an exception if http issue occurs
        r.raise_for_status()
        download_dir.mkdir(parents=True, exist_ok=True)
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


def get_and_run_ivy(JAVA_BIN: str, download_dir: Path | str = build_folder) -> None:
    download_dir = Path(download_dir)
    ivy_file_location = safe_download_file(download_dir, IVY_BIN[CHECKSUM_SHA256], IVY_BIN[LINK])
    shutil.unpack_archive(ivy_file_location, extract_dir=download_dir)
    working_dir = Path.cwd()
    logging.info(
        f"IVY working dir {working_dir}, dl dir: {download_dir} REAL PATH {build_folder}")
    logging.info(os.listdir('.'))
    for fname in ivy_folder.glob("*.xml"):
        subprocess.call(
            [JAVA_BIN, "-jar", str(ivy_file_location / "ivy-2.5.0.jar"), "-ivy",
             str(fname), "-retrieve", str(download_dir)])
    # Clean up and delete downloaded ivy files
    shutil.rmtree(download_dir / "apache-ivy-2.5.0", ignore_errors=True)
    delete_source(download_dir / "apache-ivy-2.5.0-bin.zip")
    # Keep only compile directory
    for d in (download_dir / IVY_LIB).glob("!compile"):
        shutil.rmtree(d, ignore_errors=True)


def empty_folder(folder: str | Path):
    folder = Path(folder)
    for filename in folder.iterdir():
        try:
            if filename.is_file() or filename.is_symlink():
                filename.unlink()
            elif filename.is_dir():
                shutil.rmtree(filename, ignore_errors=True)
        except Exception:
            # silent continue
            continue


def unpack_jre(filename: str | Path, jre_loc: str | Path):
    filename = Path(filename)
    jre_loc = Path(jre_loc)
    with tempfile.TemporaryDirectory() as unpack_dir:
        unpack_dir = Path(unpack_dir)
        system_os = getOS()
        logging.info(f'Unpacking JRE for {system_os}...')
        shutil.unpack_archive(filename, unpack_dir)
        try:
            result_dir = next(unpack_dir.iterdir())
        except StopIteration:
            raise Exception('Error: JRE unpacking failed.')
        # Empty jre folder
        empty_folder(jre_loc)
        if system_os == OS_MAC:
            move_dir = 'Contents/Home'
        else:  # if system_os == OS_LINUX:
            move_dir = '.'
        for file_name in (result_dir / move_dir).iterdir():
            shutil.move(file_name, jre_loc)
        logging.info('Cleaning up...')
    filename.unlink(missing_ok=True)


def check_system_dl_java(download_dir: Path | str=build_folder):
    download_dir = Path(download_dir)
    logging.info("Downloading java...")
    system_os = getOS()
    logging.info(f"- Operating system: {system_os} ")
    architecture = getArch()
    logging.info(f"- Architecture: {architecture}")
    logging.info(f"Downloading to {download_dir}")
    jre_loc = download_dir / 'jre'
    jre_loc.mkdir(exist_ok=True, parents=True)
    download_loc = download_java(system_os, architecture, download_dir)
    unpack_jre(download_loc, jre_loc)
    return jre_loc / "bin" / 'java'


def has_java():
    try:
        res = subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
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


def get_local_ivy_loc():
    return site.getsitepackages()[0] + PYRAPHTORY_DATA + '/lib/'


def check_dl_java_ivy(download_dir: str | Path = build_folder):
    download_dir = Path(download_dir)
    if has_java():
        java_bin = get_java_home()
    else:
        java_bin = check_system_dl_java(download_dir)
    get_and_run_ivy(java_bin, download_dir)
