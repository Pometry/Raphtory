import requests
from requests.adapters import HTTPAdapter, Retry
import shutil
import os.path
import hashlib
import zipfile
from pyraphtory import __version__

_URL = 'url'
_SHA256 = 'sha256'

RAPHTORY_JARS_HASH = {
    '0.2.0a1': {
        _URL: 'https://github.com/Raphtory/Raphtory/releases/download/v0.2.0a1/core-assembly-0.2.0a1.jar',
        _SHA256: '0005912de03d6d05db290f371f8bfcdec31ce376c6125e7aec9905bc16591b6e'
    }
}


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


def delete_jar(filename):
    print(f"Deleting jar file {filename}...")
    if os.path.exists(filename):
        os.remove(filename)


# Jar to the dark side
def is_raphtory_jar_found(jar_path):
    is_raph_spec_vendor = False
    is_spec_title_core = False
    with zipfile.ZipFile(jar_path, 'r') as jar_file_handler:
        files_found = jar_file_handler.infolist()
        for file_handler in files_found:
            is_raph_spec_vendor = False
            is_spec_title_core = False
            if file_handler.filename == 'META-INF/MANIFEST.MF':
                manifest_txt = str(jar_file_handler.read(file_handler.filename), encoding='utf8')
                lines = manifest_txt.split('\n')
                SPEC_VENDOR = 'Specification-Vendor: '
                SPEC_TITLE = 'Specification-Title: '
                for line in lines:
                    if SPEC_VENDOR in line:
                        vendor = line.split(SPEC_VENDOR)[1].replace('\r', '')
                        if vendor == 'raphtory':
                            is_raph_spec_vendor = True
                    elif SPEC_TITLE in line:
                        title = line.split(SPEC_TITLE)[1].replace('\r', '')
                        if title == 'core':
                            is_spec_title_core = True
                break
    return is_raph_spec_vendor & is_spec_title_core


def does_jar_version_match(jar_path):
    jar_version = None
    with zipfile.ZipFile(jar_path, 'r') as jar_file_handler:
        files_found = jar_file_handler.infolist()
        for file_handler in files_found:
            if file_handler.filename == 'META-INF/MANIFEST.MF':
                manifest_txt = str(jar_file_handler.read(file_handler.filename), encoding='utf8')
                lines = manifest_txt.split('\n')
                VERSION = 'Implementation-Version: '
                for line in lines:
                    if VERSION in line:
                        jar_version = line.split(VERSION)[1].replace('\r', '')
                        break
                break
    return __version__ == jar_version


def check_download_update_jar(pyraphtory_jar_download_loc, jars):
    if not jars:
        jars = download_raphtory_jar(__version__, pyraphtory_jar_download_loc)
    else:
        # otherwise, check if we have a raphtory jar
        has_raphtory_jar = False
        for jar in jars.split(':'):
            has_raphtory_jar = is_raphtory_jar_found(jar)
        # if we do not have a raphtory jar then we download it
        new_jar_location = ''
        if not has_raphtory_jar:
            new_jar_location = download_raphtory_jar(__version__)
        else:
            # if we have a raphtory jar, we check version
            # if version doesnt match python, we download the correct
            if not does_jar_version_match(jar):
                delete_jar(jar)
                new_jar_location = download_raphtory_jar(__version__, pyraphtory_jar_download_loc)
        # we add the new jar to the jars path
        if new_jar_location:
            jars += ':' + new_jar_location
    return jars


def download_raphtory_jar(version, download_dir):
    # get the version and sha which is stored locally
    if version not in RAPHTORY_JARS_HASH:
        msg = f'Error: Version {version} not found in Python dictionary.\nNot downloading.'
        print(msg)
        raise SystemExit(msg)
    url = RAPHTORY_JARS_HASH[version][_URL]
    expected_sha = RAPHTORY_JARS_HASH[version][_SHA256]
    # open a session and download
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    print(f"Downloading Raphtory {version} jar from {url}. Please wait...")
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
            delete_jar(file_location)
            raise SystemExit(f"Downloaded Jar {file_location} has incorrect checksum")
    except requests.exceptions.TooManyRedirects as err:
        print(f"Bad URL, Too Many Redirects: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    except requests.exceptions.RequestException as err:
        print(f"Major exception: {err}")
        delete_jar(file_location)
        raise SystemExit(err)
    return file_location


def test():
    jar_file = '/Users/haaroony/Documents/dev/Raphtory-pyraphtory3/python/pyraphtory/lib/core-assembly-0.2.0a0.jar'
    print(is_raphtory_jar_found(jar_file))
    print(does_jar_version_match(jar_file))
    print(check_download_update_jar(jar_file))
