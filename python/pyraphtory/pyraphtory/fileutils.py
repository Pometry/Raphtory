import requests
from requests.adapters import HTTPAdapter, Retry
import shutil
import os.path
import hashlib

def checksum(filepath, expected_sha_hash):
    sha256_hash = hashlib.sha256()
    with open(filepath,"rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)
    if sha256_hash.hexdigest() == expected_sha_hash:
        print(f"{filepath} hash correct")
        return True
    else:
        print(f"{filepath} hash invalid, got {sha256_hash.hexdigest()} expected {expected_sha_hash}")
        return False

def delete_jar(filename):
    if os.path.exists(filename):
        shutil.rmtree(filename)

def download_file(url):
    req_session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    req_session.mount('https://', HTTPAdapter(max_retries=retries))
    print(f"Downloading file from {url}")
    filename = url.split('/')[-1]
    # stream file to reduce mem footprint
    try:
        with req_session.get(url, stream=True) as r:
            # raise an exception if http issue occurs
            r.raise_for_status()
            with open(filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
    except requests.exceptions.TooManyRedirects as err:
        print(f"Bad URL, Too Many Redirects: {err}")
        delete_jar(filename)
        raise SystemExit(err)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error: {err}")
        delete_jar(filename)
        raise SystemExit(err)
    except requests.exceptions.RequestException as err:
        print(f"Major exception: {err}")
        delete_jar(filename)
        raise SystemExit(err)
    return filename


# Todo if jar exists then do not download and dont check sum
#
# url = "https://github.com/Raphtory/Raphtory/releases/download/v0.1.0/core-assembly-0.1.0.jar"
# download_file(url)
# filepath = 'core-assembly-0.1.0.jar'
# shahash = '39a88086833387e37751d8dbe68a23dc4e1ecd62338ca686555083e749beb4f2'
# checksum(filepath, shahash)
