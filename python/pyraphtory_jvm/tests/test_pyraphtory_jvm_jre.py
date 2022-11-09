import tempfile
from unittest import mock

import requests
import pyraphtory_jvm.jre as jre
import platform
import os.path


def test_getOS():
    this_os = platform.system()
    assert jre.getOS() == this_os


def test_getArch():
    this_arch = platform.machine()
    if this_arch == "x86_64":
        this_arch = 'x64'
    elif platform.machine() == 'arm64':
        this_arch = 'aarch64'
    assert jre.getArch() == this_arch


# Determine whether a URL exists or not
def does_urlExist(url):
    try:
        request = requests.head(url, allow_redirects=True)
        return request.status_code == requests.codes.ok
    except requests.ConnectionError:
        return False


def test_MacJavaFileExists():
    assert does_urlExist(jre.SOURCES.get(jre.OS_MAC).get(jre.OS_X64).get(jre.LINK))
    assert does_urlExist(jre.SOURCES.get(jre.OS_MAC).get(jre.OS_AARCH64).get(jre.LINK))


def test_LinuxJavaFileExists():
    assert does_urlExist(jre.SOURCES.get(jre.OS_LINUX).get(jre.OS_X64).get(jre.LINK))
    assert does_urlExist(jre.SOURCES.get(jre.OS_LINUX).get(jre.OS_AARCH64).get(jre.LINK))


def test_IVYFileExists():
    assert does_urlExist(jre.IVY_BIN.get(jre.LINK))


def test_safe_download():
    # Python make a temporary folder
    temporary_folder = tempfile.mkdtemp()
    # download a file to the temporary folder
    ivy_checksum =  jre.IVY_BIN.get(jre.CHECKSUM_SHA256)
    ivy_url =  jre.IVY_BIN.get(jre.LINK)
    file_loc = jre.safe_download_file(temporary_folder, ivy_checksum, ivy_url)
    # check that the file exists
    assert os.path.exists(file_loc)
    # delete the temporary folder and its contents
    os.unlink(file_loc)
    os.rmdir(temporary_folder)

def test_has_java_normal():
    assert jre.has_java() is True

# mock the subprocess.run function
def test_mock_suprocess2():
    with mock.patch("subprocess.run") as mock_subproc_run:
        process_mock = mock.Mock()
        attrs = {"run.return_value": ("output", 'error'), 'run.returncode': 0}
        process_mock.configure_mock(**attrs)
        mock_subproc_run.return_value = process_mock
        assert mock_subproc_run.called
        assert jre.has_java() is True
