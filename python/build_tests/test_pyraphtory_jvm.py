from pathlib import Path
import tempfile
import unittest
from unittest import mock
import platform
import os
import tarfile
print(os.environ.get("PYTHONPATH"))
import sys
sys.path.append(str(Path(__file__).parent.parent / "_custom_build"))

import requests
from ivy import *
from download import *
from check_platform import *
from java import *
import java


LOTR_CHECKSUM = '8c7400d7463b4f45daff411a5521cbcadec4cabd624200ed0c17d68dc7c99a3f'
LOTR_URL = 'https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv'
ARCH = 'TEST_ARCH'
OS = 'TEST_OS'


def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    print("Tar file created {}".format(output_filename))


def does_url_exist(url):
    try:
        request = requests.head(url, allow_redirects=True)
        return request.status_code == requests.codes.ok
    except requests.ConnectionError:
        return False


class JRETest(unittest.TestCase):

    def test_getOS(self):
        this_os = platform.system()
        self.assertTrue(getOS() == this_os)

    def test_getArch(self):
        this_arch = platform.machine()
        if this_arch == "x86_64":
            this_arch = 'x64'
        elif platform.machine() == 'arm64':
            this_arch = 'aarch64'
        self.assertEqual(getArch(), this_arch)

    def test_MacJavaFileExists(self):
        mac_x64_url = SOURCES.get(OS_MAC).get(OS_X64).link
        mac_arch_url = SOURCES.get(OS_MAC).get(OS_AARCH64).link
        self.assertTrue(does_url_exist(url=mac_x64_url))
        self.assertTrue(does_url_exist(url=mac_arch_url))

    def test_LinuxJavaFileExists(self):
        self.assertTrue(does_url_exist(SOURCES.get(OS_LINUX).get(OS_X64).link))
        self.assertTrue(does_url_exist(SOURCES.get(OS_LINUX).get(OS_AARCH64).link))

    def test_IVYFileExists(self):
        self.assertTrue(does_url_exist(IVY_BIN.link))

    def test_safe_download(self):
        with tempfile.TemporaryDirectory() as temporary_folder:
            # download a file to the temporary folder
            file_loc = safe_download_file(temporary_folder, IVY_BIN)
            # check that the file exists
            self.assertTrue(os.path.exists(file_loc))

    # Test get_java_home() when java is not installed


    # Test the jre.getOS function and mock platform.system
    def test_getOS_fail(self):
        with mock.patch('platform.system') as mock_system:
            mock_system.return_value = 'HaaroonOS'
            # Test the jre.getArch function and mock platform.machine
            self.assertRaises(Exception, getOS)

    # Test the jre.getArch function and mock platform.system
    def test_getArch_arm64(self):
        with mock.patch('platform.machine') as mock_machine:
            mock_machine.return_value = 'arm64'
            # Test the jre.getArch function and mock platform.machine
            self.assertEqual(getArch(), 'aarch64')

    # Test the jre.getArch function and mock platform.system
    def test_getArch_fail(self):
        with mock.patch('platform.machine') as mock_machine:
            mock_machine.return_value = 'HaaroonArch'
            # Test the jre.getArch function and mock platform.machine
            self.assertRaises(Exception, getArch)

    # Test the jre.delete source function, make a new temproary file, delete the file and check that it is deleted
    def test_delete_source(self):
        # Python make a temporary folder
        temp_file = tempfile.mkstemp()[1]
        delete_source(temp_file)
        self.assertFalse(os.path.exists(temp_file))

    # Test the jre.checksum function, make a new temporary file and assert that the checksum is false
    def test_checksum_fails(self):
        temp_file = tempfile.mkstemp()[1]
        self.assertFalse(checksum(temp_file, 'test'))

    # Test the jre.download_java function
    # mock the sources to use a small test file
    # ensure it passes the checksum
    @mock.patch("java.SOURCES", {OS: {ARCH: Link(LOTR_URL, LOTR_CHECKSUM)}})
    def test_download_java(self):
        self.assertEqual(download_java(OS, ARCH, '/tmp/abc'), Path('/tmp/abc/lotr.csv'))

    @mock.patch("java.SOURCES", {OS: {ARCH: Link(LOTR_URL, '11')}})
    def test_download_java_fails(self):
        self.assertRaises(SystemExit, download_java, OS, ARCH, '/tmp/')

    @mock.patch("java.SOURCES", {'TEST': {ARCH: Link(LOTR_URL, '11')}})
    def test_download_java_bad_os(self):
        self.assertRaises(SystemExit, download_java, OS, ARCH, '/tmp/')


    # Test the jre.unpack_jre function and mock shutil.unpack_archive
    @mock.patch('shutil.unpack_archive')
    def test_unpack_jre_exc(self, mock_unpack_archive):
        ts = tempfile.mkdtemp()
        tf = tempfile.mkstemp()[1]
        mock_unpack_archive.return_value = None
        # Run the function
        self.assertRaises(Exception, unpack_jre, tf, ts)
