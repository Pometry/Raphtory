from pathlib import Path
import tempfile
import unittest
from unittest import mock
import platform
import os
import tarfile

import requests
from pyraphtory_jvm import jre as jre

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
        self.assertTrue(jre.getOS() == this_os)

    def test_getArch(self):
        this_arch = platform.machine()
        if this_arch == "x86_64":
            this_arch = 'x64'
        elif platform.machine() == 'arm64':
            this_arch = 'aarch64'
        self.assertEqual(jre.getArch(), this_arch)

    def test_MacJavaFileExists(self):
        mac_x64_url = jre.SOURCES.get(jre.OS_MAC).get(jre.OS_X64).get(jre.LINK)
        mac_arch_url = jre.SOURCES.get(jre.OS_MAC).get(jre.OS_AARCH64).get(jre.LINK)
        self.assertTrue(does_url_exist(url=mac_x64_url))
        self.assertTrue(does_url_exist(url=mac_arch_url))

    def test_LinuxJavaFileExists(self):
        self.assertTrue(does_url_exist(jre.SOURCES.get(jre.OS_LINUX).get(jre.OS_X64).get(jre.LINK)))
        self.assertTrue(does_url_exist(jre.SOURCES.get(jre.OS_LINUX).get(jre.OS_AARCH64).get(jre.LINK)))

    def test_IVYFileExists(self):
        self.assertTrue(does_url_exist(jre.IVY_BIN.get(jre.LINK)))

    def test_safe_download(self):
        temporary_folder = tempfile.mkdtemp()
        # download a file to the temporary folder
        ivy_checksum = jre.IVY_BIN.get(jre.CHECKSUM_SHA256)
        ivy_url = jre.IVY_BIN.get(jre.LINK)
        file_loc = jre.safe_download_file(temporary_folder, ivy_checksum, ivy_url)
        # check that the file exists
        self.assertTrue(os.path.exists(file_loc))
        # delete the temporary folder and its contents
        os.unlink(file_loc)
        os.rmdir(temporary_folder)

    # Test get_java_home() when java is not installed
    def test_get_java_home_no_java(self):
        # Mock the os.getenv function to return None
        with mock.patch('os.getenv') as mock_getenv:
            mock_getenv.return_value = 'test'
            # Run the function
            java_home = jre.get_java_home()
            # Check that it returns None
            self.assertEqual(java_home, 'test/bin/java')
        # Mock the shutil.which function to return True
        with mock.patch('shutil.which') as mock_which:
            with mock.patch('os.getenv') as mock_getenv:
                mock_getenv.return_value = None
                mock_which.return_value = '/tmp/test'
                # Run the function
                java_home = jre.get_java_home()
                # Check that it returns None
                self.assertEqual(java_home, '/tmp/test')
        # Mock the shutil.which function and the mock.patch function to return None
        with mock.patch('shutil.which') as mock_which:
            with mock.patch('os.getenv') as mock_getenv:
                mock_getenv.return_value = None
                mock_which.return_value = None
                # assert that it throws an exception
                self.assertRaises(FileNotFoundError, jre.get_java_home)

    # Test the jre.getOS function and mock platform.system
    def test_getOS_fail(self):
        with mock.patch('platform.system') as mock_system:
            mock_system.return_value = 'HaaroonOS'
            # Test the jre.getArch function and mock platform.machine
            self.assertRaises(Exception, jre.getOS)

    # Test the jre.getArch function and mock platform.system
    def test_getArch_arm64(self):
        with mock.patch('platform.machine') as mock_machine:
            mock_machine.return_value = 'arm64'
            # Test the jre.getArch function and mock platform.machine
            self.assertEqual(jre.getArch(), 'aarch64')

    # Test the jre.getArch function and mock platform.system
    def test_getArch_fail(self):
        with mock.patch('platform.machine') as mock_machine:
            mock_machine.return_value = 'HaaroonArch'
            # Test the jre.getArch function and mock platform.machine
            self.assertRaises(Exception, jre.getArch)

    # Test the jre.delete source function, make a new temproary file, delete the file and check that it is deleted
    def test_delete_source(self):
        # Python make a temporary folder
        temp_file = tempfile.mkstemp()[1]
        jre.delete_source(temp_file)
        self.assertFalse(os.path.exists(temp_file))

    # Test the jre.checksum function, make a new temporary file and assert that the checksum is false
    def test_checksum_fails(self):
        temp_file = tempfile.mkstemp()[1]
        self.assertFalse(jre.checksum(temp_file, 'test'))

    # Test the jre.download_java function
    # mock the sources to use a small test file
    # ensure it passes the checksum
    @mock.patch("pyraphtory_jvm.jre.SOURCES", {OS: {ARCH: {'link': LOTR_URL, 'checksum': LOTR_CHECKSUM}}})
    def test_download_java(self):
        self.assertEqual(jre.download_java(OS, ARCH, '/tmp/abc'), '/tmp/abc/lotr.csv')

    @mock.patch("pyraphtory_jvm.jre.SOURCES", {OS: {ARCH: {'link': LOTR_URL, 'checksum': '11'}}})
    def test_download_java_fails(self):
        self.assertRaises(SystemExit, jre.download_java, OS, ARCH, '/tmp/')

    @mock.patch("pyraphtory_jvm.jre.SOURCES", {'TEST': {ARCH: {'link': LOTR_URL, 'checksum': '11'}}})
    def test_download_java_bad_os(self):
        self.assertRaises(SystemExit, jre.download_java, OS, ARCH, '/tmp/')

    # Mock the subprocess.call method
    @mock.patch('subprocess.run')
    @mock.patch('subprocess.CompletedProcess')
    def test_get_and_run_ivy(self, mock_subprocess_completedprocess, mock_subprocess_run):
        with mock.patch('os.listdir') as fname_endswith:
            fname_endswith.return_value = []
            mock_subprocess_completedprocess.stdout = b'success'
            mock_subprocess_completedprocess.stderr = b''
            mock_subprocess_completedprocess.returncode = 0
            mock_subprocess_run.return_value = mock_subprocess_completedprocess
            # Run the function
            self.assertIsNone(jre.get_and_run_ivy('f', '/tmp/'))

    # Test the jre.unpack_jre function and mock shutil.unpack_archive
    @mock.patch('shutil.unpack_archive')
    def test_unpack_jre_exc(self, mock_unpack_archive):
        ts = tempfile.mkdtemp()
        tf = tempfile.mkstemp()[1]
        mock_unpack_archive.return_value = None
        # Run the function
        self.assertRaises(Exception, jre.unpack_jre, tf, ts)
