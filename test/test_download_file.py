import os
from unittest import TestCase

from hexlib.web import download_file
import warnings


class TestDownloadFile(TestCase):

    def setUp(self) -> None:
        warnings.filterwarnings(action="ignore", category=ResourceWarning)

    def test_download_file(self):
        download_file("https://github.com/simon987/hexlib/raw/master/10MB.bin", "/tmp/10Mb.dat")
        self.assertTrue(os.path.exists("/tmp/10Mb.dat"))
        os.remove("/tmp/10Mb.dat")

    def test_download_file_error(self):
        exceptions = []

        def cb(ex):
            exceptions.append(ex)

        download_file("http://thisUrlIsInvalid", "/tmp/file.txt", err_cb=cb, retries=3)
        self.assertFalse(os.path.exists("/tmp/10Mb.dat"))
        self.assertEqual(len(exceptions), 3)

    def test_download_file_meta(self):
        download_file("https://github.com/simon987/hexlib/raw/master/10MB.bin", "/tmp/10Mb.dat", save_meta=True)
        self.assertTrue(os.path.exists("/tmp/10Mb.dat"))
        self.assertTrue(os.path.exists("/tmp/10Mb.dat.meta"))
        os.remove("/tmp/10Mb.dat")
        os.remove("/tmp/10Mb.dat.meta")
