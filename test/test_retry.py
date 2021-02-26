from unittest import TestCase

from hexlib.misc import retry


class TestRetry(TestCase):

    def test_simple(self):
        @retry(attempts=3)
        def a(i):
            return i + 1

        self.assertEqual(a(1), 2)

    def test_error(self):
        arr = []

        def cb(e):
            arr.append(e)

        @retry(attempts=3, callback=cb)
        def a(i):
            raise Exception("err")

        a(1)

        self.assertEqual(3, len(arr))
