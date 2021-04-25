from unittest import TestCase

from hexlib.web import url_query_value


class TestWebMiscFuncs(TestCase):
    def test_qs_1(self):
        url = "https://test.com/page?a=1&b=2&a=2&c=hello"

        self.assertEqual(url_query_value(url, "a"), "1")
        self.assertEqual(url_query_value(url, "b"), "2")
        self.assertEqual(url_query_value(url, "c"), "hello")
        self.assertEqual(url_query_value(url, "D"), None)

    def test_qs_as_list(self):
        url = "https://test.com/page?a=1&b=2&a=2&c=hello"

        self.assertEqual(url_query_value(url, "a", as_list=True), ["1", "2"])
        self.assertEqual(url_query_value(url, "b", as_list=True), ["2"])
        self.assertEqual(url_query_value(url, "c", as_list=True), ["hello"])
        self.assertEqual(url_query_value(url, "D", as_list=True), [])
