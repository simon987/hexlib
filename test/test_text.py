from unittest import TestCase

from hexlib.text import clean


class TestText(TestCase):

    def test_html_invalid(self):
        text = ""
        cleaned = clean(
            text,
            clean_html=True,
        )
        expected = ""

        self.assertEqual(cleaned, expected)

    def test_html_1(self):
        text = "<div>Hello, <strong>world</strong></div>"
        cleaned = clean(
            text,
            clean_html=True,
        )
        expected = "Hello, world"

        self.assertEqual(cleaned, expected)

    def test_html_2(self):
        text = "<div>Hello, <strong>world</strong></div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True
        )
        expected = "hello, world"

        self.assertEqual(cleaned, expected)

    def test_html_3(self):
        text = "<div>\n Hello, \t\n<strong> world    </strong>\n\t</div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True,
            compress_whitespace=True
        )
        expected = " hello, world "

        self.assertEqual(cleaned, expected)

    def test_html_4(self):
        text = "<div>\n Hello, \t\n<strong> world    </strong>\n\t</div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True,
            compress_whitespace=True,
            strip=True
        )
        expected = "hello, world"

        self.assertEqual(cleaned, expected)

    def test_html_5(self):
        text = "<div>\n Hello, \t\n<strong> world    </strong>\n\t</div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True,
            compress_whitespace=True,
            strip=True,
            remove_punctuation=True
        )
        expected = "hello world"

        self.assertEqual(cleaned, expected)

    def test_html_6(self):
        text = "<div>\n Hello, \t\n<strong>a the world    </strong>\n\t</div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True,
            remove_punctuation=True,
            strip=True,
            remove_stopwords_en=True
        )
        expected = "hello world"

        self.assertEqual(cleaned, expected)

    def test_html_7(self):
        text = "<div>\n Hello, \t\n<strong>a the worlds    </strong>\n\t</div>"
        cleaned = clean(
            text,
            clean_html=True,
            lowercase=True,
            remove_punctuation=True,
            strip=True,
            remove_stopwords_en=True,
            lemmatize=True
        )
        expected = "hello world"

        self.assertEqual(cleaned, expected)
