import re
from itertools import chain, repeat

import nltk.corpus
from lxml import etree
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from .regex_util import LINK_RE

get_text = etree.XPath("//text()")

nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)
nltk.download("punkt", quiet=True)

stop_words_en = set(stopwords.words("english"))

extra_stop_words_en = [
    "u", "&", "-", "--"
]

stop_words_en.update(extra_stop_words_en)

lemmatizer = WordNetLemmatizer()


def _transform_bigram(ngram_seq, ngrams):
    for ngram in ngram_seq:
        if ngram in ngrams:
            yield ngram[0] + "_" + ngram[1]

            next(ngram_seq)
        else:
            yield ngram[0]


def _transform_trigram(ngram_seq, ngrams):
    for ngram in ngram_seq:
        if ngram in ngrams:
            # yield ngram[0] + "_" + ngram[1] + "_" + ngram[2]
            yield "_".join(ngram)

            next(ngram_seq)
            next(ngram_seq)
        else:
            yield ngram[0]


SINGLE_QUOTES = ("’", "`", "‘")
SINGLE_QUOTE_TRANS = str.maketrans("".join(SINGLE_QUOTES), "".join(repeat("'", len(SINGLE_QUOTES))))

DASHES = ("–", "⸺", "–", "—")
DASHES_TRANS = str.maketrans("".join(DASHES), "".join(repeat("-", len(DASHES))))

DASHES_RE = re.compile(r"-+")

SPECIAL_PUNCTUATION = ";:\"/()|*=>"
SPECIAL_PUNCTUATION_TRANS = str.maketrans(SPECIAL_PUNCTUATION, " " * len(SPECIAL_PUNCTUATION))

PUNCTUATION = ".,!?"
PUNCTUATION_TRANS = str.maketrans(PUNCTUATION, " " * len(PUNCTUATION))


def preprocess(text, lowercase=False, clean_html=False, remove_punctuation=False, remove_special_punctuation=False,
               remove_stopwords_en=False, lemmatize=False, fix_single_quotes=False, strip_quotes=False,
               strip_dashes=False,
               remove_urls=False, bigrams: set = None, trigrams: set = None, remove_numbers=False,
               use_nltk_tokenizer=False):
    if lowercase:
        text = text.lower()

    if fix_single_quotes:
        text = text.translate(SINGLE_QUOTE_TRANS)

    text = text.translate(DASHES_TRANS)

    if strip_dashes:
        text = DASHES_RE.sub("-", text)

    if remove_urls:
        text = LINK_RE.sub(" ", text)

    if clean_html:
        try:
            text = "<root>" + text + "</root>"

            parser = etree.XMLParser(recover=True)
            root = etree.fromstring(text, parser)

            text = " ".join(get_text(root))
        except:
            pass

    if remove_punctuation:
        text = text.translate(PUNCTUATION_TRANS)

    if remove_special_punctuation:
        text = text.translate(SPECIAL_PUNCTUATION_TRANS)

    if use_nltk_tokenizer:
        words = word_tokenize(text, language="english")
    else:
        words = text.split()

    if strip_quotes:
        words = map(lambda w: w.strip("\"'“”"), words)

    if strip_dashes:
        words = map(lambda w: w.strip("-"), words)

    if bigrams:
        words = _transform_bigram(nltk.bigrams(chain(words, ("*",))), bigrams)

    if trigrams:
        words = _transform_trigram(nltk.trigrams(chain(words, ("*", "*"))), trigrams)

    if remove_numbers:
        words = filter(lambda w: not w.isnumeric(), words)

    if lemmatize:
        words = map(lambda w: lemmatizer.lemmatize(w), words)

    if remove_stopwords_en:
        words = filter(lambda w: w not in stop_words_en, words)

    return filter(lambda w: w != "", words)
