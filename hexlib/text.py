from functools import partial
from multiprocessing.pool import Pool

import nltk.corpus
from lxml import etree
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from .regex import WHITESPACE_RE, PUNCTUATION_RE, LINK_RE

get_text = etree.XPath("//text()")

stop_words_en = set(stopwords.words("english"))

extra_stop_words_en = [
    "u", "&", "-", "--"
]

stop_words_en.update(extra_stop_words_en)

nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)

lemmatizer = WordNetLemmatizer()


def clean_multicore(texts, processes, **kwargs):
    pool = Pool(processes=processes)
    return pool.map(
        func=partial(clean, **kwargs),
        iterable=texts,
    )


def _transform_bigram(ngram_seq, ngrams):
    for ngram in ngram_seq:
        if ngram in ngrams:
            yield "_".join(ngram)

            ngram_seq.__next__()
        else:
            yield ngram[0]


def clean(text, lowercase=False, clean_html=False, strip=False, remove_punctuation=False,
          remove_stopwords_en=False, lemmatize=False, fix_single_quotes=False, strip_quotes=False,
          remove_urls=False, bigrams: set = None):
    if lowercase:
        text = text.lower()

    if fix_single_quotes:
        text = text.replace("`", "'")

    if remove_urls:
        text = LINK_RE.sub(" ", text)

    if clean_html:
        try:
            root = etree.fromstring(text.replace("&", ""))
            text = "".join(get_text(root))
        except:
            pass

    if remove_punctuation:
        text = PUNCTUATION_RE.sub(" ", text)

    if not remove_stopwords_en or not lemmatize or not strip_quotes:
        text = WHITESPACE_RE.sub(" ", text)

    if strip_quotes:
        words = WHITESPACE_RE.split(text)
        text = " ".join(w.strip("\"'") for w in words)

    if bigrams:
        words = WHITESPACE_RE.split(text)
        words.append("*")
        text = " ".join(_transform_bigram(nltk.bigrams(words), bigrams))

    if remove_stopwords_en or lemmatize:
        words = WHITESPACE_RE.split(text)

        if lemmatize and remove_stopwords_en:
            text = " ".join(lemmatizer.lemmatize(w) for w in words if w not in stop_words_en)
        elif not lemmatize and remove_stopwords_en:
            text = " ".join(w for w in words if w not in stop_words_en)
        elif lemmatize and not remove_stopwords_en:
            text = " ".join(lemmatizer.lemmatize(w) for w in words)

    if strip:
        text = text.strip()

    return text
