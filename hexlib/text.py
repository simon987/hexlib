from functools import partial
from multiprocessing.pool import Pool

import nltk.corpus
from lxml import etree
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from .regex import WHITESPACE_RE, PUNCTUATION_RE

get_text = etree.XPath("//text()")

stop_words_en = set(stopwords.words("english"))

nltk.download("stopwords", quiet=True)
nltk.download("wordnet", quiet=True)

lemmatizer = WordNetLemmatizer()


def clean_multicore(texts, processes, **kwargs):
    pool = Pool(processes=processes)
    return pool.map(
        func=partial(clean, **kwargs),
        iterable=texts,
    )


def clean(text, compress_whitespace=False, lowercase=False, clean_html=False, strip=False, remove_punctuation=False,
          remove_stopwords_en=False, lemmatize=False, fix_single_quotes=False):
    if compress_whitespace and remove_stopwords_en:
        raise ValueError("Redundant flags: remove_stopwords implies compress_whitespace")

    if fix_single_quotes:
        text = text.replace("`", "'")

    if clean_html:
        try:
            root = etree.fromstring(text)
            text = "".join(get_text(root))
        except:
            pass

    if remove_punctuation:
        text = PUNCTUATION_RE.sub(" ", text)

    if lowercase:
        text = text.lower()

    if compress_whitespace:
        text = WHITESPACE_RE.sub(" ", text)

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
