import re

LINK_RE = re.compile(r"(https?://[\w\-_.]+\.[a-z]{2,4}([^\s<'\"]*|$))")
HTML_HREF_RE = re.compile(r"href=\"([^\"]+)\"")
WHITESPACE_RE = re.compile(r"\s+")
PUNCTUATION_RE = re.compile(r"[.,;:\"!?]+")
