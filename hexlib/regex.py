import re

LINK_RE = re.compile(r"(https?://[\w\-_.]+\.[a-z]{2,4}([^\s<'\"]*|$))")
HTML_HREF_RE = re.compile(r"href=\"([^\"]+)\"")
