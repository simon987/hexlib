from http.cookiejar import Cookie
import re

from dateutil.parser import parse
import pickle

from requests.cookies import RequestsCookieJar


def cookie_from_string(text: str, domain: str) -> Cookie:

    tokens = [t.strip() for t in text.split(";")]

    name, value = tokens[0].split("=")

    path = None
    expires = None

    for tok in tokens[1:]:
        k, v = tok.split("=")
        if k == "path":
            path = v
        if k == "expires":
            expires = parse(v).timestamp()

    return Cookie(
        version=0,
        name=name, value=value,
        port=None, port_specified=False,
        domain=domain, domain_specified=True, domain_initial_dot=False,
        path=path, path_specified=path is not None,
        secure=False,
        expires=expires,
        discard=None,
        comment=None,
        comment_url=None,
        rest=None,
        rfc2109=False
    )


def save_cookiejar(cj, filename):
    with open(filename, "wb") as f:
        f.truncate()
        pickle.dump(cj._cookies, f)


def load_cookiejar(filename):
    with open(filename, "rb") as f:
        cookies = pickle.load(f)
        cj = RequestsCookieJar()
        cj._cookies = cookies
    return cj


def cookiejar_filter(cj, pattern):
    filtered_cj = RequestsCookieJar()
    for c in cj:
        if re.match(pattern, c.domain):
            filtered_cj.set_cookie(c)
    return filtered_cj
