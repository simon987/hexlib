import pickle
import re
import os
from datetime import datetime
from base64 import b64encode, b64decode
from http.cookiejar import Cookie
from time import time
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup

import requests
import orjson as json

from dateutil.parser import parse
from requests.cookies import RequestsCookieJar

from hexlib.misc import rate_limit, retry


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


def encode_cookiejar(cj):
    return b64encode(pickle.dumps(cj._cookies)).decode()


def decode_cookiejar(data):
    cj = RequestsCookieJar()
    cj._cookies = pickle.loads(b64decode(data))
    return cj


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


def cookiejar_filter_name(cj, pattern):
    filtered_cj = RequestsCookieJar()
    for c in cj:
        if re.match(pattern, c.name):
            filtered_cj.set_cookie(c)
    return filtered_cj


def url_query_value(url, arg, as_list=False):
    qs = urlparse(url).query
    parsed_qs = parse_qs(qs)

    arg = parsed_qs.get(arg, [])

    if as_list:
        return arg if arg else []
    else:
        return arg[0] if arg else None


def download_file(url, destination, session=None, headers=None, overwrite=False, retries=1, err_cb=None,
                  save_meta=False):
    if os.path.exists(destination) and not overwrite:
        return

    if session is None:
        session = requests.session()

    while retries > 0:
        try:
            r = session.get(url, stream=True, headers=headers)

            with open(destination + ".part", "wb") as f:
                for chunk in r.iter_content(chunk_size=4096):
                    if chunk:
                        f.write(chunk)
            os.rename(destination + ".part", destination)

            if save_meta:
                with open(destination + ".meta", "wb") as f:
                    f.write(json.dumps({
                        "headers": dict(**r.headers),
                        "url": url,
                        "timestamp": datetime.utcnow().replace(microsecond=0).isoformat()
                    }))
            break
        except Exception as e:
            if err_cb:
                err_cb(e)
            retries -= 1


class Web:
    def __init__(self, proxy=None, rps=1, retries=3, retry_sleep=0, logger=None, cookie_file=None, retry_codes=None,
                 session=None,
                 ua=None):
        self._cookie_file = cookie_file
        self._proxy = proxy
        self._logger = logger
        self._current_req = None
        if retry_codes is None or not retry_codes:
            retry_codes = {502, 504, 520, 522, 524, 429}
        self._retry_codes = retry_codes

        if session is None:
            session = requests.session()

        self._session = session

        if ua is not None:
            session.headers["User-Agent"] = ua

        if self._cookie_file:
            self._session.cookies = load_cookiejar(cookie_file)

        if self._proxy:
            self._session.proxies = {
                "http": proxy,
                "https": proxy,
            }

        @rate_limit(rps)
        @retry(retries, callback=self._error_callback, retry_sleep=retry_sleep)
        def get(url, **kwargs):
            self._current_req = "GET", url, kwargs
            r = self._session.get(url, **kwargs)

            if r.status_code in self._retry_codes:
                raise Exception(f"HTTP {r.status_code}")
            return r

        self._get = get

        @rate_limit(rps)
        @retry(retries, callback=self._error_callback, retry_sleep=retry_sleep)
        def post(url, **kwargs):
            self._current_req = "POST", url, kwargs
            r = self._session.post(url, **kwargs)

            if r.status_code in self._retry_codes:
                raise Exception(f"HTTP {r.status_code}")
            return r

        self._post = post

    def _error_callback(self, e):
        self._logger.critical(f"{self._format_url(*self._current_req)}: {e}")

    def _format_url(self, method, url, kwargs, r=None):
        if "params" in kwargs and kwargs["params"]:
            return "%s %s?%s <%s>" % (method, url, "&".join(f"{k}={v}" for k, v in kwargs["params"].items()),
                                      r.status_code if r is not None else "ERR")
        else:
            return "%s %s <%s>" % (method, url, r.status_code if r is not None else "ERR",)

    def get(self, url, **kwargs):

        time_start = time()
        r = self._get(url, **kwargs)

        if self._cookie_file:
            save_cookiejar(self._session.cookies, self._cookie_file)

        if self._logger and r is not None:
            self._logger.debug(self._format_url("GET", url, kwargs, r) + " %.2fs" % (time() - time_start))
        return r

    def post(self, url, **kwargs):

        time_start = time()
        r = self._post(url, **kwargs)

        if self._cookie_file:
            save_cookiejar(self._session.cookies, self._cookie_file)

        if self._logger and r is not None:
            self._logger.debug(self._format_url("POST", url, kwargs, r) + " %.2fs" % (time() - time_start))
        return r

    def get_soup(self, url, **kwargs):
        r = self.get(url, **kwargs)
        if not r:
            return None
        return BeautifulSoup(r.content, "html.parser")
