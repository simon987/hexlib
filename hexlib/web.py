from http.cookiejar import Cookie

from dateutil.parser import parse


def cookie_from_string(text: str, domain: str) -> Cookie:

    tokens = [t.strip() for t in text.split(";")]

    name, value = tokens[0].split("=")

    path = None
    expires = None

    for tok in tokens[1:]:
        name, value = tok.split("=")
        if name == "path":
            path = value
        if name == "expires":
            expires = parse(value).timestamp()

    return Cookie(
        version=None,
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
