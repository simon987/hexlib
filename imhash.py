import base64


def b64hash(imhash, bcount):
    """ImageHash hash to base64 string"""
    return base64.b64encode(
        sum(1 << i for i, b in enumerate(imhash.hash.flatten()) if b).to_bytes(bcount, "big")
    ).decode("ascii")
