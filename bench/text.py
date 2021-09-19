from timeit import timeit

t = bytes.maketrans(b".,;:\"!?/()|*=>", b"              ")


def translate(x: str):
    arr = x.encode("utf8")

    return arr.translate(t).decode("utf8")


if __name__ == '__main__':
    res = timeit(
        setup='t = str.maketrans(".,;:\\"!?/()|*=>", "              ")',
        stmt='x = "Hello, world %123 & *".translate(t)'
    )

    # 0.865953s
    print("translate = %fs" % res)

    res = timeit(
        setup='from text import translate',
        stmt='x = translate("Hello, world %123 & *")'
    )

    # 0.865953s
    print("custom = %fs" % res)
