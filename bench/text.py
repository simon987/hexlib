from timeit import timeit

if __name__ == '__main__':

    res = timeit(
        setup="from hexlib.text import preprocess",
        stmt='text = "x A b c d e f g h"\ncleaned = preprocess(\n    text,\n    lowercase=True,\n    trigrams={\n        ("a", "b", "c"),\n        ("e", "f", "g"),\n    }\n)'
    )

    print(res)