import os
import random
from functools import lru_cache


@lru_cache
def _words():
    with open(os.path.join(os.path.dirname(__file__), "data/words.txt")) as f:
        return [line.strip().lower() for line in f]


def random_word():
    return random.choice(_words())


def random_phrase(words=10, capitalize=True, suffix="."):
    phrase = " ".join(random_word() for _ in range(words))
    if capitalize:
        phrase = phrase.capitalize()
    return phrase + suffix


def fuzz(buf: bytes, n: int, width: int):
    fuzzed = bytearray(buf)
    for _ in range(n):
        i = random.randint(0, len(buf))
        for off in range(width):
            if i + off < len(buf):
                fuzzed[i + off] = fuzzed[i + off] + 1
    return fuzzed


def fuzz_file(file_in: str, out_files: list, n: int, width: int):
    with open(file_in, "rb") as f:
        buf = f.read()
        for out_path in out_files:
            with open(out_path, "wb") as out:
                out.write(fuzz(buf, n, width))

