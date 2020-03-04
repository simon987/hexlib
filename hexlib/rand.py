import random


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

