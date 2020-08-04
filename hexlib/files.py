import os
from io import BytesIO
from tarfile import TarFile, TarInfo


def ftw(path):
    for cur, _dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(cur, file)


def add_bytes_to_tar(tar: TarFile, filename: str, data: bytes):
    buf = BytesIO()
    buf.write(data)
    buf.flush()
    buf.seek(0)

    info = TarInfo(name=filename)
    info.size = len(data)
    tar.addfile(info, buf)


def add_buf_to_tar(tar: TarFile, filename: str, buf: BytesIO):
    buf.flush()
    buf.seek(0)

    info = TarInfo(name=filename)
    info.size = len(buf.getvalue())
    tar.addfile(info, buf)
