import os
from io import BytesIO, BufferedReader
from tarfile import TarFile, TarInfo
import subprocess
import gzip
import zstandard

try:
    import orjson as json
except ImportError:
    import json


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


def _is_executable(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)


def find_program(*programs):
    for program in programs:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if _is_executable(exe_file):
                return exe_file


def program_is_in_path(program) -> bool:
    for path in os.environ["PATH"].split(os.pathsep):
        exe_file = os.path.join(path, program)
        if _is_executable(exe_file):
            return True

    return False


COMPRESSION_GZIP = "gz"
COMPRESSION_ZSTD = "zstd"


class NDJsonLine:
    __slots__ = "text"

    def __init__(self, text):
        self.text = text

    def json(self):
        return json.loads(self.text)


def ndjson_iter(*files, compression=""):
    for file in files:
        cleanup = None
        if compression == COMPRESSION_GZIP:
            prog = find_program("pigz", "gzip")
            if prog:
                process = subprocess.Popen([prog, "-dc", file], stdout=subprocess.PIPE)
                line_iter = process.stdout
            else:
                # This is much slower
                line_iter = BufferedReader(gzip.open(file))
        elif compression == COMPRESSION_ZSTD:
            fp = open(file, "rb")
            dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
            reader = dctx.stream_reader(fp)
            line_iter = BufferedReader(reader)

            def cleanup():
                fp.close()
                reader.close()

        else:
            line_iter = open(file)

            def cleanup():
                line_iter.close()

        for line in line_iter:
            yield NDJsonLine(line)
        if cleanup:
            cleanup()
