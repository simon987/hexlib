import os


def ftw(path):
    for cur, _dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(cur, file)

