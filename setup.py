from setuptools import setup

setup(
    name="hexlib",
    version="1.40",
    description="Misc utility methods",
    author="simon987",
    author_email="me@simon987.net",
    packages=["hexlib"],
    include_package_data=True,
    package_data={"": [
        "data/*"
    ]},
    install_requires=[
        "ImageHash", "influxdb", "siphash", "python-dateutil", "redis", "orjson", "zstandard",
        "u-msgpack-python", "psycopg2-binary", "fake-useragent", "bs4", "lxml", "nltk"
    ]
)
