from setuptools import setup

setup(
    name="hexlib",
    version="1.26",
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
        "u-msgpack-python"
    ]
)
