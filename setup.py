from setuptools import setup

setup(
    name="hexlib",
    version="1.0",
    description="Midx ",
    author="simon987",
    author_email="me@simon987.net",
    packages=["hexlib"],
    install_requires=[
        "ImageHash", "influxdb",
    ]
)
