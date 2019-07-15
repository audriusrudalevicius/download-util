import setuptools
from os import path

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt', session='work')
reqs = [str(ir.req) for ir in install_reqs]

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'readme.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name="download-util",
    version="0.0.1",
    author="d3trax",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/audriusrudalevicius/download-util",
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    install_requires=reqs,
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
)
