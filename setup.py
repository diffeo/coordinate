#!/usr/bin/env python

import os
import sys
import fnmatch
import subprocess

## prepare to run PyTest as a command
from distutils.core import Command

from setuptools import setup, find_packages

from version import get_git_version
VERSION, SOURCE_HASH = get_git_version()
PROJECT = 'coordinate'
URL = 'http://github.com/diffeo/coordinate'
AUTHOR = 'Diffeo, Inc.'
AUTHOR_EMAIL = 'support@diffeo.com'
DESC = 'Tools for large scale data clustering.'
LICENSE = 'MIT/X11 license http://opensource.org/licenses/MIT'

def read_file(file_name):
    file_path = os.path.join(
        os.path.dirname(__file__),
        file_name
    )
    return open(file_path).read()


def recursive_glob(treeroot, pattern):
    results = []
    for base, dirs, files in os.walk(treeroot):
        goodfiles = fnmatch.filter(files, pattern)
        results.extend(os.path.join(base, f) for f in goodfiles)
    return results

def recursive_glob_with_tree(root_path, *patterns, **kwargs):
    '''
    generate a list of two-tuples that is appropriate for appending to
    the data_files kwarg to setup

    The two-tuples are (destdir, sourcefile), where destdir is the
    directory path in the package where the sourcefile should appear.

    @cwd (kwarg): os.chdir to this directory before os.walk
    @root_path: directory to walk
    @patterns: zero or more pattens to filter the file names
    '''
    results = []
    ## temporarily change the working dir
    old_cwd = os.getcwd()
    cwd = kwargs.get('cwd', '.')
    os.chdir(cwd)
    for base, dirs, files in os.walk(root_path):
        one_dir_results = []
        for pattern in patterns:
            for fname in fnmatch.filter(files, pattern):
                ## build full paths from the previous working dir
                one_dir_results.append(os.path.join(cwd, base, fname))
        results.append((base, one_dir_results))
    ## put working dir back to what it was previously
    os.chdir(old_cwd)
    return results

class InstallTestDependencies(Command):
    '''install test dependencies'''

    description = 'installs all dependencies required to run all tests'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def easy_install(self, packages):
        cmd = ['easy_install']
        if packages:
            cmd.extend(packages)
            errno = subprocess.call(cmd)
            if errno:
                raise SystemExit(errno)

    def run(self):
        if self.distribution.install_requires:
            self.easy_install(self.distribution.install_requires)
        if self.distribution.tests_require:
            self.easy_install(self.distribution.tests_require)


class PyTest(Command):
    '''run py.test'''

    description = 'runs py.test to execute all tests'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        if self.distribution.install_requires:
            self.distribution.fetch_build_eggs(
                self.distribution.install_requires)
        if self.distribution.tests_require:
            self.distribution.fetch_build_eggs(
                self.distribution.tests_require)

        errno = subprocess.call([sys.executable, 'runtests.py'])
        raise SystemExit(errno)

setup(
    name=PROJECT,
    version=VERSION,
    # source_label=SOURCE_HASH,
    license=LICENSE,
    description=DESC,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(),
    cmdclass={'test': PyTest,
              'install_test': InstallTestDependencies},
    # We can select proper classifiers later
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Topic :: Utilities',
        'License :: Diffeo',
    ],
    tests_require=[
    ],
    install_requires=[
        'cbor >= 0.1.17',
        'python-daemon',
        'dblogger',
        'lockfile',
        'psutil',
        'pyyaml',
        'setproctitle',
        'yakonfig',
        'psycopg2', # not generally necessary, comment out if you don't want it
    ],
    extras_require = {
        'jinja': [  # used for http info serving
            'Jinja2',
        ],
    },
    include_package_data=True,
    # We don't want all the data files shipping.
    # + recursive_glob_with_tree('data', '*'),
    entry_points={
        'console_scripts': [
            'coordinate = coordinate.coordinatec:main',
            'coordinated = coordinate.run:main',
            'coordinate_worker = coordinate.run_multi_worker:main',
        ]
    },
)
