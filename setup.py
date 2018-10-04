#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
from distutils.core import setup


setup(
        name='logshipper',
        version='0.2.2',
        description='A simple and fast logshipper inspired by logstash.',
        author='Robin Wittler',
        author_email='r.wittler@mysportworld.de',
        maintainer='Robin Wittler',
        maintainer_email='r.wittler@mysportworld.de',
        license='GPL3+',
        install_requires=[
            'simplejson',
            'apscheduler',
            'pyinotify',
            'python-daemon',
            'lockfile',
            'pyelasticsearch',
            'redis',
            'setproctitle',
            'python-dateutil'
        ],
        data_files=[
            ('/usr/sbin', ['logshipper.py']),
            ('/etc/logshipper/conf.d', []),
            ('/etc/logshipper/patterns.d', []),
            (
                '/etc/logshipper',
                ['etc/logshipper/logshipper.cfg',]
            ),
            ('/usr/share/logshipper', ['LICENSE.txt', 'README.txt']),
            ('/var/run/logshipper', []),
            ('/var/log/logshipper', []),
            ('/var/cache/logshipper', []),
        ],
        platforms='POSIX',
)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
