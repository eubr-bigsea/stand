# -*- coding: utf-8 -*-
import sys

import yaml

import os


def load():
    if 'STAND_CONFIG' in os.environ and os.environ.get('STAND_CONFIG'):
        with open(os.environ.get('STAND_CONFIG')) as f:
            config = yaml.load(f.read())
    else:
        print >> sys.stderr, 'Please, set STAND_CONFIG environment variable'
        sys.exit(1)
    return config

stand_configuration = load()
