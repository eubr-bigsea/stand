# -*- coding: utf-8 -*-
import yaml

import os


def load():
    if 'STAND_CONFIG' in os.environ and os.environ.get('STAND_CONFIG'):
        with open(os.environ.get('STAND_CONFIG')) as f:
            config = yaml.load(f.read())
    else:
        config = {'stand': {}}
    return config

stand_configuration = load()
