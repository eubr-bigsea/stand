# -*- coding: utf-8 -*-
import yaml

import os


def load():
    with open(os.environ.get('STAND_CONFIG_FILE')) as f:
        config = yaml.load(f.read())
    return config

stand_configuration = load()
