# -*- coding: utf-8 -*-
import sys

import os
import yaml


def load():
    if 'STAND_CONFIG' in os.environ and os.environ.get('STAND_CONFIG') != '':
        with open(os.environ.get('STAND_CONFIG')) as f:
            config = yaml.load(f.read(), Loader=yaml.FullLoader)
    else:
        print('Please, set STAND_CONFIG environment variable',
              file=sys.stderr)
        sys.exit(1)
    return config


stand_configuration = load()
