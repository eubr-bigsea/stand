#!/bin/bash

python ./stand/manage.py db upgrade
python ./stand/app.py
