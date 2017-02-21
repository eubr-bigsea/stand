#!/bin/bash

python ./limonero/manage.py db upgrade
python ./limonero/app.py
