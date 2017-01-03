# -*- coding: utf-8 -*-
import requests
from flask import current_app


class TahitiService:
    def __init__(self):
        pass

    @staticmethod
    def get_workflow(workflow_id):
        """ Queries Tahiti component and returns Workflow information """
        url = current_app.config['STAND_CONFIG']['services']['tahiti']['url']
        token = current_app.config['STAND_CONFIG']['services']['tahiti'][
            'token']
        headers = {}
        r = requests.get('{}/{}'.format(url, workflow_id), headers=headers)
        if r.status_code == 200:
            pass
        else:
            raise Exception()
