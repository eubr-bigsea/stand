from flask_babel import gettext
import logging

import requests
from configuration import load


log = logging.getLogger()
log.setLevel(logging.DEBUG)

tahiti_config = load()

def query_tahiti(item_path,
                 item_id=None,
                 base_url=tahiti_config['services']['tahiti']['url'],
                 token=tahiti_config['services']['tahiti']['auth_token'],
                 params:dict=None):

    headers = {'X-Auth-Token': token}

    if item_id == '' or item_id is None:
        url = '{}/{}'.format(
            base_url, item_path if item_path[0] != '/' else item_path[1:])
    else:
        url = '{}/{}/{}'.format(
            base_url, item_path if item_path[0] != '/' else item_path[1:],
            item_id)

    log.debug('Querying Tahiti URL: {}'.format(url))

    r = requests.get(url, params, headers=headers)
    if r.status_code == 200:
        return r.json()["data"]
    else:
        raise RuntimeError(gettext(
            "Error loading data from tahiti: id {}: HTTP {} - {}  ({})").format(
            item_id, r.status_code, r.text, url))