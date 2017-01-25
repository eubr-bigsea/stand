# -*- coding: utf-8 -*-}
from datetime import datetime


def get_now():
    """
     Returns current date and time. Avoid using datetime module because here
     we could centralize date configuration (e.g. timezone) and it facilitates
     testing.
    """
    return datetime.now()
