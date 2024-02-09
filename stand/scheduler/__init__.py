from datetime import datetime
import croniter
from typing import List


def all_cron_executions(
        cron_expression: str, 
        start_date: datetime, 
        end_date: datetime) -> List[datetime]:
    """
    Calculate all execution datetimes for a given crontab expression
    within a range of dates.

    Args:
    - cron_expression (str): The crontab expression.
    - start_date (datetime): The start date.
    - end_date (datetime): The end date.

    Returns:
    - list: List of datetime objects representing all execution datetimes.
    """
    cron = croniter.croniter(cron_expression, start_date)
    executions = []

    next_execution = cron.get_next(datetime)

    while next_execution <= end_date:
        if next_execution >= start_date:
            executions.append(next_execution)
        next_execution = cron.get_next(datetime)

    return executions
