from stand.models import PipelineWindow, StatusExecution, PipelineStepWindowLog
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from gettext import gettext
from typing import Tuple
import calendar

# Create new pipeline window from a list of dictionaries containing the info
def create_pipeline_window(session: Session, pipeline_info_list, 
                           reference: datetime, user):

    pipeline_ids = dict([(p['id'], p) for p in pipeline_info_list])

    
    canceled_windows = session.query(PipelineWindow).filter(
        PipelineWindow.pipeline_id.not_in_(pipeline_ids.keys())
    ).filter(PipelineWindow.status != StatusExecution.CANCELED)
    
    update_when_pipeline_canceled(user, canceled_windows)


    existing_windows = session.query(PipelineWindow).filter(
        PipelineWindow.pipeline_id.in_(pipeline_ids.keys())
    ).filter(PipelineWindow.status != StatusExecution.CANCELED)

    for pipeline_info in pipeline_info_list:
        exists = False
        start, end = get_periodicity(pipeline_info, reference)
        # Test if exist a window for this pipeline and periodicity
        for window in existing_windows:
            if (window.pipeline_id == pipeline_info["id"] 
                    and window.start == start and window.end == end):
                exists = True
                break
        if not exists:
            pipeline_window = PipelineWindow(
                pipeline_id=pipeline_info["id"],
                start=None,
                end=None,
                status=StatusExecution.PENDING,
            )
            session.add(pipeline_window)
            session.commit()

def get_periodicity(pipeline_info, reference: datetime) -> Tuple[datetime, datetime]:
    start = None
    end = None
    if pipeline_info["periodicity"] == "daily":
         # start has hour equals to 00:00:00
         start = datetime(reference.year, reference.month, 
                                   reference.day, 
                                   hour=0, minute=0, second=0, microsecond=0)
         end = reference.replace(hour=23, minute=59, second=59, microsecond=999999);
    elif pipeline_info["periodicity"] == "weekly":
         dow = reference.weekday()
         start = reference - timedelta(days=dow)
         start = datetime(year=start.year, month=start.month, day=start.day, 
                          hour=0, minute=0, second=0, microsecond=0)
         
         end = (start + timedelta(days=7)).replace(hour=23, minute=59, second=59, microsecond=999999)
    elif pipeline_info["periodicity"] == "monthly":
         start = datetime(year=reference.year, month=reference.month, day=1,
                          hour=0, minute=0, second=0, microsecond=0)
         last_day_of_month = calendar.monthrange(reference.year, reference.month)[1]
         end = datetime(year=reference.year, month=reference.month, day=last_day_of_month,
                        hour=0, minute=0, second=0, microsecond=0)
    
    return (start, end)
    

def update_when_pipeline_canceled(user, canceled_windows):
    for canceled in canceled_windows:
        canceled.status = StatusExecution.CANCELED
        canceled.final_status = StatusExecution.CANCELED
        for step in canceled.steps:
            step.updated = datetime.now()
            step.comment = gettext('Pipeline was canceled or removed.')
            step.final_status = StatusExecution.CANCELED
            step.status = StatusExecution.CANCELED
            log = PipelineStepWindowLog(
                action='CANCELED',
                user_id=user.id,
                user_login=user.login,
                user_name=user.name,
                comment=step.comment,
            )
            step.logs.append(log)


