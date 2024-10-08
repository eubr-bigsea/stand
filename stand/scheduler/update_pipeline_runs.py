from stand.models import (
    PipelineRun,
    StatusExecution,
)

# Basic code to start a scheduler using aiocron package
import typing
from datetime import datetime

from stand.scheduler.commands import (
    Command,
    CreatePipelineRun,
    UpdatePipelineInfo,
    UpdatePipelineRunStatus
    
)


def get_pipeline_run_commands(
    updated_pipelines: typing.Dict,
    pipeline_runs: typing.List,
    current_time: datetime,
) -> typing.List[Command]:
    """Update pipelines' runs"""
    runs = dict([[r.pipeline_id, r] for r in pipeline_runs])

    commands = []

    for pipeline in updated_pipelines.values():
        run: PipelineRun = runs.get(pipeline["id"])
       
        if run:
       
                # run expired
                if run.finish < current_time:
                    # all steps completed
                    if run.last_executed_step == len(run.steps):
                        commands.append(
                            UpdatePipelineRunStatus(
                                pipeline_run=run,
                                status=StatusExecution.COMPLETED,
                            )
                        )
                        commands.append(CreatePipelineRun(pipeline=pipeline))
                    # not all steps completed
                    else:
                        commands.append(
                            UpdatePipelineRunStatus(
                                pipeline_run=run, status=StatusExecution.PENDING
                            )
                        )
                        commands.append(CreatePipelineRun(pipeline=pipeline))

                # Test if run is using latest pipeline data
                
                elif run.updated < datetime.strptime(pipeline["updated"], "%Y-%m-%dT%H:%M:%S"):
                    commands.append(
                        UpdatePipelineInfo(
                            pipeline_run=run, update_time=pipeline["updated"]
                        )
                    )

        else:
            commands.append(CreatePipelineRun(pipeline=pipeline))

    return commands
