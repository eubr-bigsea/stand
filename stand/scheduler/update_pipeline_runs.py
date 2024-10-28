import typing
from datetime import datetime
import pytz
from stand.models import (
    PipelineRun,
    StatusExecution,
)
from stand.scheduler.commands import (
    Command,
    CreatePipelineRun,
    UpdatePipelineInfo,
    UpdatePipelineRunStatus,
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
            finish_utc = run.finish.astimezone(pytz.utc) if run.finish.tzinfo else pytz.utc.localize(run.finish)
            current_time_utc = current_time.astimezone(pytz.utc) if current_time.tzinfo else pytz.utc.localize(current_time)
            if finish_utc < current_time_utc:
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

            elif run.updated < datetime.strptime(
                pipeline["updated"], "%Y-%m-%dT%H:%M:%S"
            ):
                commands.append(
                    UpdatePipelineInfo(
                        pipeline_run=run, update_time=pipeline["updated"]
                    )
                )

        else:
            commands.append(CreatePipelineRun(pipeline=pipeline))

    return commands
