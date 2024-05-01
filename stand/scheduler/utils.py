from stand.models import (
    Job,
    PipelineRun,
    PipelineStepRun,
    StatusExecution,
    Pipeline,
    PipelineStep,
)


def get_latest_pipeline_step_run(run: PipelineRun) -> PipelineStepRun:
    pass


def get_latest_job_from_pipeline_step_run(step_run: PipelineStepRun) -> Job:
    pass


def update_pipeline_step_run_status(step_run: PipelineStepRun, status: StatusExecution):
    pass


def update_pipeline_run_status(pipeline_run: PipelineRun, status: StatusExecution):
    pass


def get_job_status(job: Job) -> StatusExecution:
    pass


def get_pipeline_steps(pipeline: Pipeline):
    pass


def create_step_run(pipelineStep: PipelineStep):
    pass
