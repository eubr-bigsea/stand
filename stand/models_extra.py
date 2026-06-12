from dataclasses import dataclass
from datetime import datetime
import typing


## duplicated model that also exists in tahiti
## this class should be used only for convenience
@dataclass
class Workflow:
    id: int
    name: str
    type: str

@dataclass
class PipelineStep:
    """Pipeline step"""

    id: int
    name: str
    order: int
    enabled: bool
    scheduling: typing.Optional[str] = None
    # trigger_type: int
    description: str = ''
    workflow: typing.Any = None
    def __setattr__(self, prop, val):
        new_val = val
        if prop == 'workflow' and val is not None:
            new_val = Workflow(**val)
        super().__setattr__(prop, new_val)


@dataclass
class Pipeline:
    id: int
    name: str
    enabled: bool
    user_id: int
    user_login: str
    user_name: str
    created: datetime
    updated: datetime
    version: int
    steps: typing.List[PipelineStep]
    identifier: typing.Optional[str] = None
    execution_window: typing.Optional[int] = None
    variables: typing.Optional[str] = None
    preferred_cluster_id: typing.Optional[int] = None
    description: str = ''
    periodicity: str = 'monthly'
    periodicity_start: int = 1
    periodicity_interval: typing.Optional[int] = None
    run_creation_method: typing.Optional[str] = None
    def __setattr__(self, prop, val):
        new_val = val
        if prop == 'steps':
            new_val = [PipelineStep(**v) for v in val]
        super().__setattr__(prop, new_val)


@dataclass
class Period:
    start: datetime
    finish: datetime
