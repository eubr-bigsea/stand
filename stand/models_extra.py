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
    scheduling: str = None
    # trigger_type: int
    description: str = ''
    workflow: any = None
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
    execution_window: int = None
    variables: str = None
    preferred_cluster_id: int = None
    description: str = ''

    def __setattr__(self, prop, val):
        new_val = val
        if prop == 'steps':
            new_val = [PipelineStep(**v) for v in val]
        super().__setattr__(prop, new_val)


@dataclass
class Period:
    start: datetime
    finish: datetime
