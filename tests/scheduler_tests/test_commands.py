from datetime import datetime
import pytest

from stand.scheduler.commands import CreatePipelineRun
from stand.scheduler.utils import load_config

@pytest.mark.parametrize(
    "pipeline,current_time,expected_time",
    [
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(
                year=2024,
                month=5,
                day=20,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=12, day=31, hour=23, minute=30, second=0),
            datetime(
                year=2024,
                month=12,
                day=31,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=0),
            datetime(
                year=2024,
                month=5,
                day=1,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(
                year=2024,
                month=5,
                day=25,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=22, hour=12, minute=0, second=0),
            datetime(
                year=2024,
                month=5,
                day=25,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=19, hour=9, minute=0, second=0),
            datetime(
                year=2024,
                month=5,
                day=25,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=12, day=31, hour=5, minute=0, second=0),
            datetime(
                year=2024,
                month=12,
                day=31,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=2, day=12, hour=15, minute=20, second=0),
            datetime(
                year=2024,
                month=2,
                day=29,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2023, month=2, day=10, hour=10, minute=10, second=10),
            datetime(
                year=2023,
                month=2,
                day=28,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=4, day=5, hour=8, minute=0, second=0),
            datetime(
                year=2024,
                month=4,
                day=30,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        ),
    ],
    ids=[
        "Diario - ano bissexto",
        "Diario - ultimo dia do ano",
        "Diario - inicio do mes",
        "Semanal - meio da semana, ano bissexto",
        "Semanal - comecando no meio da semana",
        "Semanal - começando no domingo",
        "Mensal - ultimo dia de ano bissexto",
        "Mensal - fevereiro de ano bissexto",
        "Mensal - ultimo dia do mes, nao bissexto",
        "Mensal - ultimo dia do mes de 30 dias",
    ],
)
def test_CreatePipelineRun_has_correct_end_time(
    pipeline, current_time, expected_time
):
    """
    tests if the CreatePipelineRun command auxiliar functions
    for correctly generates the end time for the run
    """
    command = CreatePipelineRun(pipeline=pipeline)
    end_time = command.get_pipeline_run_end(current_time=current_time)
    assert end_time == expected_time


@pytest.mark.parametrize(
    "pipeline,current_time,expected_time",
    [
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=20, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=1),
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "daily"},
            datetime(year=2024, month=5, day=31, hour=23, minute=59, second=59),
            datetime(year=2024, month=5, day=31, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "weekly"},  # starts sunday
            datetime(year=2024, month=5, day=20, hour=15, minute=20, second=0),
            datetime(year=2024, month=5, day=19, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=22, hour=10, minute=0, second=0),
            datetime(year=2024, month=5, day=19, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=26, hour=9, minute=0, second=0),
            datetime(year=2024, month=5, day=26, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "weekly"},
            datetime(year=2024, month=5, day=25, hour=23, minute=59, second=59),
            datetime(year=2024, month=5, day=19, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=5, day=20, hour=5, minute=0, second=0),
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=1, day=15, hour=8, minute=0, second=0),
            datetime(year=2024, month=1, day=1, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=5, day=31, hour=23, minute=59, second=59),
            datetime(year=2024, month=5, day=1, hour=0, minute=0, second=0),
        ),
        (
            {"execution_window": "monthly"},
            datetime(year=2024, month=2, day=29, hour=23, minute=59, second=59),
            datetime(year=2024, month=2, day=1, hour=0, minute=0, second=0),
        ),
    ],
    ids=[
        "Diario - meio do mes",
        "Diario - inicio do mes",
        "Diario - final do mes",
        "Semanal - comeco da semana, segunda-feira",
        "Semanal - comecando na quarta-feira",
        "Semanal - comecando no domingo",
        "Semanal - final da semana",
        "Mensal - meio do mes",
        "Mensal - inicio do ano",
        "Mensal - ultimo dia do mes",
        "Mensal - mes de fevereiro no ano bissexto",
    ],
)
def test_CreatePipelineRun_has_correct_start_time(
    pipeline, current_time, expected_time
):
    """
    tests if the CreatePipelineRun command auxiliar functions
    for correctly generates the start time for the run
    """
    command = CreatePipelineRun(pipeline=pipeline)
    start_time = command.get_pipeline_run_start(current_time=current_time)
    assert start_time == expected_time


@pytest.mark.asyncio
async def test_CreatePipelineRun_returns_correct_object():
    """
    tests if the CreatePipelineRun command returns a PipelineRun Object
    correctly
    """
    steps = [
        {
            "id": 100,
            "name": "Stage",
            "order": 2,
            "scheduling": '{"stepSchedule":{"executeImmediately":false,"frequency":"weekly","startDateTime":"2024-02-28T20:02:00","intervalDays":null,"intervalWeeks":"3","weekDays":["2","4","5"],"months":[],"days":[]}}',
            "description": "Etapa 2 da pipeline.",
            "enabled": True,
            "workflow": {"id": 79, "name": "Histograma", "type": "WORKFLOW"},
        },
        {
            "id": 102,
            "name": "Dataset",
            "order": 3,
            "scheduling": '{"stepSchedule":{"executeImmediately":false,"frequency":"once","startDateTime":"2024-03-07T18:25:00","intervalDays":null,"intervalWeeks":null,"weekDays":[],"months":[],"days":[]}}',
            "description": "Etapa 3 da pipeline.",
            "enabled": True,
            "workflow": {"id": 709, "name": "Novo workflow", "type": "WORKFLOW"},
        },
    ]
    pipeline = {
        "id": 34,
        "name": "Pipeline Anac",
        "description": "Descri\u00e7\u00e3o Pipeline Anac.",
        "enabled": True,
        "user_id": 1,
        "user_login": "waltersf@gmail.com",
        "user_name": "Administrador Lemonade",
        "created": "2024-01-23T13:25:05",
        "updated": "2024-05-07T18:40:09",
        "version": 163,
        "steps": steps,
        "execution_window": "monthly",
    }
    command = CreatePipelineRun(pipeline=pipeline)
    config = load_config()
    pipeline_run = await command.execute(config)
    # TODO: Create assert logic
