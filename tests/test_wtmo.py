import pytest
from unittest.mock import patch
from datetime import datetime

from taar_monitor import wtmo


def mocked__get_runtime(*args, **kwargs):
    return [
        {
            "dag_id": "taar_amodump",
            "duration": 1216.64,
            "start_date": "2019-06-16T00:00:32.558",
            "state": "success",
            "task_id": "taar_amodump",
        },
        {
            "dag_id": "taar_amodump",
            "duration": 1220.64,
            "start_date": "2019-06-17T00:00:32.558",
            "state": "success",
            "task_id": "taar_amodump",
        },
        {
            "dag_id": "taar_amodump",
            "duration": 1230.64,
            "start_date": "2019-06-18T00:00:32.558",
            "state": "success",
            "task_id": "taar_amodump",
        },
    ]


def test_workflowtaskinfo():
    """
    Test that the workflow data extraction class works properly

    We need to mock out the WorkflowTaskInfo::_get_runtime 
    method to return a mock set of data that can be transformed back
    by WorkflowTaskInfo::get_etl_durations
    """

    job = ("taar_weekly", "taar_ensemble")

    wtmo_data = wtmo.WorkflowTaskInfo()
    with patch.object(wtmo_data, "_get_runtime", new=mocked__get_runtime):
        dag_id, task_id = job
        runtime_meta = wtmo_data.get_etl_durations(
            dag_id=dag_id,
            task_id=task_id,
            batch_size=7,
            extra_where=" and start_date >= '2019-06-10'",
        )

        assert len(runtime_meta) == 3
        assert runtime_meta == [
            (datetime(2019, 6, 16, 0, 0, 32, 558000), 1216.64),
            (datetime(2019, 6, 17, 0, 0, 32, 558000), 1220.64),
            (datetime(2019, 6, 18, 0, 0, 32, 558000), 1230.64),
        ]
