import pytest
from pprint import pprint
from unittest.mock import patch
import datetime
from datetime import date

from taar_monitor import stmolog


def mocked_query_redash(*args, **kwargs):
    return [
        {
            "TIMESTAMP": "2019-06-16T00:00:32.558",
            "msg": """client_id: [1], ensemble_weight: [{'locale': 1, 'similarity': 2, 'collaborative': 3}], guids: ['guid1', 'guid2', 'guid3', 'guid4']""",
        },
        {
            "TIMESTAMP": "2019-06-17T00:00:32.558",
            "msg": """client_id: [2], ensemble_weight: [{'locale': 1.1, 'similarity': 2, 'collaborative': 3}], guids: ['guid1a', 'guid2', 'guid3', 'guid4']""",
        },
        {
            "TIMESTAMP": "2019-06-18T00:00:32.558",
            "msg": """client_id: [3], ensemble_weight: [{'locale': 1.2, 'similarity': 2, 'collaborative': 3}], guids: ['guid1c', 'guid2', 'guid3', 'guid4']""",
        },
    ]


EXPECTED = [
    ("1", "guid1", datetime.datetime(2019, 6, 16, 0, 0, 32, 558000)),
    ("1", "guid2", datetime.datetime(2019, 6, 16, 0, 0, 32, 558000)),
    ("1", "guid3", datetime.datetime(2019, 6, 16, 0, 0, 32, 558000)),
    ("1", "guid4", datetime.datetime(2019, 6, 16, 0, 0, 32, 558000)),
    ("2", "guid1a", datetime.datetime(2019, 6, 17, 0, 0, 32, 558000)),
    ("2", "guid2", datetime.datetime(2019, 6, 17, 0, 0, 32, 558000)),
    ("2", "guid3", datetime.datetime(2019, 6, 17, 0, 0, 32, 558000)),
    ("2", "guid4", datetime.datetime(2019, 6, 17, 0, 0, 32, 558000)),
    ("3", "guid1c", datetime.datetime(2019, 6, 18, 0, 0, 32, 558000)),
    ("3", "guid2", datetime.datetime(2019, 6, 18, 0, 0, 32, 558000)),
    ("3", "guid3", datetime.datetime(2019, 6, 18, 0, 0, 32, 558000)),
    ("3", "guid4", datetime.datetime(2019, 6, 18, 0, 0, 32, 558000)),
]


def test_stmolog():
    ed = stmolog.EnsembleSuggestionData()
    with patch.object(ed, "_query_redash", new=mocked_query_redash):
        data = ed.get_raw_data(date(2019, 6, 11))
        assert list(data) == EXPECTED
