import pytest
from pprint import pprint
from unittest.mock import patch
import datetime
from datetime import date, datetime

from taar_monitor import datadog


def test_dynamo_read_latency(spark):
    dd = datadog.DataDogSource(spark)

    EXPECTED = [(1561410798, 1.0), (1561510898, 1.5), (1561610998, 2.0)]

    def mocked_process_query(*args, **kwargs):
        return {
            "status": "ok",
            "series": [
                {
                    "pointlist": [
                        (1561410798.140208 * 1000, 1.0),
                        (1561510898.140208 * 1000, 1.5),
                        (1561610998.140208 * 1000, 2.0),
                    ]
                }
            ],
        }

    with patch.object(dd, "_process_query", new=mocked_process_query):
        results = dd.get_dynamodb_read_latency(24 * 60)
        assert results.collect() == EXPECTED


def test_python_backend_latency(spark):
    dd = datadog.DataDogSource(spark)

    EXPECTED = [(1561410798, 1.0), (1561510898, 1.5), (1561610998, 2.0)]

    def mocked_process_query(*args, **kwargs):
        return {
            "status": "ok",
            "series": [
                {
                    "pointlist": [
                        (1561410798 * 1000, 1.0),
                        (1561510898 * 1000, 1.5),
                        (1561610998 * 1000, 2.0),
                    ]
                }
            ],
        }

    with patch.object(dd, "_process_query", new=mocked_process_query):
        results = dd.get_python_backend_latency(24 * 60)
        assert results.collect() == EXPECTED


def test_total_http200_served(spark):
    dd = datadog.DataDogSource(spark)

    EXPECTED = 342

    def mocked_process_query(*args, **kwargs):
        return {
            "status": "ok",
            "series": [
                {
                    "pointlist": [
                        (1561410798.740208 * 1000, 100),
                        (1561510798.740208 * 1000, 142),
                        (1561610798.740208 * 1000, 90),
                        (1561610798.740208 * 1000, 10),
                    ]
                }
            ],
        }

    with patch.object(dd, "_process_query", new=mocked_process_query):
        assert EXPECTED == dd.get_total_http200_served(minutes=24 * 60)
