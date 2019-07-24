import time
from datadog import api, initialize
from decouple import config
from pyspark.sql.types import LongType, StructField, StructType, FloatType


DATADOG_API_KEY = config("DATADOG_API_KEY")
DATADOG_APP_KEY = config("DATADOG_APP_KEY")


def parse_ts(ts):
    return int(ts / 1000)


class DatadogQueryType:
    QUERY_VALUE = "query_value"
    TIME_SERIES = "timeseries"


class DataDogSource:
    OPTIONS = {"api_key": DATADOG_API_KEY, "app_key": DATADOG_APP_KEY}

    def __init__(self, spark):
        initialize(**self.OPTIONS)
        self._spark = spark

    def get_total_http200_served(self, minutes=24 * 60):
        """
        :return: count of HTTP200 requests served within a window
        """
        cmd = "sum"
        metric = "aws.elb.httpcode_backend_2xx"
        tags = "{app:data,stack:taar,env:prod}"
        data = self._process_query(
            cmd,
            metric,
            tags,
            minutes,
            query=DatadogQueryType.QUERY_VALUE,
            as_count=True,
        )
        if data["status"] == "ok":
            return sum([scalar for (ts, scalar) in data["series"][0]["pointlist"]])
        return 0

    def get_dynamodb_read_latency(self, minutes=20):
        """
        Return a list of 2-tuples of (timestamps, latency in ms)
        """
        cmd = "max"
        metric = "aws.dynamodb.successful_request_latency"
        tags = "{app:data,env:prod,stack:taar}"
        data = self._process_query(cmd, metric, tags, minutes)
        result = []
        if data["status"] == "ok":
            for (ts, scalar) in data["series"][0]["pointlist"]:
                result.append((parse_ts(ts), scalar))

        cSchema = StructType(
            [StructField("timestamp", LongType()), StructField("latency", FloatType())]
        )

        df = self._spark.createDataFrame(result, schema=cSchema)
        return df

    def get_dashboard(self, dash_id):
        return api.Dashboard.get(dash_id)

    def _process_query(
        self, cmd, metric, tags, minutes, query=DatadogQueryType.TIME_SERIES, **kwargs
    ):
        now = time.time()
        query = "{}:{}{}".format(cmd, metric, tags)

        if kwargs.get("as_count", False):
            query += ".as_count()"

        return api.Metric.query(start=now - 60 * minutes, end=now, query=query)
