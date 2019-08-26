from datadog import api, initialize
from datetime import date, datetime
from datetime import timedelta

from decouple import config

from pyspark.sql.types import LongType, StructField, StructType, FloatType

import csv
import dateutil
import time
import s3fs

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


def msts_to_sects(ts):
    """
    Parse unix millisecond timestamp into unix second timestamp
    """
    return int(ts / 1000)


def date_to_ts(dt):
    """
    Convert a datetime string into a unix timestamp
    """
    return int((time.mktime(dateutil.parser.parse(dt).timetuple())))


class DatadogQueryType:
    QUERY_VALUE = "query_value"
    TIME_SERIES = "timeseries"


class DataDogSource:
    def __init__(
        self, spark, s3_bucket="srg-team-bucket", s3_path="taar-metrics/datadog"
    ):
        OPTIONS = {
            "api_key": config("DATADOG_API_KEY", ""),
            "app_key": config("DATADOG_APP_KEY", ""),
        }
        initialize(**OPTIONS)
        self._spark = spark

        self._s3_bucket = s3_bucket
        self._s3_path = s3_path

        self._fs = s3fs.S3FileSystem()

        self._dynamo_read_schema = StructType(
            [StructField("timestamp", LongType()), StructField("latency", FloatType())]
        )

        self._http_schema = StructType(
            [StructField("timestamp", LongType()), StructField("latency", FloatType())]
        )

    def get_http200_served_df(self):
        """
        :return: count of HTTP200 requests served within the last 14
        days
        """
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=14)

        cached_results = self._get_cached_http200_df(start_date, end_date)
        if cached_results is None:
            self._write_http200()
            cached_results = self._get_cached_http200_df(start_date, end_date)
        return cached_results

    def _get_cached_http200_df(self, start_date, end_date):
        df_list = []
        sdate = start_date
        while sdate < end_date:
            iso_strdate = sdate.strftime("%Y%m%d")

            filename = iso_strdate + ".csv"
            s3_human_path = "s3a://{}/{}/http200/{}".format(
                self._s3_bucket, self._s3_path, filename
            )

            print("Reading {}".format(s3_human_path))
            try:
                new_df = self._spark.read.csv(s3_human_path, schema=self._http_schema)
                df_list.append(new_df)
                print("Read {}".format(s3_human_path))
            except Exception:
                # If any data is missing, just return None and just
                # recompute the entire day of data
                return None

            sdate = sdate + timedelta(days=1)

        return unionAll(*df_list)

    def _write_http200(self, minutes=15 * 24 * 60):
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

        result = []

        if data["status"] == "ok":
            result = [
                (msts_to_sects(ts), float(scalar))
                for (ts, scalar) in data["series"][0]["pointlist"]
            ]

        hour_ago_cutoff = datetime.now() - timedelta(hours=1)

        # Collect all the records into a dictionary of
        # "y-m-d" -> list of records for the day
        records = {}
        for rec in result:
            parsed_date = datetime.fromtimestamp(rec[0])
            if parsed_date < hour_ago_cutoff:
                rec_isodate = parsed_date.strftime("%Y%m%d")
                records.setdefault(rec_isodate, [])
                records[rec_isodate].append(rec)

        for isodate, new_rows in records.items():
            filename = isodate + ".csv"
            s3_fname = "{}/{}/http200/{}".format(
                self._s3_bucket, self._s3_path, filename
            )

            # Write out this chunk of rows for one day to S3 merging
            # with any existing data
            try:
                if self._fs.exists(s3_fname):
                    with self._fs.open(s3_fname, "r") as file_in:
                        reader = csv.reader(file_in)
                        new_rows = list(set(reader.readrows()) + set(new_rows))
            except Exception:
                pass

            with self._fs.open(s3_fname, "w") as fout:
                writer = csv.writer(fout)
                writer.writerows(new_rows)
            print("Wrote out {}".format(s3_fname))

    def get_dynamodb_read_latency_df(self):

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=14)

        cached_results = self._get_cached_dynamo_df(start_date, end_date)
        if cached_results is None:
            self._write_dynamo_read_latency()
            cached_results = self._get_cached_dynamo_df(start_date, end_date)
        return cached_results

    def _get_cached_dynamo_df(self, start_date, end_date):

        df_list = []
        sdate = start_date
        while sdate < end_date:
            iso_strdate = sdate.strftime("%Y%m%d")

            s3_path = self._s3_path + "/latency/" + iso_strdate + ".csv"
            s3_human_path = "s3a://{}/{}".format(self._s3_bucket, s3_path)

            try:
                new_df = self._spark.read.csv(
                    s3_human_path, schema=self._dynamo_read_schema
                )
                df_list.append(new_df)
                print("Read {}".format(s3_human_path))
            except Exception:
                # If any data is missing, just return None and just
                # recompute the entire day of data
                return None

            sdate = sdate + timedelta(days=1)

        return unionAll(*df_list)

    def _write_dynamo_read_latency(self, minutes=15 * 24 * 60):
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
                result.append((msts_to_sects(ts), scalar))

        hour_ago_cutoff = datetime.now() - timedelta(hours=1)

        # Collect all the records into a dictionary of
        # "y-m-d" -> list of records for the day
        records = {}
        for rec in result:
            parsed_date = datetime.fromtimestamp(rec[0])
            if parsed_date < hour_ago_cutoff:
                rec_isodate = parsed_date.strftime("%Y%m%d")
                records.setdefault(rec_isodate, [])
                records[rec_isodate].append(rec)

        for isodate, new_rows in records.items():
            filename = isodate + ".csv"
            s3_fname = "{}/{}/latency/{}".format(
                self._s3_bucket, self._s3_path, filename
            )

            # Write out this chunk of rows for one day to S3 merging
            # with any existing data
            try:
                if self._fs.exists(s3_fname):
                    with self._fs.open(s3_fname, "r") as file_in:
                        reader = csv.reader(file_in)
                        new_rows = list(set(reader.readrows()) + set(new_rows))
            except Exception:
                pass

            with self._fs.open(s3_fname, "w") as fout:
                writer = csv.writer(fout)
                writer.writerows(new_rows)
            print("Wrote out {}".format(s3_fname))

    def _process_query(
        self, cmd, metric, tags, minutes, query=DatadogQueryType.TIME_SERIES, **kwargs
    ):
        now = time.time()
        query = "{}:{}{}".format(cmd, metric, tags)

        if kwargs.get("as_count", False):
            query += ".as_count()"

        return api.Metric.query(start=now - 60 * minutes, end=now, query=query)
