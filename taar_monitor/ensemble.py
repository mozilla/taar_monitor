"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import dateutil.parser
import json
import ast
import os
import re
import requests
import boto3
import botocore
from io import StringIO
import csv

from pyspark.sql.types import StructType, StringType, BooleanType, LongType, StructField

from taar_monitor.utils import safe_createDataFrame
from .redash_base import AbstractData


def _store_to_s3(data, bucket, path, filename):
    s3_path = s3_normpath(path, filename)

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).put_object(Key=s3_path, Body=data)


def s3_normpath(path, filename):
    # Normalize the path s3_path = os.path.normpath(os.path.join(path, filename))
    s3_path = os.path.join(path, filename)
    while s3_path.startswith("/"):
        s3_path = s3_path[1:]
    return s3_path


def s3_file_exists(bucket, path, filename):
    s3 = boto3.resource("s3")
    s3_path = s3_normpath(path, filename)

    s3_human_path = "s3://{}/{}".format(bucket, s3_path)
    try:
        s3.Object(bucket, s3_path).load()
        print("{} already exists".format(s3_human_path))
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            # The object does not exist.
            print("Err[{}] {} does not exists".format(code, s3_human_path))
            return False

        # Re-raise the exception as something terrible has happened in
        # AWS
        raise


def get_addon_default_name(guid):
    uri = "https://addons.mozilla.org/api/v3/addons/search/?app=firefox&sort=created&type=extension&guid={}"
    detail_uri = uri.format(guid.encode("utf8"))
    req = requests.get(detail_uri)

    try:
        jdata = json.loads(req.content)
        blob = jdata["results"][0]
        locale = blob["default_locale"]
        return blob["name"][locale]
    except Exception:
        return guid


class EnsembleSuggestionData(AbstractData):
    QUERY_ID = 63202

    def __init__(self, spark, s3_bucket, s3_path):
        super().__init__(spark)

        self._s3_bucket = s3_bucket
        self._s3_path = s3_path

    def get_suggestions(self, tbl_date):
        return list(self._get_raw_data(tbl_date))

    def get_suggestion_df(self, tbl_date):
        cached_results = self._get_cached_df(tbl_date)
        if cached_results is not None:
            return cached_results

        row_iter = self._get_raw_data(tbl_date)

        spark_schema = StructType(
            [
                StructField("client", StringType()),
                StructField("guid", StringType()),
                StructField("top_4", BooleanType()),
                StructField("timestamp", LongType()),
            ]
        )

        df = safe_createDataFrame(self._spark, list(row_iter), schema=spark_schema)

        return df

    def write_s3(self, data_date):
        """
        Write out data to S3
        """
        filename = data_date.strftime("%Y%m%d.csv")
        if not s3_file_exists(self._s3_bucket, self._s3_path, filename):
            rows = self.get_suggestions(data_date)
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows(rows)
            fout.seek(0)
            data = fout.getvalue().encode("utf8")
            _store_to_s3(data, self._s3_bucket, self._s3_path, filename)

    def _get_cached_df(self, data_date):
        """
        Fetch a date of data as a dataframe
        """
        filename = data_date.strftime("%Y%m%d.csv")
        if not s3_file_exists(self._s3_bucket, self._s3_path, filename):
            return None

        s3_path = s3_normpath(self._s3_path, filename)
        s3_human_path = "s3a://{}/{}".format(self._s3_bucket, s3_path)
        spark_schema = StructType(
            [
                StructField("client", StringType()),
                StructField("guid", StringType()),
                StructField("top_4", BooleanType()),
                StructField("timestamp", LongType()),
            ]
        )

        return self._spark.read.csv(s3_human_path, schema=spark_schema)

    def _get_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        guids_re = re.compile(r"guids *: *\[(\[[^]]*\])")
        client_re = re.compile(r"client_id *: *\[([^]]*)\]")

        results = self._query_redash(tbl_date)

        for row in results:
            ts = int(dateutil.parser.parse(row["TIMESTAMP"]).timestamp())
            payload = row["msg"]
            guids_json = guids_re.findall(payload)[0]
            # Note that the AMO server incorrectly queries the TAAR server for 10 addons instead of the spec'd 4
            # so we need to manually truncate that list
            try:
                guids = ast.literal_eval(guids_json)
            except Exception:
                print("Error parsing GUIDS out of : {}".format(guids_json))
                continue

            client_id = client_re.findall(payload)[0]
            for guid_rank, guid in enumerate(guids):
                parsed_data = (client_id, guid, guid_rank < 4, ts)
                yield parsed_data
