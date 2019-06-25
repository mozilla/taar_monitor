"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import os
import requests
import time
from pyspark.sql.types import *

import dateutil.parser
import math

import json
import re
from datetime import date, datetime
from decouple import config
from pprint import pprint


STMO_API_KEY = config("STMO_API_KEY")


def build_params(**param_dict):
    tmp = dict([("p_{}".format(k), v) for k, v in param_dict.items()])
    return tmp


def get_addon_default_name(guid):
    uri = "https://addons.mozilla.org/api/v3/addons/search/?app=firefox&sort=created&type=extension&guid={}"
    detail_uri = uri.format(guid.encode("utf8"))
    req = requests.get(detail_uri)

    try:
        jdata = json.loads(req.content)
        blob = jdata["results"][0]
        locale = blob["default_locale"]
        return blob["name"][locale]
    except:
        return guid


class AbstractData:
    def __init__(self, spark):
        self._spark = spark

    def poll_job(self, s, redash_url, job):
        while job["status"] not in (3, 4):
            uri = "{}/api/jobs/{}".format(redash_url, job["id"])
            response = s.get(uri)
            job = response.json()["job"]
            time.sleep(1)

        if job["status"] == 3:
            return job["query_result_id"]

        return None

    def get_fresh_query_result(self, redash_url, query_id, api_key, params):
        s = requests.Session()
        s.headers.update({"Authorization": "Key {}".format(api_key)})

        url = "{}/api/queries/{}/refresh".format(redash_url, query_id)
        response = s.post(url, params=params)

        if response.status_code != 200:
            raise Exception("Refresh failed.")

        result_id = self.poll_job(s, redash_url, response.json()["job"])

        if result_id:
            response = s.get(
                "{}/api/queries/{}/results/{}.json".format(
                    redash_url, query_id, result_id
                )
            )
            if response.status_code != 200:
                raise Exception("Failed getting results.")
        else:
            raise Exception("Query execution failed.")

        return response.json()["query_result"]["data"]["rows"]

    def _query_redash(self, tbl_date):
        iso_date = tbl_date.strftime("%Y%m%d")
        tbl = "taar_prod_logs.docker_taar_api_{}".format(iso_date)
        ts_list = [
            """ AND timestamp >= '{} 00:00:00' AND timestamp < '{} 06:00:00'""",
            """ AND timestamp >= '{} 06:00:00' AND timestamp < '{} 12:00:00'""",
            """ AND timestamp >= '{} 12:00:00' AND timestamp < '{} 18:00:00'""",
            """ AND timestamp >= '{} 18:00:00' AND timestamp <= '{} 23:59:59'""",
        ]

        data = []
        for ts in ts_list[:1]:
            iso_date_hyphens = tbl_date.strftime("%Y-%m-%d")
            params = build_params(
                table_name=tbl, time_slice=ts.format(iso_date_hyphens, iso_date_hyphens)
            )

            # Need to use a *user API key* here (and not a query API key).
            tmp_data = self.get_fresh_query_result(
                "https://sql.telemetry.mozilla.org", self.QUERY_ID, STMO_API_KEY, params
            )
            data.extend(tmp_data)
        return data


class EnsembleSuggestionData(AbstractData):
    QUERY_ID = 63202

    def __init__(self, spark):
        super().__init__(spark)

    def get_suggestion_df(self, tbl_date):
        row_iter = self._get_raw_data(tbl_date)

        cSchema = StructType([
            StructField("client", StringType()),
            StructField("guid", StringType()),
            StructField("timestamp", LongType())])

        df = self._spark.createDataFrame(row_iter,schema=cSchema)
        return df

    def _get_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        weights_re = re.compile(r"ensemble_weight: .([^}]*})")
        guids_re = re.compile(r"guids *: *(\[[^]]*\])")
        client_re = re.compile(r"client_id *: *\[([^]]*)\]")

        results = self._query_redash(tbl_date)

        for row in results:
            ts = int(dateutil.parser.parse(row["TIMESTAMP"]).timestamp())
            payload = row["msg"]
            weights = json.loads(weights_re.findall(payload)[0].replace("'", '"'))
            guids = json.loads(guids_re.findall(payload)[0].replace("'", '"'))
            client_id = client_re.findall(payload)[0]
            for guid in guids:
                parsed_data = (client_id, guid, ts)
                yield parsed_data
