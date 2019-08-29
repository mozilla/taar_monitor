import requests
import time
from decouple import config
from pyspark.sql.types import LongType, StructField, StructType, FloatType
import s3fs
import dateutil.parser

from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame

import csv


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)


class WorkflowTaskInfo:
    QUERY_ID = 63309

    def __init__(
        self, spark, s3_bucket="srg-team-bucket", s3_path="taar-metrics/wtmo-jobs"
    ):
        self._spark = spark
        self.API_KEY = config("STMO_API_KEY", "")

        self._s3_bucket = s3_bucket
        self._s3_path = s3_path

        self._s3_root = "{}/{}".format(self._s3_bucket, self._s3_path)

        self._fs = s3fs.S3FileSystem()

        self._wtmo_schema = StructType(
            [StructField("timestamp", LongType()), StructField("duration", FloatType())]
        )

    def _poll_job(self, s, redash_url, job):
        while job["status"] not in (3, 4):
            response = s.get("{}/api/jobs/{}".format(redash_url, job["id"]))
            job = response.json()["job"]
            time.sleep(1)

        if job["status"] == 3:
            return job["query_result_id"]

        return None

    def _get_fresh_query_result(self, redash_url, query_id, api_key, params):
        s = requests.Session()
        s.headers.update({"Authorization": "Key {}".format(api_key)})

        url = "{}/api/queries/{}/refresh".format(redash_url, query_id)
        # print("Hitting url: [{}]".format(url))
        response = s.post(url, params=params)

        if response.status_code != 200:
            # print(response.text)
            # print(response.status_code)
            raise Exception("Refresh failed.")

        result_id = self._poll_job(s, redash_url, response.json()["job"])

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

    def build_params(self, **param_dict):
        tmp = dict([("p_{}".format(k), v) for k, v in param_dict.items()])
        # print("Got params: {}".format(tmp))
        return tmp

    def _get_runtime(self, **kwargs):
        """
        Fetch ETL runtime information, specifically duration of the
        ETL job.

        :param task_id: the name of the task to search for
        :param batch_size: integer size of the resultset, defaults to 30
        :param extra_where: Extra where clause to further restrict the resultset
        :return: a list of dictionaries with duration specified in
        seconds for each run of the ETL job

        {'dag_id': 'taar_amodump',
         'duration': 1216.64,
         'start_date': '2019-06-16T00:00:32.558',
         'state': 'success',
         'task_id': 'taar_amodump'},
        """
        dag_id = kwargs["dag_id"]
        task_id = kwargs["task_id"]
        batch_size = kwargs.get("batch_size", 30)
        extra_where = kwargs.get("extra_where", "")

        # Need to use a *user API key* here (and not a query API key).
        params = self.build_params(
            task_id=task_id,
            batch_size=batch_size,
            extra_where=extra_where,
            dag_id=dag_id,
        )
        data = self._get_fresh_query_result(
            "https://sql.telemetry.mozilla.org", self.QUERY_ID, self.API_KEY, params
        )
        return data

    def get_etl_durations(
        self, dag_id, task_id, batch_size, extra_where="", force_refresh=False
    ):

        if force_refresh:
            self.write_etl_durations(dag_id, task_id, batch_size, extra_where)

        return self._get_cached_durations_df(dag_id, task_id)

    def _get_cached_durations_df(self, dag_id, task_id):

        filename = "{}_{}.csv".format(dag_id, task_id)
        s3_human_path = "s3a://{}/{}".format(self._s3_root, filename)

        print("Reading {}".format(s3_human_path))
        try:
            return self._spark.read.csv(s3_human_path, schema=self._wtmo_schema)
        except Exception:
            # If any data is missing, just continue
            pass
            return None

    def write_etl_durations(self, dag_id, task_id, batch_size, extra_where=""):
        """
        Fetch a list of durations in seconds for a particular job.

        :return: A list of 2-tuples of of (datetime, duration in seconds) in chronological order
        """

        """
        This should save the results for each execution record into a
        date stamped CSV file in S3 so that we can restore data
        directly from S3.
        """
        data = self._get_runtime(
            dag_id=dag_id,
            task_id=task_id,
            batch_size=batch_size,
            extra_where=extra_where,
        )
        tuples = []
        for r in data:
            if r["state"] == "success":
                tuples.append((r["start_date"], r["duration"]))
            else:
                tuples.append((r["start_date"], 0))

        parsed_tuples = [
            (int(dateutil.parser.parse(r[0]).timestamp()), float(r[1]))
            for r in tuples
            if (r[0] is not None and r[1] is not None)
        ]

        filename = "{}_{}.csv".format(dag_id, task_id)
        s3_fname = "{}/{}".format(self._s3_root, filename)

        # Write out this chunk of rows for one day to S3 merging
        # with any existing data
        try:
            if self._fs.exists(s3_fname):
                with self._fs.open(s3_fname, "r") as file_in:
                    reader = csv.reader(file_in)
                    parsed_tuples = list(set(reader.readrows()) + set(parsed_tuples))
                    print("Refreshed tuples in {}".format(s3_fname))
        except Exception:
            pass

        with self._fs.open(s3_fname, "w") as fout:
            writer = csv.writer(fout)
            writer.writerows(parsed_tuples)
            print("Wrote rows: {}".format(len(parsed_tuples)))
        print("Wrote out {}".format(s3_fname))
