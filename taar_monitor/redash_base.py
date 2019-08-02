import requests
import time
from decouple import config

STMO_API_KEY = config("STMO_API_KEY")


def build_params(**param_dict):
    tmp = dict([("p_{}".format(k), v) for k, v in param_dict.items()])
    return tmp


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
        print("APIKey: {}".format(api_key))
        print("hitting URL: {}".format(url))
        print("params: {}".format(params))

        response = s.post(url, params=params)

        if response.status_code != 200:
            raise Exception("Refresh failed. Status: {}".format(response.status_code))

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
        """
        This splices up the query to the redash table into 24 hour long slices
        to reduce the chance that we exceed the maximum row count in a resultset
        """
        iso_date = tbl_date.strftime("%Y%m%d")
        tbl = "taar_prod_logs.docker_taar_api_{}".format(iso_date)
        data = []

        ts_list = [
            """ AND timestamp >= '{} {:01d}:00:00' AND timestamp < '{} {:01d}:29:59'""",
            """ AND timestamp >= '{} {:01d}:30:00' AND timestamp < '{} {:01d}:59:59'""",
        ]
        for splice_hour in range(24):
            iso_date_hyphens = tbl_date.strftime("%Y-%m-%d")
            for ts in ts_list:
                params = build_params(
                    table_name=tbl,
                    time_slice=ts.format(
                        iso_date_hyphens, splice_hour, iso_date_hyphens, splice_hour
                    ),
                )
                # Need to use a *user API key* here (and not a query API key).
                tmp_data = self.get_fresh_query_result(
                    "https://sql.telemetry.mozilla.org",
                    self.QUERY_ID,
                    STMO_API_KEY,
                    params,
                )
                data.extend(tmp_data)
        return data
