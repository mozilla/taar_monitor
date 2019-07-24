"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType
import dateutil.parser
import json
import re
import requests

from .redash_base import AbstractData


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


class CollaborativeSuggestionData(AbstractData):
    QUERY_ID = 63284

    def __init__(self, spark):
        super().__init__(spark)

    def get_suggestion_df(self, tbl_date):
        row_iter = self._get_raw_data(tbl_date)

        cSchema = StructType(
            [
                StructField("client", StringType()),
                StructField("guid", StringType()),
                StructField("timestamp", LongType()),
            ]
        )

        df = self._spark.createDataFrame(row_iter, schema=cSchema)
        return df

    def _get_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        guids_re = re.compile(r"guids *: *(\[[^]]*\])")
        client_re = re.compile(r"client_id *: *\[([^]]*)\]")

        results = self._query_redash(tbl_date)

        for row in results:
            ts = int(dateutil.parser.parse(row["TIMESTAMP"]).timestamp())
            payload = row["msg"]
            guids = json.loads(guids_re.findall(payload)[0].replace("'", '"'))
            client_id = client_re.findall(payload)[0]
            for guid in guids:
                parsed_data = (client_id, guid, ts)
                yield parsed_data
