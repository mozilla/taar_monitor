"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import dateutil.parser
import json
import ast
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


class EnsembleSuggestionData(AbstractData):
    QUERY_ID = 63202

    def __init__(self, spark):
        super().__init__(spark)

    def get_suggestions(self, tbl_date):
        return list(self._get_raw_data(tbl_date))

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
