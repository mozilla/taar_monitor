"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import dateutil.parser
import csv
import ast
import re
from pyspark.sql.types import LongType, StringType, StructField, StructType

from .redash_base import AbstractData


class CollaborativeSuggestionData(AbstractData):
    QUERY_ID = 63284

    def __init__(self, spark, s3_bucket, s3_path):
        super().__init__(spark)

        self._schema = StructType(
            [
                StructField("client", StringType()),
                StructField("guid", StringType()),
                StructField("timestamp", LongType()),
            ]
        )

    def get_suggestion_df(self, tbl_date):
        cached_results = self._get_cached_df(tbl_date)
        if cached_results is None:
            self._write_raw_data(tbl_date)
            cached_results = self._get_cached_df(tbl_date)

        return cached_results

    def _wriite_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        guids_re = re.compile(r"guids *: *\[(\[[^]]*\])")
        client_re = re.compile(r"client_id *: *\[([^]]*)\]")

        for chunk_id, chunk in self._query_redash(tbl_date):
            chunk_rows = []
            for row in chunk:
                ts = int(dateutil.parser.parse(row["TIMESTAMP"]).timestamp())
                payload = row["msg"]
                guids_json = guids_re.findall(payload)[0]
                try:
                    guids = ast.literal_eval(guids_json)
                except Exception:
                    print("Error parsing GUIDS out of : {}".format(guids_json))
                    continue

                client_id = client_re.findall(payload)[0]
                for guid in guids:
                    parsed_data = (client_id, guid, ts)
                    chunk_rows.extend(parsed_data)

            # Write out this chunk of rows to S3
            iso_strdate = tbl_date.strftime("%Y%m%d")

            filename = "{}.csv.part{:05d}".format(iso_strdate, chunk_id)
            s3_dirpath = self._s3_path + "/" + iso_strdate + "/parts"
            s3_fname = "{}/{}/{}".format(self._s3_bucket, s3_dirpath, filename)
            print("Writing to {}".format(s3_fname))
            with self._fs.open(s3_fname, "w") as fout:
                writer = csv.writer(fout)
                writer.writerows(chunk_rows)
