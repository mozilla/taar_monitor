"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import dateutil.parser
import ast
import re
import csv
import s3fs

from pyspark.sql.types import StructType, StringType, BooleanType, LongType, StructField
from .redash_base import AbstractData


class EnsembleSuggestionData(AbstractData):
    QUERY_ID = 63202

    def __init__(self, spark, s3_bucket, s3_path):
        super().__init__(spark)

        self._s3_bucket = s3_bucket
        self._s3_path = s3_path

        self._fs = s3fs.S3FileSystem()

        self._spark_schema = StructType(
            [
                StructField("client", StringType()),
                StructField("guid", StringType()),
                StructField("top_4", BooleanType()),
                StructField("timestamp", LongType()),
            ]
        )

    def get_suggestions(self, tbl_date):
        return list(self._get_raw_data(tbl_date))

    def get_suggestion_df(self, tbl_date):
        cached_results = self._get_cached_df(tbl_date)
        if cached_results is None:
            self._write_raw_data(tbl_date)
            cached_results = self._get_cached_df(tbl_date)

        return cached_results

    def _get_cached_df(self, data_date):
        """
        Fetch a date of data as a dataframe
        """

        iso_strdate = data_date.strftime("%Y%m%d")

        s3_path = self._s3_path + "/" + iso_strdate + "/parts"
        s3_human_path = "s3a://{}/{}".format(self._s3_bucket, s3_path)

        try:
            return self._spark.read.csv(s3_human_path, schema=self._spark_schema)
        except Exception:
            # No cached dataset is found
            return None

    def _write_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        guids_re = re.compile(r"guids *: *\[(\[[^]]*\])")
        client_re = re.compile(r"client_id *: *\[([^]]*)\]")

        for chunk_id, chunk in enumerate(self._query_redash(tbl_date)):
            chunk_rows = []
            for row in chunk:
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
