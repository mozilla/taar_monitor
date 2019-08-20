from pyspark.sql.types import LongType, StringType, StructField, StructType
import ast
import dateutil.parser
import re
import time
from .redash_base import AbstractData
import s3fs
import csv


def parse_ts(ts):
    return int(ts / 1000)


def date_to_ts(dt):
    return int((time.mktime(dateutil.parser.parse(dt).timetuple())))


class LocaleSuggestionData(AbstractData):
    QUERY_ID = 63422

    def __init__(self, spark, s3_bucket, s3_path, aws_key, aws_secret):
        super().__init__(spark)

        self._s3_bucket = s3_bucket
        self._s3_path = s3_path
        self._fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)

        self._spark_schema = StructType(
            [
                StructField("locale", StringType()),
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
        locale_re = re.compile(r"client_locale: \[([^]]*)\]")
        guids_re = re.compile(r"guids *: *\[(\[[^]]*\])")

        for chunk_id, chunk in enumerate(self._query_redash(tbl_date)):
            chunk_rows = []
            for row in chunk:
                ts = date_to_ts(row["TIMESTAMP"])
                payload = row["msg"]
                guids_json = guids_re.findall(payload)[0]

                # Note that the AMO server incorrectly queries the TAAR
                # server for 10 addons instead of the spec'd 4 so we need
                # to manually truncate that list
                try:
                    guids = ast.literal_eval(guids_json)
                except Exception as e:
                    print("Error parsing GUIDS out of : {} {}".format(e, guids_json))
                    continue

                client_locale = locale_re.findall(payload)[0]
                chunk_rows.extend([(client_locale, guid, ts) for guid in guids])

            # Write out this chunk of rows to S3
            iso_strdate = tbl_date.strftime("%Y%m%d")

            filename = "{}.csv.part{:05d}".format(iso_strdate, chunk_id)
            s3_dirpath = self._s3_path + "/" + iso_strdate + "/parts"
            s3_fname = "{}/{}/{}".format(self._s3_bucket, s3_dirpath, filename)
            print("Writing to {}".format(s3_fname))
            with self._fs.open(s3_fname, "w") as fout:
                writer = csv.writer(fout)
                writer.writerows(chunk_rows)
