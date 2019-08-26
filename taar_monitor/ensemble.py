"""
This module provides access to the logs that are uplifted into
sql.telemetry.mozilla.org from the TAAR production logs
"""

import dateutil.parser
import ast
import re
import csv
import s3fs

from datetime import date, timedelta
from pyspark.sql.types import StructType, StringType, BooleanType, LongType, StructField
from pyspark.sql.functions import col, lit
from .redash_base import AbstractData
from .utils import format_to_short_isodate


DAILY_SUMMARY_PATH = "taar-metrics/ensemble/daily_suggestions"
WEEKLY_SUMMARY_PATH = "taar-metrics/ensemble/weekly_suggestions"


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
                chunk_rows.extend(
                    [
                        (client_id, guid, guid_rank < 4, ts)
                        for guid_rank, guid in enumerate(guids)
                    ]
                )

            # Write out this chunk of rows to S3
            iso_strdate = tbl_date.strftime("%Y%m%d")

            filename = "{}.csv.part{:05d}".format(iso_strdate, chunk_id)
            s3_dirpath = self._s3_path + "/" + iso_strdate + "/parts"
            s3_fname = "{}/{}/{}".format(self._s3_bucket, s3_dirpath, filename)
            print("Writing to {}".format(s3_fname))
            with self._fs.open(s3_fname, "w") as fout:
                writer = csv.writer(fout)
                writer.writerows(chunk_rows)

    def _get_week_suggestions(self, d_start):
        schema_suggestions = StructType(
            [
                StructField("client", StringType(), True),
                StructField("guid", StringType(), True),
                StructField("s3_date", StringType(), True),
            ]
        )

        week_data = self._spark.createDataFrame(
            self._spark.emptyRDD(), schema_suggestions
        )
        for i in range(7):
            READ_LOC = "taar_suggestions/{}.csv".format(
                format_to_short_isodate(d_start + timedelta(i))
            )
            week_data = week_data.union(self._spark.read.csv(READ_LOC, header=True))
        return week_data

    def _write_daily_summary(self, thedate):
        OUTPUT_LOC = "s3://{}/{}/{}.csv".format(
            self._s3_bucket, DAILY_SUMMARY_PATH, format_to_short_isodate(thedate)
        )

        suggestions_by_date_df = (
            self.get_suggestion_df(thedate)
            .where(col("top_4") == True)
            .drop("top_4")
            .withColumn("s3_date", lit(format_to_short_isodate(thedate)))
            .select("client", "guid", "s3_date")
            .distinct()
        )

        suggestions_by_date_df.write.format("com.databricks.spark.csv").options(
            inferschema="true"
        ).option("header", "true").save(OUTPUT_LOC, mode="overwrite")

    def weekly_ensemble_rollup(self, thedate):

        # The weekly rollup can only be done on a day prior to today
        assert (date.today() - thedate).days >= 1

        self._write_daily_summary(thedate)

        # add weekly rollup on Sunday
        if thedate.weekday() == 6:
            tmp_date = thedate - timedelta(days=7)
            week_rollup = (
                self._get_week_suggestions(tmp_date)
                .groupBy("guid")
                .agg(
                    F.countDistinct("client").alias("unique_users_recd"),
                    F.count("client").alias("times_recd"),
                )
                .withColumn("week_start", lit(format_to_short_isodate(tmp_date)))
            )
            week_rollup.write.format("com.databricks.spark.csv").options(
                inferschema="true"
            ).option("header", "true").save("week_rollup.csv", mode="append")
