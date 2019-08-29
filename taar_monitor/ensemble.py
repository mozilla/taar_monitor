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
from pyspark.sql.types import (
    StructType,
    StringType,
    BooleanType,
    LongType,
    StructField,
    IntegerType,
)
import pyspark.sql.functions as F

from .redash_base import AbstractData


DAILY_SUGGESTIONS_PATH = "taar-metrics/ensemble/daily_suggestions/{}.csv"
WEEKLY_ROLLUP_PATH = "taar-metrics/ensemble/weekly_suggestions/{}.csv"


MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)


def datestr(date):
    return date.strftime("%Y%m%d")


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

    def get_suggestion_df(self, tbl_date):
        cached_results = self._get_cached_df(tbl_date)
        if cached_results is None:
            self._write_raw_ensemble_data(tbl_date)
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

    def compute_weekly_suggestion_rollup(self, sunday_start_date):
        """
        Fetch week suggestions starting with sunday_start_date.

        Reads 7 CSV files from DAILY_SUGGESTIONS_PATH directory and
        reading in 7 files with YYYYMMDD.csv extension.
        """
        schema_suggestions = StructType(
            [
                StructField("client", StringType(), True),
                StructField("guid", StringType(), True),
                StructField("s3_date", StringType(), True),
            ]
        )

        week_data = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(), schema_suggestions
        )

        for i in range(7):
            thedate_str = datestr(sunday_start_date + timedelta(days=i))
            READ_LOC = "s3://{}/{}".format(
                self._s3_bucket, DAILY_SUGGESTIONS_PATH.format(thedate_str)
            )

            week_data = week_data.union(self._spark.read.csv(READ_LOC, header=True))
        return week_data

    def read_daily_suggestion_rollup(self, thedate):
        schema_suggestions = StructType(
            [
                StructField("client", StringType(), True),
                StructField("guid", StringType(), True),
                StructField("s3_date", StringType(), True),
            ]
        )
        LOC = "s3://{}/{}".format(
            self._s3_bucket, DAILY_SUGGESTIONS_PATH.format(thedate.strftime("%Y%m%d"))
        )
        return self._.read.csv(LOC, schema=schema_suggestions)

    def write_daily_suggestion_rollup(self, thedate):
        """
        Compute daily suggestions
        """
        YESTERDAY = date.today() - timedelta(days=1)

        # daily suggestions
        d = thedate
        while d < YESTERDAY:
            suggestions = self.get_suggestion_df(d)
            OUTPUT_LOC = "s3://{}/{}".format(
                self._s3_bucket, DAILY_SUGGESTIONS_PATH.format(datestr(d))
            )

            backfill = (
                suggestions.where(F.col("top_4") == True)  # noqa
                .drop("top_4")
                .withColumn("s3_date", F.lit(datestr(d)))
                .select("client", "guid", "s3_date")
                .distinct()
            )

            backfill.write.format("com.databricks.spark.csv").options(
                inferschema="true"
            ).option("header", "true").save(OUTPUT_LOC, mode="overwrite")

            d += timedelta(days=1)

    def read_weekly_suggestion_rollup(self, thedate):
        weekly_suggestion_schema = StructType(
            [
                StructField("guid", StringType(), True),
                StructField("unique_users_recd", StringType(), True),
                StructField("times_recd", IntegerType(), True),
                StructField("week_start", IntegerType(), True),
            ]
        )

        LOC = "s3://{}/{}".format(self._s3_bucket, WEEKLY_ROLLUP_PATH.format(thedate))
        return self._spark.read.csv(LOC, schema=weekly_suggestion_schema)

    def write_weekly_suggestion_rollup(self, thedate):
        """
        Compute a weekly rollup of suggestions starting on a Sunday
        """
        YESTERDAY = date.today() - timedelta(days=1)
        OUTPUT_LOC = "s3://{}/{}".format(
            self._s3_bucket, WEEKLY_ROLLUP_PATH.format(thedate)
        )

        if thedate.weekday() != SUNDAY:
            # Only run the week suggestion rollup on Sunday
            return

        if (thedate + timedelta(days=6)) >= YESTERDAY:
            # Not enough days to make a full week of weekly
            # suggestions
            return

        # for weekly rollups
        week_rollup = self.compute_weekly_suggestion_rollup(thedate)

        week_rollup = (
            week_rollup.groupBy("guid")
            .agg(
                F.countDistinct("client").alias("unique_users_recd"),
                F.count("client").alias("times_recd"),
            )
            .withColumn("week_start", F.lit(datestr(thedate)))
        )

        week_rollup.write.format("com.databricks.spark.csv").options(
            inferschema="true"
        ).option("header", "true").save(OUTPUT_LOC, mode="overwrite")

    def _write_raw_ensemble_data(self, tbl_date):
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

        # Compute rollups for the day
        self.write_all_rollups(tbl_date)

    def write_all_rollups(self, tbl_date):
        self.write_daily_suggestion_rollup(tbl_date)
        self.write_weekly_suggestion_rollup(tbl_date)
