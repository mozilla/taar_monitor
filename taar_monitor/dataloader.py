from .utils import check_py3
from .amo_installs import AddonInstallEvents

from .locale import LocaleSuggestionData
from .collab import CollaborativeSuggestionData
from .ensemble import EnsembleSuggestionData


from datetime import date, timedelta
from io import StringIO
import boto3
import botocore
from hashlib import sha256

import json
import csv
import os.path

import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from taar_monitor.utils import safe_createDataFrame


check_py3()


DEFAULT_BUCKET = "srg-team-bucket"

INSTALL_EVENTS_PATH = "taar-metrics/install_events"
LOCALE_PATH = "taar-metrics/locale"
ENSEMBLE_PATH = "taar-metrics/ensemble"
COLLABORATIVE_PATH = "taar-metrics/collaborative"


MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)


def get_whitelist(spark, thedate):
    f = StringIO()
    client = boto3.client("s3")
    client.download_fileobj(
        "telemetry-parquet", "taar/locale/only_guids_top_200{}.json".format(thedate), f
    )
    guids = json.loads(f.getvalue())
    df = safe_createDataFrame(spark, guids, StringType())
    return df


sha256_udf = udf(lambda clientid: sha256(clientid).hexdigest(), StringType())

schema_suggestions = StructType(
    [
        StructField("client", StringType(), True),
        StructField("guid", StringType(), True),
        StructField("s3_date", StringType(), True),
    ]
)


def get_week_suggestions(sqlContext, sparkContext, d_start):
    week_data = sqlContext.createDataFrame(sparkContext.emptyRDD(), schema_suggestions)
    for i in range(7):
        READ_LOC = "taar_suggestions/{}.csv".format(datestr(d_start + timedelta(i)))
        week_data = week_data.union(sqlContext.read.csv(READ_LOC, header=True))
    return week_data


def make_df_copy(df):
    return df.select([c for c in df.columns])


def datestr(date):
    return date.strftime("%Y%m%d")


def weekly_ensemble_rollup(spark, sparkContext, sqlContext, week_end_date):
    """
    This was formerly `run_backfill` in the notebook
    Compute a weekly rollup from week_start_date to week_end_date,
    excluding the week_end_date from the dataset.

    The dates must be sunday to sunday covering a 7 day window.
    """

    week_start_date = week_end_date - timedelta(days=7)

    TODAY = date.today()

    # Verify that the start date is a Sunday
    end_dow = week_end_date.weekday()
    if not ((end_dow == SUNDAY) and ((TODAY - week_end_date).days > 7)):
        raise Exception("Invalid date range for weekly enssemble rollup")

    ens = EnsembleSuggestionData(spark, DEFAULT_BUCKET, ENSEMBLE_PATH)
    # daily suggestions
    d = week_start_date
    while d < week_end_date:
        suggestions = ens.get_suggestion_df(d)
        OUTPUT_LOC = "taar_suggestions/{}.csv".format(datestr(d))

        backfill = (
            suggestions.where(col("top_4") == True)  # noqa
            .drop("top_4")
            .withColumn("s3_date", lit(datestr(d)))
            .select("client", "guid", "s3_date")
            .distinct()
        )

        backfill.write.format("com.databricks.spark.csv").options(
            inferschema="true"
        ).option("header", "true").save(OUTPUT_LOC, mode="overwrite")
        d += timedelta(days=1)

    # for weekly rollups
    schema_doc = StructType(
        [
            StructField("guid", StringType(), True),
            StructField("unique_users_recd", StringType(), True),
            StructField("times_recd", IntegerType(), True),
            StructField("week_start", IntegerType(), True),
        ]
    )

    week_rollup = sqlContext.createDataFrame(sparkContext.emptyRDD(), schema_doc)

    d = week_start_date
    while d + timedelta(6) < week_end_date:
        print(d)
        week_suggestions = get_week_suggestions(sqlContext, sparkContext, d)
        week_rollup = week_rollup.union(
            week_suggestions.groupBy("guid")
            .agg(
                F.countDistinct("client").alias("unique_users_recd"),
                F.count("client").alias("times_recd"),
            )
            .withColumn("week_start", lit(datestr(d)))
        )
        d += timedelta(7)

    OUTPUT_LOC = "week_rollup.csv"
    week_rollup.write.format("com.databricks.spark.csv").options(
        inferschema="true"
    ).option("header", "true").save(OUTPUT_LOC, mode="overwrite")


def update_install_events(spark, num_days=30, end_date=None):

    event_gen = AddonInstallEvents(spark)
    if end_date is None:
        today = date.today()
    else:
        today = end_date

    def convert_to_csv(row):
        return (row["submission_date"], row["client_id"], row["value"])

    def locale_to_csv_row(r):
        return (
            r["locale"],
            r["guid"],
            date.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d"),
        )

    for i in range(num_days):
        thedate = today - timedelta(days=(i + 1))
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, INSTALL_EVENTS_PATH, filename):
            rows = event_gen.get_install_events(thedate)
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows([convert_to_csv(r) for r in rows])
            fout.seek(0)
            data = fout.getvalue().encode("utf8")
            _store_to_s3(data, DEFAULT_BUCKET, INSTALL_EVENTS_PATH, filename)


def update_locale(spark, num_days=30):
    def locale_to_csv_row(r):
        return (
            r["locale"],
            r["guid"],
            date.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d"),
        )

    ll = LocaleSuggestionData(spark)
    today = date.today()

    for i in range(num_days):
        thedate = today - timedelta(days=(i + 1))
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, LOCALE_PATH, filename):

            df = ll.get_suggestion_df(thedate)
            rows = df.collect()
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows([locale_to_csv_row(r) for r in rows])
            fout.seek(0)
            data = fout.getvalue().encode("utf8")

            _store_to_s3(data, DEFAULT_BUCKET, LOCALE_PATH, filename)


def update_ensemble_suggestions(spark, num_days=30, end_date=None):
    ensemble_gen = EnsembleSuggestionData(spark)

    if end_date is None:
        today = date.today()
    else:
        today = end_date

    for i in range(num_days):
        thedate = today - timedelta(days=(i + 1))
        ensemble_gen.write_s3(thedate, DEFAULT_BUCKET, ENSEMBLE_PATH)


def update_collaborative_suggestions(spark, num_days=30):
    def locale_to_csv_row(r):
        return (
            r["client_id"],
            r["guid"],
            date.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d"),
        )

    collab_gen = CollaborativeSuggestionData(spark)
    today = date.today()

    for i in range(num_days):
        thedate = today - timedelta(days=(i + 1))
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, COLLABORATIVE_PATH, filename):
            rows = collab_gen.get_suggestion_df(thedate)
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows(rows)
            fout.seek(0)
            data = fout.getvalue().encode("utf8")

            _store_to_s3(data, DEFAULT_BUCKET, COLLABORATIVE_PATH, filename)


def s3_normpath(path, filename):
    # Normalize the path s3_path = os.path.normpath(os.path.join(path, filename))
    s3_path = os.path.join(path, filename)
    while s3_path.startswith("/"):
        s3_path = s3_path[1:]
    return s3_path


def s3_file_exists(bucket, path, filename):
    s3 = boto3.resource("s3")
    s3_path = s3_normpath(path, filename)

    s3_human_path = "s3://{}/{}".format(bucket, s3_path)
    try:
        s3.Object(bucket, s3_path).load()
        print("{} already exists".format(s3_human_path))
        return True
    except botocore.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            # The object does not exist.
            print("Err[{}] {} does not exists".format(code, s3_human_path))
            return False

        # Re-raise the exception as something terrible has happened in
        # AWS
        raise


def _store_to_s3(data, bucket, path, filename):
    s3_path = s3_normpath(path, filename)

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).put_object(Key=s3_path, Body=data)
