from .utils import check_py3
from .amo_installs import AddonInstallEvents

from .locale import LocaleSuggestionData
from .collab import CollaborativeSuggestionData


from datetime import date, timedelta
from io import StringIO
import boto3
import botocore
import csv
import os.path


check_py3()


DEFAULT_BUCKET = "srg-team-bucket"


def update_install_events(spark, num_days=30):
    def convert_to_csv(row):
        yield (row["submission_date"], row["client_id"], row["value"])

    def locale_to_csv_row(r):
        return (
            r["locale"],
            r["guid"],
            date.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d"),
        )

    event_gen = AddonInstallEvents(spark)
    today = date.today()
    path = "taar-metrics/install_events"

    for i in range(num_days):
        thedate = today - timedelta(days=(i + 1))
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, path, filename):
            rows = event_gen.get_install_events(thedate)
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows([convert_to_csv(r) for r in rows])
            fout.seek(0)
            data = fout.getvalue().encode("utf8")
            _store_to_s3(data, DEFAULT_BUCKET, path, filename)


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
        path = "taar-metrics/locale"
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, path, filename):

            df = ll.get_suggestion_df(thedate)
            rows = df.collect()
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows([locale_to_csv_row(r) for r in rows])
            fout.seek(0)
            data = fout.getvalue().encode("utf8")

            _store_to_s3(data, DEFAULT_BUCKET, path, filename)


def update_ensemble_suggestions(spark, num_days=30):
    # TODO:
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
        path = "taar-metrics/ensemble"
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, path, filename):

            df = ll.get_suggestion_df(thedate)
            rows = df.collect()
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows([locale_to_csv_row(r) for r in rows])
            fout.seek(0)
            data = fout.getvalue().encode("utf8")

            _store_to_s3(data, DEFAULT_BUCKET, path, filename)


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
        path = "taar-metrics/collaborative"
        filename = thedate.strftime("%Y%m%d.csv")
        if not s3_file_exists(DEFAULT_BUCKET, path, filename):
            rows = collab_gen.get_suggestion_df(thedate)
            fout = StringIO()
            writer = csv.writer(fout)
            writer.writerows(rows)
            fout.seek(0)
            data = fout.getvalue().encode("utf8")

            _store_to_s3(data, DEFAULT_BUCKET, path, filename)


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
