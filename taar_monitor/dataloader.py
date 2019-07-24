from .utils import check_py3
from .locale import LocaleSuggestionData

from datetime import date, timedelta
from io import StringIO
import boto3
import botocore
import csv
import os.path


check_py3()


DEFAULT_BUCKET = "srg-team-bucket"


def update_last_30days_locale(spark):
    def locale_to_csv_row(r):
        return (
            r["locale"],
            r["guid"],
            date.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d"),
        )

    ll = LocaleSuggestionData(spark)
    today = date.today()

    for i in range(30):
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


def s3_normpath(path, filename):
    # Normalize the path s3_path = os.path.normpath(os.path.join(path, filename))
    s3_path = os.path.join(path, filename)
    while s3_path.startswith("/"):
        s3_path = s3_path[1:]
    return s3_path


def s3_file_exists(bucket, path, filename):
    s3 = boto3.resource("s3")
    s3_path = s3_normpath(path, filename)

    try:
        s3.Object("my-bucket", s3_path).load()
        print("s3://{}/{} already exists".format(bucket, s3_path))
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("403", "404"):
            # The object does not exist.
            return False

        # Re-raise the exception as something terrible has happened in
        # AWS
        raise


def _store_to_s3(data, bucket, path, filename):
    s3_path = s3_normpath(path, filename)

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).put_object(Key=s3_path, Body=data)
