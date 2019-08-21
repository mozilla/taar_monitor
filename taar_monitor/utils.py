import sys
import os
import boto3
import botocore
from py4j.protocol import Py4JJavaError
import json
import requests


def check_py3():
    if sys.version_info.major != 3:
        raise Exception("Requires Python 3 to run properly")


class ClusterRebootRequired(Exception):
    pass


def safe_createDataFrame(spark, pydata, schema):
    """ This wraps the spark.createDataFrame function to clarify that
    the cluster needs a reboot if an exception is encountered.
  """
    try:
        df = spark.createDataFrame(pydata, schema=schema)
    except Py4JJavaError as py4j_exc:
        raise ClusterRebootRequired(
            "A Spark error was encountered which seems to indicate the cluster needs a restart.",
            py4j_exc,
        )

    return df


def store_to_s3(data, bucket, path, filename):
    s3_path = s3_normpath(path, filename)

    s3 = boto3.resource("s3")
    s3.Bucket(bucket).put_object(Key=s3_path, Body=data)


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
