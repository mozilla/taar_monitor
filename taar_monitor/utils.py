import sys
from py4j.protocol import Py4JJavaError


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
