from .stmolog import AbstractData

from py4j.protocol import Py4JJavaError
from pyspark.sql.types import LongType, StringType, StructField, StructType
import ast
import dateutil.parser
import re
import time


def parse_ts(ts):
    return int(ts / 1000)


def date_to_ts(dt):
    return int((time.mktime(dateutil.parser.parse(dt).timetuple())))


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


class LocaleSuggestionData(AbstractData):
    QUERY_ID = 63422

    def __init__(self, spark):
        AbstractData.__init__(self, spark)

    def get_suggestion_df(self, tbl_date):
        row_iter = self._get_raw_data(tbl_date)

        spark_schema = StructType(
            [
                StructField("locale", StringType()),
                StructField("guid", StringType()),
                StructField("timestamp", LongType()),
            ]
        )

        df = safe_createDataFrame(self._spark, list(row_iter), schema=spark_schema)
        return df

    def _get_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        locale_re = re.compile(r"client_locale: \[([^]]*)\]")
        guids_re = re.compile(r"guids *: *\[(\[[^]]*\])")

        results = self._query_redash(tbl_date)

        for row in results:
            ts = date_to_ts(row["TIMESTAMP"])
            payload = row["msg"]
            guids_json = guids_re.findall(payload)[0]

            # Note that the AMO server incorrectly queries the TAAR server for 10 addons instead of the spec'd 4
            # so we need to manually truncate that list
            try:
                guids = ast.literal_eval(guids_json)
            except Exception:
                print("Error parsing GUIDS out of : {}".format(guids_json))
                continue

            client_locale = locale_re.findall(payload)[0]
            for guid in guids:
                parsed_data = (client_locale, guid, ts)
                yield parsed_data
