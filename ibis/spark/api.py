from ibis.spark.client import SparkClient


def connect(sql_context):
    return SparkClient(sql_context)
