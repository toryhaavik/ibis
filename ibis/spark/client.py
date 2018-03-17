from ibis.client import Client
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir

from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.types as pt


class SparkClient(Client):
    def __init__(self, sql_context : SQLContext):
        self.sql_context = sql_context

    def schema(self, name):
        table = self.sql_context.table(name)
        return table.schema

    def table(self, name):
        spark_sql_table = self.sql_context.table(name)
        node = SparkSQLTable(spark_sql_table, self)
        return ir.TableExpr(node)

    def execute(self, expr, params=None, limit='default', async=False, **kwargs):
        pass


class SparkSQLTable(ops.DatabaseTable):
    def __init__(self, table, source):
        schema = sch.infer(table)
        super(SparkSQLTable, self).__init__(table, schema, source)
        self.spark_sql_table = table


@dt.dtype.register(pt.StringType)
def pyspark_string(pstype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(pt.LongType)
def pyspark_long(pstype, nullable=True):
    return dt.Int64(nullable=nullable)


@sch.infer.register(DataFrame)
def schema_from_table(table):
    pairs = []
    for f in table.schema.fields:
        dtype = dt.dtype(f.dataType)
        pairs.append((f.name, dtype))
    return sch.schema(pairs)
