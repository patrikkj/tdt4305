from itertools import chain
import pyspark


def to_rdd(spark_context, data):
    if isinstance(data, pyspark.rdd.RDD):
        return data

    if type(data) in (list, tuple):
        return spark_context.parallelize(data)
    
    return spark_context.parallelize((data, ))

def parse_row(row):
    return _parse(row, ('\t', ',', ';'))

def _parse(data, delimiters, depth=0):
    delimiter = delimiters[depth % len(delimiters)]

    if type(data) in (list, tuple):
        return delimiter.join(_parse(elem, delimiters, depth=depth+1) for elem in data)
    return str(data)
