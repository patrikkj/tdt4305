from base64 import b64decode
from .. import utils
from pyspark.sql.functions import unbase64
from itertools import islice


def load_rdds(spark_context, paths):
    """
    Loads each CSV file into separate RDD objects.
    """
    rdds = (spark_context.textFile(path, ) for path in paths)
    bt_rdd, rt_rdd, fg_rdd = rdds

    bt_rdd_raw = bt_rdd.map(lambda line: line.split("\t"))
    rt_rdd_raw = rt_rdd.map(lambda line: line.split("\t"))
    fg_rdd_raw = fg_rdd.map(lambda line: line.split(","))
    return bt_rdd_raw, rt_rdd_raw, fg_rdd_raw

def preprocessing(bt_rdd_raw, rt_rdd_raw, fg_rdd_raw):
    """
    Convert RDDs into a format suitable for further analysis.

    Returns:
        A tuple containing each of the processed RDD objects.
    """
    # Remove headers
    bt_rdd = bt_rdd_raw.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
    rt_rdd = rt_rdd_raw.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
    fg_rdd = fg_rdd_raw.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )

    # Decode review text strings
    def review_text_decoder(row):
        row[3] = b64decode(row[3]).decode('utf-8', errors='replace')
        return row
        
    rt_rdd = rt_rdd.map(review_text_decoder)

    # Change storage policy
    bt_rdd.persist()
    rt_rdd.persist()
    fg_rdd.persist()
    return bt_rdd, rt_rdd, fg_rdd

def task_1a(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the number of rows in each of the given RDDs.
    """
    bt_size = bt_rdd.count()
    rt_size = rt_rdd.count()
    fg_size = fg_rdd.count()
    return ("business_table", bt_size), ("review_table", rt_size), ("friendship_graph", fg_size)


def run(bt_rdd, rt_rdd, fg_rdd):
    print(" --- TASK 1 --- ")
    bt_size, rt_size, fg_size = task_1a(bt_rdd, rt_rdd, fg_rdd)
    print(f"Business table size = {bt_size[1]}")
    print(f"Reviews table size = {rt_size[1]}")
    print(f"Friendship graph size = {fg_size[1]}")

def export(spark_context, bt_rdd, rt_rdd, fg_rdd, output_dir, extension):
    results_1a = task_1a(bt_rdd, rt_rdd, fg_rdd)

    # Convert data to RDD
    rdd_1a = utils.to_rdd(spark_context, results_1a)

    # Parse RDDs to a format suitable for export
    rdd_1a = rdd_1a.map(utils.parse_row)
    
    # Write to file
    path = f"{output_dir}/task_1a.{extension}"
    print(f"Writing to '{path}' ...")
    rdd_1a.coalesce(1).saveAsTextFile(path)
    print(f"Done writing to '{path}'\n")