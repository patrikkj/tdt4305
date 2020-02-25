import base64


def load_rdds(spark_context, paths):
    """
    Loads each CSV file into separate RDD objects.
    """
    rdds = [spark_context.textFile(path, ) for path in paths]
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

    # Extract headers
    bt_header = bt_rdd_raw.first() 
    rt_header = rt_rdd_raw.first() 
    fg_header = fg_rdd_raw.first()

    bt_rdd = bt_rdd_raw.filter(lambda row: row != bt_header)
    rt_rdd = rt_rdd_raw.filter(lambda row: row != rt_header)
    fg_rdd = fg_rdd_raw.filter(lambda row: row != fg_header)

    # Decode review text strings
    def review_text_decoder(row):
        review_text_b64 = row[3]
        review_text_bytes = base64.b64decode(review_text_b64)
        review_text = review_text_bytes.decode('utf-8', errors='replace')
        row[3] = review_text
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
    return bt_size, rt_size, fg_size


def print_subtasks():
    pass

def export_to_csv():
    pass