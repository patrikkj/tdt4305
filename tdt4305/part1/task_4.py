from operator import add
from .. import utils


def task_4a(fg_rdd):
    """
    Returns the top 10 nodes with the most in and out degrees in the friendship graph.
    """
    # Source
    highest_src = fg_rdd.map(lambda row: (row[0], 1)) \
        .reduceByKey(add) \
        .top(10, key=lambda tup: tup[1]) 

    # Destination
    highest_dest = fg_rdd.map(lambda row: (row[1], 1)) \
        .reduceByKey(add) \
        .top(10, key=lambda tup: tup[1]) 
    return highest_src, highest_dest

def task_4b(fg_rdd):
    """
    Returns mean and median for number of in and out degrees in the friendship graph.
    """
    # Source
    src_degrees = fg_rdd.map(lambda row: (row[0], 1)) \
        .reduceByKey(add)
    
    # Calculate mean
    src_mean = src_degrees.values().mean()

    # Calculate median
    src_count = src_degrees.count() 
    src_sorted = src_degrees.sortBy(lambda tup: tup[1]).values().zipWithIndex().map(lambda tup: (tup[1], tup[0]))
    if src_count % 2 == 0:
        a = src_sorted.lookup(src_count // 2)[0]
        b = src_sorted.lookup(src_count // 2 + 1)[0]
        src_median = (a + b) / 2
    else:
        src_median = src_sorted.lookup(src_count // 2)[0]
    
    # Destination
    dest_degrees = fg_rdd.map(lambda row: (row[1], 1)) \
        .reduceByKey(add)
    
    # Calculate mean
    dest_mean = dest_degrees.values().mean()

    # Calculate median
    dest_count = dest_degrees.count() 
    dest_sorted = dest_degrees.sortBy(lambda tup: tup[1]).values().zipWithIndex().map(lambda tup: (tup[1], tup[0]))
    if dest_count % 2 == 0:
        a = dest_sorted.lookup(dest_count // 2)[0]
        b = dest_sorted.lookup(dest_count // 2 + 1)[0]
        dest_median = (a + b) / 2
    else:
        dest_median = dest_sorted.lookup(dest_count // 2)[0]

    means = (src_mean, dest_mean)
    medians = (src_median, dest_median)
    return means, medians


def run(fg_rdd):
    print("\n --- TASK 4a --- ")
    print("FG node degrees")
    highest_src, highest_dest = task_4a(fg_rdd)
    print(f"highest_src = {sorted(highest_src, key=lambda tup: tup[1], reverse=True)}")
    print(f"highest_dest = {sorted(highest_dest, key=lambda tup: tup[1], reverse=True)}")

    print("\n --- TASK 4b --- ")
    print("Mean and median")
    means, medians = task_4b(fg_rdd)
    print(f"means = {means}")
    print(f"medians = {medians}")

def export(spark_context, fg_rdd, output_dir, extension):
    for subtask in (task_4a, task_4b):
        results = subtask(fg_rdd)

        # Convert data to RDD
        results_rdd = utils.to_rdd(spark_context, results)

        # Parse RDDs to a format suitable for export
        results_rdd = results_rdd.map(utils.parse_row)
        
        # Write to file
        path = f"{output_dir}/{subtask.__name__}.{extension}"
        print(f"Writing to '{path}' ...")
        results_rdd.coalesce(1).saveAsTextFile(path)
        print(f"Done writing to '{path}'\n")