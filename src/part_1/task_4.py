from operator import add


def task_4a(bt_rdd, rt_rdd, fg_rdd):
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

def task_4b(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns mean and median for number of in and out degrees in the friendship graph.
    """
    # Source
    src_degrees = fg_rdd.map(lambda row: (row[0], 1)) \
        .reduceByKey(add)
    
    # Calculate mean
    mean_src_degrees = src_degrees.values().mean()

    # Calculate median
    sorted_src_degrees = src_degrees.sortBy(lambda tup: tup[1]).values().zipWithIndex().map(lambda tup: (tup[1], tup[0]))
    median_src_degree = sorted_src_degrees.lookup(src_degrees.count() // 2)
    
    # Destination
    dest_degrees = fg_rdd.map(lambda row: (row[1], 1)) \
        .reduceByKey(add)
    
    # Calculate mean
    mean_dest_degrees = dest_degrees.values().mean()

    # Calculate median
    sorted_dest_degrees = dest_degrees.sortBy(lambda tup: tup[1]).values().zipWithIndex().map(lambda tup: (tup[1], tup[0]))
    median_dest_degree = sorted_dest_degrees.lookup(dest_degrees.count() // 2)

    means = (mean_src_degrees, mean_dest_degrees)
    medians = (median_src_degree[0], median_dest_degree[0])
    return means, medians


def print_subtasks():
    pass

def export_to_csv():
    pass