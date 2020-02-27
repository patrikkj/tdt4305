import csv, io, os
import tdt4305.utils as utils

def task_3a(bt_rdd):
    """
    Returns the average rating for businesses in each city.
    """
    bt_ratings = bt_rdd.map(lambda row: [row[3], (1, int(row[8]))])
    bt_per_city = bt_ratings.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
    bt_average_ratings = bt_per_city.mapValues(lambda value: value[1] / value[0])
    return bt_average_ratings

def task_3b(bt_rdd):
    """
    Returns the top 10 most frequent categories in the data.
    """
    bt_categories = bt_rdd.map(lambda row: row[10])
    bt_flattened = bt_categories.flatMap(lambda categories: (c.strip() for c in categories.split(',')))
    bt_per_category = bt_flattened.countByValue()
    return list(bt_per_category.items())[:10]

def task_3c(bt_rdd):
    """
    Returns the geographical centroid of the region for each postal code in the Business table.
    """
    bt_coords = bt_rdd.map(lambda row: [row[5], (1, float(row[6]), float(row[7]))])
    bt_per_postal_code = bt_coords.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1], v1[2] + v2[2]))
    bt_centroids = bt_per_postal_code.mapValues(lambda v: (v[1] / v[0], v[2] / v[0]))
    return bt_centroids

def run(bt_rdd):
    print("\n --- TASK 3a --- ")
    print("Average rating per city")
    average_ratings = task_3a(bt_rdd).collect()
    print(f"Ratings = {sorted(average_ratings)}")

    print("\n --- TASK 3b --- ")
    print("Category frequency")
    category_freq = task_3b(bt_rdd)
    print(f"frequencies = {sorted(category_freq)}")

    print("\n --- TASK 3c --- ")
    print("Geographical centroids")
    centroids = task_3c(bt_rdd).collect()
    print(f"centroids = {sorted(centroids)}")

def export(spark_context, bt_rdd, output_dir, extension):
    for subtask in (task_3a, task_3b, task_3c):
        results = subtask(bt_rdd)

        # Convert data to RDD
        results_rdd = utils.to_rdd(spark_context, results)

        # Parse RDDs to a format suitable for export
        results_rdd = results_rdd.map(utils.parse_row)
        
        # Write to file
        path = f"{output_dir}/{subtask.__name__}.{extension}"
        print(f"Writing to '{path}' ...")
        results_rdd.coalesce(1).saveAsTextFile(path)
        print(f"Done writing to '{path}'\n")
