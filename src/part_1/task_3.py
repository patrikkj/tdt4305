import csv, io, os


def task_3a(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the average rating for businesses in each city.
    """
    bt_ratings = bt_rdd.map(lambda row: [row[3], (1, int(row[8]))])
    bt_per_city = bt_ratings.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
    bt_average_ratings = bt_per_city.mapValues(lambda value: value[1] / value[0])
    return bt_average_ratings

def task_3b(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the top 10 most frequent categories in the data.
    """
    bt_categories = bt_rdd.map(lambda row: row[10])
    bt_flattened = bt_categories.flatMap(lambda categories: (c.strip() for c in categories.split(',')))
    bt_per_category = bt_flattened.countByValue()
    return list(bt_per_category.items())[:10]

def task_3c(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the geographical centroid of the region for each postal code in the Business table.
    """
    bt_coords = bt_rdd.map(lambda row: [row[5], (1, float(row[6]), float(row[7]))])
    bt_per_postal_code = bt_coords.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1], v1[2] + v2[2]))
    bt_centroids = bt_per_postal_code.mapValues(lambda v: (v[1] / v[0], v[2] / v[0]))
    return bt_centroids


def print_subtasks():
    pass

def list_to_csv_str(x):
    """Given a list of strings, returns a properly-csv-formatted string."""
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()

def export_to_csv(bt_rdd, rt_rdd, fg_rdd):
    # output = io.StringIO("")
    print(f"CWD: {os.getcwd()}")

    average_ratings = task_3a(bt_rdd, rt_rdd, fg_rdd)
    average_ratings = average_ratings.map(list_to_csv_str)
    average_ratings.saveAsTextFile("tasks")

    # category_freq = task_3b(bt_rdd, rt_rdd, fg_rdd)
    # csv.writer(output).writerow(sorted(category_freq))
    
    # centroids = task_3c(bt_rdd, rt_rdd, fg_rdd)
    # csv.writer(output).writerow(sorted(centroids))


    # output.saveAsTextFile("./output/task_3.csv")

