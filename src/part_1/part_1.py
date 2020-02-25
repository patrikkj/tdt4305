import base64
from datetime import datetime
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, types
from pyspark.sql.functions import from_unixtime

## 
from time import perf_counter

def time_func(func):
    def inner(*args, **kwargs):
        t1 = perf_counter()
        output = func(*args, **kwargs)
        t2 = perf_counter()
        print(f"{func.__name__} executed in {t2-t1:.4f} seconds.")
        return output
    return inner 
##




APP_NAME = "Spark application"
SAMPLE_SIZE = 0.00001

# BUSINESS_TABLE_PATH = "data/yelp_businesses.csv"
# REVIEW_TABLE_PATH = "data/yelp_top_reviewers_with_reviews.csv"
# FRIENDSHIP_GRAPH_PATH = "data/yelp_top_users_friendship_graph.csv"
BUSINESS_TABLE_PATH = "data/yelp_businesses_sample.csv"
REVIEW_TABLE_PATH = "data/yelp_top_reviewers_with_reviews_sample.csv"
FRIENDSHIP_GRAPH_PATH = "data/yelp_top_users_friendship_graph_sample.csv"
PATHS = (BUSINESS_TABLE_PATH, REVIEW_TABLE_PATH, FRIENDSHIP_GRAPH_PATH) 

# Initialize spark context
conf = SparkConf().setAppName(APP_NAME)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sql_context = SQLContext(sc)
print('\n'*100)

# RDD FORMAT
# BUSINESS TABLE:  ["business_id"	"name"	"address"	"city"	"state"	"postal_code"	"latitude"	"longitude"	"stars"	"review_count"	"categories"]
# REVIEW TABLE:     ["review_id"	"user_id"	"business_id"	"review_text"	"review_date"]
# FRIENDSHIP GRAPH: ["src_user_id","dst_user_id"]


# --- --- --- TASK 1 --- --- --- 


# Task 1a (Load RDDs)
rdds = [sc.textFile(path, ) for path in PATHS]
bt_rdd, rt_rdd, fg_rdd = rdds

bt_rdd_raw = bt_rdd.map(lambda line: line.split("\t"))
rt_rdd_raw = rt_rdd.map(lambda line: line.split("\t"))
fg_rdd_raw = fg_rdd.map(lambda line: line.split(","))

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


@time_func
def task_1a():
    bt_size = bt_rdd.count()
    rt_size = rt_rdd.count()
    fg_size = fg_rdd.count()
    return bt_size, rt_size, fg_size


# --- --- --- TASK 2 --- --- --- 

@time_func
def task_2a():
    user_ids = rt_rdd.map(lambda row: row[1])
    unique_user_count = user_ids.distinct().count()
    return unique_user_count

@time_func
def task_2b():
    reviews_length = rt_rdd.map(lambda row: len(row[3]))
    average_review_length = reviews_length.sum() / reviews_length.count()
    return average_review_length

@time_func
def task_2c_1():
    bt_sorted = bt_rdd.sortBy(lambda row: row[9])
    top_businesses = bt_sorted.map(lambda row: row[0]).take(10)
    return top_businesses

@time_func
def task_2c_2():
    # Group reviews by business ID
    rt_grouped = rt_rdd.groupBy(lambda row: row[2]) 
    rt_count_per_id = rt_grouped.mapValues(len)
    rt_sorted = rt_count_per_id.sortBy(lambda row: row[1])
    top_businesses = rt_sorted.map(lambda row: row[0]).take(10)
    return top_businesses

@time_func
def task_2d():
    rt_grouped_by_year = rt_rdd.groupBy(lambda row: datetime.fromtimestamp(int(float(row[4]))).year)
    rt_reviews_per_year = rt_grouped_by_year.mapValues(len)
    return sorted(rt_reviews_per_year.collect())

@time_func
def task_2e():
    rt_parsed = rt_rdd.map(lambda row: int(float(row[4])))
    datetime_first = datetime.fromtimestamp(rt_parsed.min())
    datetime_last = datetime.fromtimestamp(rt_parsed.max())
    return datetime_first, datetime_last

@time_func
def task_2f():
    # Find number of reviews per user
    rt_grouped_by_user = rt_rdd.groupBy(lambda row: row[1])
    rt_reviews_count_per_user = rt_grouped_by_user.mapValues(len)
    
    # Find review length per review, along with a counter variable
    rt_extracted_lengths = rt_rdd.map(lambda row: [row[1], (1, len(row[3]))])
    rt_tuples_per_user = rt_extracted_lengths.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
    rt_average_length_per_user = rt_tuples_per_user.mapValues(lambda value: value[1] / value[0])
    
    # Average review count
    rt_review_counts = rt_reviews_count_per_user.values()
    X = rt_review_counts.sum() / rt_review_counts.count()
    
    # Average review length
    rt_review_lengths = rt_average_length_per_user.values()
    Y = rt_review_lengths.sum() / rt_review_lengths.count()

    x_diff = rt_review_counts.map(lambda x_i: x_i - X)
    y_diff = rt_review_lengths.map(lambda y_i: y_i - Y)
    sum_sqdiff_x = x_diff.map(lambda x_diff: x_diff**2).sum()
    sum_sqdiff_y = y_diff.map(lambda y_diff: y_diff**2).sum()
    numerator = x_diff.zip(y_diff).map(lambda diff: diff[0] * diff[1]).sum()
    denominator = sum_sqdiff_x**0.5 * sum_sqdiff_y**0.5
    return numerator / denominator


# --- --- --- TASK 3 --- --- --- 


@time_func
def task_3a():
    bt_ratings = bt_rdd.map(lambda row: [row[3], (1, int(row[8]))])
    bt_per_city = bt_ratings.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
    bt_average_ratings = bt_per_city.mapValues(lambda value: value[1] / value[0])
    return bt_average_ratings.collect()

@time_func
def task_3b():
    bt_categories = bt_rdd.map(lambda row: row[10])
    bt_flattened = bt_categories.flatMap(lambda categories: (c.strip() for c in categories.split(',')))
    bt_per_category = bt_flattened.countByValue()
    return list(bt_per_category.items())[:10]

@time_func
def task_3c():
    bt_coords = bt_rdd.map(lambda row: [row[5], (1, float(row[6]), float(row[7]))])
    bt_per_postal_code = bt_coords.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1], v1[2] + v2[2]))
    bt_centroids = bt_per_postal_code.mapValues(lambda v: (v[1] / v[0], v[2] / v[0]))
    return bt_centroids


# --- --- --- TASK 4 --- --- --- 


@time_func
def task_4a():
    # Source
    highest_src = fg_rdd.map(lambda row: (row[0], 1)) \
        .reduceByKey(add) \
        .top(10, key=lambda tup: tup[1]) 

    # Destination
    highest_dest = fg_rdd.map(lambda row: (row[1], 1)) \
        .reduceByKey(add) \
        .top(10, key=lambda tup: tup[1]) 
    return highest_src, highest_dest

@time_func
def task_4b():
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


# --- --- --- TASK 5 --- --- --- 


# Load Dataframes
_bt_struct = types.StructType([
    types.StructField("business_id", types.StringType(), True),
    types.StructField("name", types.StringType(), True),
    types.StructField("address", types.StringType(), True),
    types.StructField("city", types.StringType(), True),
    types.StructField("state", types.StringType(), True),
    types.StructField("postal_code", types.StringType(), True),
    types.StructField("latitude", types.FloatType(), True),
    types.StructField("longitude", types.FloatType(), True),
    types.StructField("stars", types.FloatType(), True),
    types.StructField("review_count", types.IntegerType(), True),
    types.StructField("categories", types.StringType(), True)
])

_rt_struct = types.StructType([
    types.StructField("review_id", types.StringType(), True),
    types.StructField("user_id", types.StringType(), True),
    types.StructField("business_id", types.StringType(), True),
    types.StructField("review_text", types.StringType(), True),
    types.StructField("review_date", types.FloatType(), True)
])

_fg_struct = types.StructType([
    types.StructField("src_user_id", types.StringType(), True),
    types.StructField("dst_user_id", types.StringType(), True)
])

bt_df = sql_context.read.csv(BUSINESS_TABLE_PATH, schema=_bt_struct, sep='\t', header=True)
rt_df = sql_context.read.csv(REVIEW_TABLE_PATH, schema=_rt_struct, sep='\t', header=True)
fg_df = sql_context.read.csv(FRIENDSHIP_GRAPH_PATH, schema=_fg_struct, sep=',', header=True)

# Postprocessing
rt_df = rt_df.withColumn("review_date", from_unixtime(rt_df.review_date))


@time_func
def task_5a():
    print("Business table schema")
    bt_df.printSchema()
    bt_df.show()    
    
    print("\rReview table schema")
    rt_df.printSchema()    
    rt_df.show()    

    print("Friendship graph schema")
    fg_df.printSchema()    
    fg_df.show()    


# --- --- --- TASK 6 --- --- --- 


@time_func
def task_6a():
    return rt_df.join(bt_df, "business_id")

@time_func
def task_6b():
    df = task_6a()
    
    print("Tables 1")
    sql_context.tables().show()

    sql_context.registerDataFrameAsTable(df, "temp_table")

    print("Tables 2")
    sql_context.tables().show()

@time_func
def task_6c():
    top_users = rt_df.groupBy(rt_df.user_id).count().sort('count', ascending=False).take(20)
    return list(map(lambda row: (row['user_id'], row['count']), top_users))



def run():
    # print("\n --- TASK 1 --- ")
    # bt_size, rt_size, fg_size = task_1a()
    # print(f"Business table size: {bt_size}")
    # print(f"Reviews table size: {rt_size}")
    # print(f"Friendship graph size: {fg_size}")


    # print("\n --- TASK 2a --- ")
    # unique_user_count = task_2a()
    # print(f"Unique user count: {unique_user_count}")


    # print("\n --- TASK 2b --- ")
    # average_review_length = task_2b()
    # print(f"Average review length: {average_review_length}")


    # print("\n --- TASK 2c (1) --- ")
    # top_businesses = task_2c_1()
    # print(f"Top 10 businesses: {top_businesses}")


    # print("\n --- TASK 2c (2) --- ")
    # top_businesses = task_2c_2()
    # print(f"Top 10 businesses: {top_businesses}")


    # print("\n --- TASK 2d --- ")
    # reviews_per_year = task_2d()
    # print(f"Reviews per year: {reviews_per_year}")


    # print("\n --- TASK 2e --- ")
    # datetime_first, datetime_last = task_2e()
    # print(f"Time of first review: {datetime_first}")
    # print(f"Time of last review: {datetime_last}")


    # print("\n --- TASK 2f --- ")
    # print("Pearson correlation coefficient")
    # pcc = task_2f()
    # print(f"PCC = {pcc}")


    # print("\n --- TASK 3a --- ")
    # print("Average rating per city")
    # average_ratings = task_3a()
    # print(f"Ratings = {sorted(average_ratings)}")

    # print("\n --- TASK 3b --- ")
    # print("Category frequency")
    # category_freq = task_3b()
    # print(f"frequencies = {sorted(category_freq)}")



    print("\n --- TASK 3c --- ")
    print("Geographical centroids")
    centroids = task_3c()
    # print(f"centroids = {sorted(centroids)}")
    # import os
    # print(os.getcwd())
    centroids.saveAsTextFile("./output/centroids.csv")







    # print("\n --- TASK 4a --- ")
    # print("FG node degrees")
    # highest_src, highest_dest = task_4a()
    # print(f"highest_src = {sorted(highest_src, key=lambda tup: tup[1], reverse=True)}")
    # print(f"highest_dest = {sorted(highest_dest, key=lambda tup: tup[1], reverse=True)}")

    # print("\n --- TASK 4b --- ")
    # print("Mean and median")
    # means, medians = task_4b()
    # print(f"means = {means}")
    # print(f"medians = {medians}")


    # # --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- 


    # print("\n --- TASK 5a --- ")
    # task_5a()


    # print("\n --- TASK 6a --- ")
    # print("Inner join of BT and RT")
    # task_6a().show()


    # print("\n --- TASK 6b --- ")
    # print("Inner join of BT and RT")
    # # joined_table = task_6b()
    # # print(f"joined_table = {joined_table")
    # task_6b()

    # print("\n --- TASK 6c --- ")
    # print("Get top 20 users by review count")
    # top_users = task_6c()
    # print(f"top_users = {top_users}")


def main():
    run()

if __name__ == "__main__":
    main()
