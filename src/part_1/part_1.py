from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import os
print(f"CWD: {os.getcwd()}")
from tdt4305.src.part_1 import task_1, task_2, task_3, task_4, task_5, task_6

# import tdt4305.src.part_1.task_1 as task_1

# RDD FORMAT
# BUSINESS TABLE (BT):      ["business_id"	"name"	"address"	"city"	"state"	"postal_code"	"latitude"	"longitude"	"stars"	"review_count"	"categories"]
# REVIEW TABLE (RT):        ["review_id"	"user_id"	"business_id"	"review_text"	"review_date"]
# FRIENDSHIP GRAPH (FG):    ["src_user_id"  "dst_user_id"]

# BT_PATH = "data/yelp_businesses.csv"
# RT_PATH = "data/yelp_top_reviewers_with_reviews.csv"
# FG_PATH = "data/yelp_top_users_friendship_graph.csv"

BT_PATH = "samples/yelp_businesses_sample.csv"
RT_PATH = "samples/yelp_top_reviewers_with_reviews_sample.csv"
FG_PATH = "samples/yelp_top_users_friendship_graph_sample.csv"
paths = (BT_PATH, RT_PATH, FG_PATH) 


def initialize():
    conf = SparkConf()
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    sql_context = SQLContext(spark_context)
    print('\n'*100)
    return spark_context, sql_context


# --- INITIALIZATION --- 
spark_context, sql_context = initialize()

# --- TASK 1 --- 
bt_rdd_raw, rt_rdd_raw, fg_rdd_raw = task_1.load_rdds(spark_context, paths)

bt_rdd, rt_rdd, fg_rdd = task_1.preprocessing(bt_rdd_raw, rt_rdd_raw, fg_rdd_raw)

bt_size, rt_size, fg_size = task_1.task_1a(bt_rdd, rt_rdd, fg_rdd)

# --- TASK 2 --- 

# --- TASK 3 --- 

# --- TASK 4 --- 

# --- TASK 5 --- 

# --- TASK 6 --- 


def export_all():
    pass


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


    simpleeee = spark_context.parallelize([("1", ), ("2", ), ("3", ), ("4", )])
    # print(simpleeee.count())
    # simpleeee.coalesce(1, True).saveAsTextFile("simpledirectory")
    simpleeee.coalesce(1, True).saveAsTextFile('./results/result_1.tsv')
    # simpleeee.repartition(1).saveAsTextFile("./out/lala")
    # task_3.export_to_csv(bt_rdd, rt_rdd, fg_rdd)


    # print("\n --- TASK 3a --- ")
    # print("Average rating per city")
    # average_ratings = task_3a()
    # print(f"Ratings = {sorted(average_ratings)}")

    # print("\n --- TASK 3b --- ")
    # print("Category frequency")
    # category_freq = task_3b()
    # print(f"frequencies = {sorted(category_freq)}")

    # print("\n --- TASK 3c --- ")
    # print("Geographical centroids")
    # centroids = task_3c()
    # # print(f"centroids = {sorted(centroids)}")
    # # import os
    # # print(os.getcwd())
    # centroids.saveAsTextFile("./output/centroids.csv")


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


    # # --- --- --- --- --- --- --- --- --- --- --- --- 


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
