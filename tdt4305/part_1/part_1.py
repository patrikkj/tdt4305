import sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from . import task_1, task_2, task_3, task_4, task_5, task_6

# BT_PATH = "data/yelp_businesses.csv"
# RT_PATH = "data/yelp_top_reviewers_with_reviews.csv"
# FG_PATH = "data/yelp_top_users_friendship_graph.csv"

BT_PATH = "samples/yelp_businesses_sample.csv"
RT_PATH = "samples/yelp_top_reviewers_with_reviews_sample.csv"
FG_PATH = "samples/yelp_top_users_friendship_graph_sample.csv"
paths = (BT_PATH, RT_PATH, FG_PATH) 


# Initialization
conf = SparkConf()
spark_context = SparkContext(conf=conf)
spark_context.setLogLevel("ERROR")
sql_context = SQLContext(spark_context)
print('\n'*100)


def run(tasks):
    # Load RDDs (task 1a) and dataframes (task 5a)
    bt_rdd_raw, rt_rdd_raw, fg_rdd_raw = task_1.load_rdds(spark_context, paths)
    bt_rdd, rt_rdd, fg_rdd = task_1.preprocessing(bt_rdd_raw, rt_rdd_raw, fg_rdd_raw)
    bt_df, rt_df, fg_df = task_5.load_dataframes(sql_context, paths)

    run_args = {
        1: (bt_rdd, rt_rdd, fg_rdd),
        2: (rt_rdd, ),
        3: (bt_rdd, ),
        4: (fg_rdd, ),
        5: (bt_df, rt_df, fg_df),
        6: (sql_context, bt_df, rt_df, fg_df)
    }

    # Run tasks
    for task in tasks:
        globals()[f'task_{task}'].run(*run_args[task])

def export_tsv(tasks):
    # Load RDDs (task 1a) and dataframes (task 5a)
    bt_rdd_raw, rt_rdd_raw, fg_rdd_raw = task_1.load_rdds(spark_context, paths)
    bt_rdd, rt_rdd, fg_rdd = task_1.preprocessing(bt_rdd_raw, rt_rdd_raw, fg_rdd_raw)
    bt_df, rt_df, fg_df = task_5.load_dataframes(sql_context, paths)

    export_args = {
        1: (spark_context, bt_rdd, rt_rdd, fg_rdd),
        2: (spark_context, rt_rdd),
        3: (spark_context, bt_rdd),
        4: (spark_context, fg_rdd),
        5: (spark_context, bt_df, rt_df, fg_df),
        6: (spark_context, sql_context, bt_df, rt_df, fg_df)
    }

    # Export tasks
    for task in tasks:
        header = f"Exporting task {task}"
        print(header)
        print('-'*len(header))
        globals()[f'task_{task}'].export(*export_args[task])
        print()

def export_txt(tasks):
    '''Redirects console output to file.'''
    print("Writing console output to './results/output.txt' ...")
    with open(f"./results/output.txt", 'w') as sys.stdout:
        run(tasks)


def main(action, tasks):
    if action == 'run':
        run(tasks)
    elif action == 'export-tsv':
        export_tsv(tasks)
    elif action == 'export-txt':
        export_txt(tasks)
