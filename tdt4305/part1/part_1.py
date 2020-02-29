import os, sys

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

from . import task_1, task_2, task_3, task_4, task_5, task_6
from .. import utils


INPUT_DIR = "./data"
OUTPUT_DIR = "./results"

SAMPLES_INPUT_DIR = "./samples/data"
SAMPLES_OUTPUT_DIR = "./samples/results"

BT_FILENAME = "yelp_businesses.csv"
RT_FILENAME = "yelp_top_reviewers_with_reviews.csv"
FG_FILENAME = "yelp_top_users_friendship_graph.csv"
FILENAMES = (BT_FILENAME, RT_FILENAME, FG_FILENAME) 

def initialize():
    # Initialization
    conf = SparkConf()
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    sql_context = SQLContext(spark_context)
    print('\n'*100)
    return spark_context, sql_context

def load_data(spark_context, sql_context, input_dir):
    # Load RDDs (task 1a) and dataframes (task 5a)
    paths = (utils.gen_path(input_dir, filename) for filename in FILENAMES)
    raw_rdds = task_1.load_rdds(spark_context, paths)
    rdds = task_1.preprocessing(*raw_rdds)
    dataframes = task_5.load_dataframes(sql_context, paths)
    return rdds, dataframes

def run(spark_context, sql_context, tasks, input_dir):
    # Load RDDs (task 1a) and dataframes (task 5a)    
    rdds, dataframes = load_data(spark_context, sql_context, input_dir)
    bt_rdd, rt_rdd, fg_rdd = rdds
    bt_df, rt_df, fg_df = dataframes

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

def export_csv(spark_context, sql_context, tasks, input_dir, output_dir, extension):
    # Load RDDs (task 1a) and dataframes (task 5a)    
    rdds, dataframes = load_data(spark_context, sql_context, input_dir)
    bt_rdd, rt_rdd, fg_rdd = rdds
    bt_df, rt_df, fg_df = dataframes

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
        globals()[f'task_{task}'].export(*export_args[task], output_dir, extension)
        print()

def export_txt(spark_context, sql_context, tasks, input_dir, output_dir):
    '''Redirects console output to file.'''
    path = f"{output_dir}/output.txt"
    print(f"Writing console output to '{path}' ...")
    with open(path, 'w') as sys.stdout:
        run(spark_context, sql_context, tasks, input_dir)


def main(action, tasks, sample):
    input_dir = SAMPLES_INPUT_DIR if sample else INPUT_DIR
    output_dir = SAMPLES_OUTPUT_DIR if sample else OUTPUT_DIR
    spark_context, sql_context = initialize()

    if action == 'run':
        run(spark_context, sql_context, tasks, input_dir)
    elif action == 'export-csv':
        export_csv(spark_context, sql_context, tasks, input_dir, output_dir, 'csv')
    elif action == 'export-tsv':
        export_csv(spark_context, sql_context, tasks, input_dir, output_dir, 'tsv')
    elif action == 'export-txt':
        export_txt(spark_context, sql_context, tasks, input_dir, output_dir)