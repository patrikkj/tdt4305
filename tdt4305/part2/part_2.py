import sys
from base64 import b64decode
from operator import add

from pyspark import SparkConf, SparkContext

from .. import utils
from . import analysis, loader

INPUT_DIR = "./data"
OUTPUT_DIR = "./results"
SAMPLES_INPUT_DIR = "./samples/data"
RESOURCES_DIR = "./resources"
SAMPLES_OUTPUT_DIR = "./samples/results"
RT_FILENAME = "yelp_top_reviewers_with_reviews.csv"


def initialize():
    conf = SparkConf()
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    print('\n'*100)
    return spark_context

def load(spark_context, input_dir, resources_dir):
    rt_rdd = loader.load_reviews(spark_context, input_dir, RT_FILENAME)
    afinn, stopwords = loader.load_sentiment_mapping(resources_dir)
    return rt_rdd, afinn, stopwords

def run(spark_context, input_dir, resources_dir, k=10):
    # Load dataset and sentiment mapping
    rt_rdd, afinn, stopwords = load(spark_context, input_dir, resources_dir)

    # Perform sentiment analysis and extract top 10 entries
    top_k_businesses = analysis.analyze(rt_rdd, afinn, stopwords, k)
    
    # Print results
    print(f" --- TOP {k} BUSINESSES --- ")
    for business_id, score in top_k_businesses:
        print(f"{business_id:24} {score}")

def export_csv(spark_context, input_dir, output_dir, resources_dir, extension, k=10):
    # Load prerequisites and run analysis
    rt_rdd, afinn, stopwords = load(spark_context, input_dir, resources_dir)
    results = analysis.analyze(rt_rdd, afinn, stopwords, k)

    # Parse  output
    results_rdd = utils.to_rdd(spark_context, results)
    results_rdd = results_rdd.map(utils.parse_row)

    # Write to file
    path = f"{output_dir}/part_2.{extension}"
    print(f"Writing to '{path}' ...")
    results_rdd.coalesce(1).saveAsTextFile(path)
    print(f"Done writing to '{path}'\n")

def export_txt(spark_context, input_dir, output_dir, resources_dir, k=10):
    '''Redirects console output to file.'''
    path = f"{output_dir}/part_2.txt"
    print(f"Writing console output to '{path}' ...")
    with open(path, 'w') as sys.stdout:
        run(spark_context, input_dir, resources_dir, k)

def main(action, sample):
    input_dir = SAMPLES_INPUT_DIR if sample else INPUT_DIR
    output_dir = SAMPLES_OUTPUT_DIR if sample else OUTPUT_DIR
    resources_dir = RESOURCES_DIR
    spark_context = initialize()

    if action == 'run':
        run(spark_context, input_dir, resources_dir)
    elif action == 'export-csv':
        export_csv(spark_context, input_dir, output_dir, resources_dir, 'csv')
    elif action == 'export-tsv':
        export_csv(spark_context, input_dir, output_dir, resources_dir, 'tsv')
    elif action == 'export-txt':
        export_txt(spark_context, input_dir, output_dir, resources_dir)
