from base64 import b64decode
from itertools import islice

from .. import utils


def load_sentiment_mapping(resources_dir):
    STOPWORDS_PATH = f"{resources_dir}\stopwords.txt"
    AFINN_PATH = f"{resources_dir}\AFINN-111.txt"

    with open(STOPWORDS_PATH) as f1, open(AFINN_PATH) as f2:
        stopwords = {line.strip() for line in f1}
        afinn = {k: int(v) for k, v in (line.split('\t') for line in f2)}
        return afinn, stopwords

def load_reviews(spark_context, input_dir, filename):
    # Load RDD
    path = utils.gen_path(input_dir, filename)
    rdd_strings = spark_context.textFile(path, 10)
    rt_rdd_raw = rdd_strings.map(lambda line: line.split("\t"))

    # Remove header
    rt_rdd = rt_rdd_raw.mapPartitionsWithIndex(lambda i, it: islice(it, 1, None) if i == 0 else it)

    # Decode review text strings
    def review_text_decoder(row):
        row[3] = b64decode(row[3]).decode('utf-8', errors='replace')
        return row
    rt_rdd = rt_rdd.map(review_text_decoder)

    # Change storage policy
    rt_rdd.persist()
    return rt_rdd
