from base64 import b64decode
from itertools import islice
from pyspark.sql import functions, types

from .. import utils


def load_sentiment_mapping(resources_dir):
    STOPWORDS_PATH = f"{resources_dir}\stopwords.txt"
    AFINN_PATH = f"{resources_dir}\AFINN-111.txt"

    with open(STOPWORDS_PATH) as f1, open(AFINN_PATH) as f2:
        stopwords = {line.strip() for line in f1}
        afinn = {k: int(v) for k, v in (line.split('\t') for line in f2)}
        return afinn, stopwords

def load_reviews(sql_context, input_dir, filename):
    rt_struct = types.StructType([
        types.StructField("review_id", types.StringType(), True),
        types.StructField("user_id", types.StringType(), True),
        types.StructField("business_id", types.StringType(), True),
        types.StructField("review_text", types.StringType(), True),
        types.StructField("review_date", types.FloatType(), True)
    ])
    path = utils.gen_path(input_dir, filename)
    rt_df = sql_context.read.csv(path, schema=rt_struct, sep='\t', header=True)

    # Decode base64-encoded review texts
    rt_df = rt_df.withColumn("review_text", functions.unbase64(rt_df.review_text))  # base64 -> bytes(UTF-8)
    rt_df = rt_df.withColumn("review_text", functions.decode(rt_df.review_text, 'UTF-8'))   # bytes(UTF-8) -> string(UTF-8)

    # Drop redundant columns
    rt_df = rt_df.drop("review_id", "user_id", "review_date")
    return rt_df
