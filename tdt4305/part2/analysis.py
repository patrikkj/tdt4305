from operator import add

from pyspark.sql import functions, types


def remove_stopwords(tokens, stopwords):
    return list(token for token in tokens if token not in stopwords)

def evaluate_score(tokens, afinn):
    return sum(afinn.get(token, 0) for token in tokens)

def analyze(rt_df, afinn, stopwords, k):
    # Create functions for evaluating tokens
    remove_stopwords_udf = functions.udf(lambda col: remove_stopwords(col, stopwords), types.ArrayType(types.StringType()))
    evaluate_score_udf = functions.udf(lambda col: evaluate_score(col, afinn), types.IntegerType())

    # Map functions over review text column
    rt_df = rt_df.withColumn("review_text", functions.lower(rt_df.review_text))
    rt_df = rt_df.withColumn("review_text", functions.regexp_replace(rt_df.review_text, r"[^\sa-z]", ''))
    rt_df = rt_df.withColumn("review_text", functions.split(rt_df.review_text, r"\s+"))
    rt_df = rt_df.withColumn("review_text", remove_stopwords_udf(rt_df.review_text))
    rt_df = rt_df.withColumn("review_text", evaluate_score_udf(rt_df.review_text))

    # Group scores by business
    busniesses_grouped = rt_df.groupBy(rt_df.business_id)
    businesses_scores = busniesses_grouped.sum()
    businesses_sorted = businesses_scores.sort('sum(review_text)', ascending=False)
    top_businesses_df = businesses_sorted.take(k)
    return list(map(lambda row: (row['business_id'], row['sum(review_text)']), top_businesses_df))
