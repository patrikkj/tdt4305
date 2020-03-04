from datetime import datetime
from operator import add, truediv
from .. import utils


def task_2a(rt_rdd):
    """
    Returns the number of distinct users in 'review table'.
    """
    user_ids = rt_rdd.map(lambda row: row[1])
    unique_user_count = user_ids.distinct().count()
    return unique_user_count

def task_2b(rt_rdd):
    """
    Returns the average review length.
    """
    reviews_length = rt_rdd.map(lambda row: len(row[3]))
    average_review_length = reviews_length.sum() / reviews_length.count()
    return average_review_length

def task_2c(rt_rdd):
    """
    Returns the top 10 businesses by number of reviews.
    """
    rt_grouped = rt_rdd.groupBy(lambda row: row[2]) 
    rt_count_per_id = rt_grouped.mapValues(len)
    rt_sorted = rt_count_per_id.sortBy(lambda row: row[1], ascending=False)
    top_businesses = rt_sorted.take(10)
    # top_businesses = rt_sorted.map(lambda row: row[0]).take(10)
    return top_businesses

def task_2d(rt_rdd):
    """
    Returns the number of reviews per year.
    """
    rt_grouped_by_year = rt_rdd.groupBy(lambda row: datetime.fromtimestamp(int(float(row[4]))).year)
    rt_reviews_per_year = rt_grouped_by_year.mapValues(len)
    return sorted(rt_reviews_per_year.collect())

def task_2e(rt_rdd):
    """
    Returns the time and date for the first and last review.
    """
    rt_parsed = rt_rdd.map(lambda row: int(float(row[4])))
    datetime_first = datetime.fromtimestamp(rt_parsed.min())
    datetime_last = datetime.fromtimestamp(rt_parsed.max())
    return datetime_first, datetime_last


# def task_2f_ORIGINAL(rt_rdd):
#     """
#     Calculates the 'Pearson correlation coefficient' between the number of reviews 
#     by a user and the average number of the characters in the user’s reviews.
#     """
#     # Element-wise difference from average number of reviews
#     rt_grouped_by_user = rt_rdd.groupBy(lambda row: row[1])
#     rt_count_per_user = rt_grouped_by_user.mapValues(len).values()
#     X = rt_count_per_user.sum() / rt_count_per_user.count()
#     x_diff = rt_count_per_user.map(lambda x_i: x_i - X)

#     # Element-wise difference from average review length
#     rt_review_lengths = rt_rdd.map(lambda row: [row[1], (1, len(row[3]))])
#     rt_tuples_per_user = rt_review_lengths.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
#     rt_average_per_user = rt_tuples_per_user.mapValues(lambda v: v[1] / v[0]).values()
#     Y = rt_average_per_user.sum() / rt_average_per_user.count()
#     y_diff = rt_average_per_user.map(lambda y_i: y_i - Y)
    
#     # Sum of squared differences for number of reviews.
#     sum_sqdiff_x = x_diff.map(lambda x_diff: x_diff**2).sum()

#     # Sum of squared differences for review length.
#     sum_sqdiff_y = y_diff.map(lambda y_diff: y_diff**2).sum()
    
#     # Combine expressions
#     numerator = x_diff.zip(y_diff).map(lambda diff: diff[0] * diff[1]).sum()
#     denominator = sum_sqdiff_x**0.5 * sum_sqdiff_y**0.5
#     return numerator / denominator


# def task_2f(rt_rdd):
#     """
#     Calculates the 'Pearson correlation coefficient' between the number of reviews 
#     by a user and the average number of the characters in the user’s reviews.
#     """
#     # Mapping of the form: 'user_id' -> 'decoded_review_text'
#     rt_user_reviews = rt_rdd.map(lambda row: [row[1], row[3]])

#     # Element-wise difference from average number of reviews
#     rt_counts = rt_user_reviews.groupByKey().mapValues(len).values()
#     X = rt_counts.sum() / rt_counts.count()
#     x_diff = rt_counts.map(lambda x_i: x_i - X)

#     # Element-wise difference from average review length
#     rt_totals = rt_user_reviews.mapValues(len).reduceByKey(add).values()
#     rt_averages = rt_totals.zip(rt_counts).map(lambda row: row[0] / row[1])
#     Y = rt_averages.sum() / rt_averages.count()
#     y_diff = rt_averages.map(lambda y_i: y_i - Y)
    
#     # Sum of squared differences for number of reviews.
#     sum_sqdiff_x = x_diff.map(lambda x_diff: x_diff**2).sum()

#     # Sum of squared differences for review length.
#     sum_sqdiff_y = y_diff.map(lambda y_diff: y_diff**2).sum()
    
#     # Combine expressions
#     numerator = x_diff.zip(y_diff).map(lambda diff: diff[0] * diff[1]).sum()
#     denominator = sum_sqdiff_x**0.5 * sum_sqdiff_y**0.5
#     return numerator / denominator


# def task_2f(rt_rdd):
#     """
#     Calculates the 'Pearson correlation coefficient' between the number of reviews 
#     by a user and the average number of the characters in the user’s reviews.
#     """
#     # Mapping of the form: 'user_id' -> 'decoded_review_text'
#     rt_user_reviews = rt_rdd.map(lambda row: [row[1], row[3]])
#     rt_counts = rt_user_reviews.groupByKey().mapValues(len).values()
#     rt_totals = rt_user_reviews.mapValues(len).reduceByKey(add).values()
#     rt_averages = rt_totals.zip(rt_counts).map(lambda row: row[0] / row[1])

#     # Element-wise difference from average number of reviews
#     X = rt_counts.sum() / rt_counts.count()
#     x_diff = rt_counts.map(lambda x_i: x_i - X)

#     # Element-wise difference from average review length
#     Y = rt_averages.sum() / rt_averages.count()
#     y_diff = rt_averages.map(lambda y_i: y_i - Y)
    
#     # Sum of squared differences for number of reviews.
#     sum_sqdiff_x = x_diff.map(lambda x_diff: x_diff**2).sum()

#     # Sum of squared differences for review length.
#     sum_sqdiff_y = y_diff.map(lambda y_diff: y_diff**2).sum()
    
#     # Combine expressions
#     numerator = x_diff.zip(y_diff).map(lambda diff: diff[0] * diff[1]).sum()
#     denominator = sum_sqdiff_x**0.5 * sum_sqdiff_y**0.5
#     return numerator / denominator

from pyspark.mllib.stat import Statistics

def task_2f(rt_rdd):
    """
    Calculates the 'Pearson correlation coefficient' between the number of reviews 
    by a user and the average number of the characters in the user’s reviews.
    """
    # Mapping of the form: 'user_id' -> 'decoded_review_text'
    rt_user_reviews = rt_rdd.map(lambda row: [row[1], row[3]])
    rt_counts = rt_user_reviews.combineByKey(bool, lambda a, b: a + bool(b), add).values()
    # rt_counts = rt_user_reviews.mapValues(bool).reduceByKey(add).values()
    # rt_counts = rt_user_reviews.groupByKey().mapValues(len).values()
    rt_totals = rt_user_reviews.mapValues(len).reduceByKey(add).values()
    rt_averages = rt_totals.zip(rt_counts).map(lambda row: row[0] / row[1])
    return Statistics.corr(rt_counts, rt_averages)


def run(rt_rdd):
    print("\n --- TASK 2a --- ")
    unique_user_count = task_2a(rt_rdd)
    print(f"Unique user count = {unique_user_count}")

    print("\n --- TASK 2b --- ")
    average_review_length = task_2b(rt_rdd)
    print(f"Average review length = {average_review_length}")

    print("\n --- TASK 2c  --- ")
    top_businesses = task_2c(rt_rdd)
    print(f"Top 10 businesses = {top_businesses}")

    print("\n --- TASK 2d --- ")
    reviews_per_year = task_2d(rt_rdd)
    print(f"Reviews per year = {reviews_per_year}")

    print("\n --- TASK 2e --- ")
    datetime_first, datetime_last = task_2e(rt_rdd)
    print(f"Time of first review = {datetime_first}")
    print(f"Time of last review = {datetime_last}")

    print("\n --- TASK 2f --- ")
    pcc = task_2f(rt_rdd)
    print(f"PCC = {pcc}")

def export(spark_context, rt_rdd, output_dir, extension):
    for subtask in (task_2a, task_2b, task_2c, task_2d, task_2e, task_2f):
        results = subtask(rt_rdd)

        # Convert data to RDD
        results_rdd = utils.to_rdd(spark_context, results)

        # Parse RDDs to a format suitable for export
        results_rdd = results_rdd.map(utils.parse_row)
        
        # Write to file
        path = f"{output_dir}/{subtask.__name__}.{extension}"
        print(f"Writing to '{path}' ...")
        results_rdd.coalesce(1).saveAsTextFile(path)
        print(f"Done writing to '{path}'\n")
