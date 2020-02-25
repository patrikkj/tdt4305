from datetime import datetime


def task_2a(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the number of distinct users in 'review table'.
    """
    user_ids = rt_rdd.map(lambda row: row[1])
    unique_user_count = user_ids.distinct().count()
    return unique_user_count

def task_2b(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the average review length.
    """
    reviews_length = rt_rdd.map(lambda row: len(row[3]))
    average_review_length = reviews_length.sum() / reviews_length.count()
    return average_review_length

def task_2c(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the top 10 businesses by number of reviews.
    """
    rt_grouped = rt_rdd.groupBy(lambda row: row[2]) 
    rt_count_per_id = rt_grouped.mapValues(len)
    rt_sorted = rt_count_per_id.sortBy(lambda row: row[1])
    top_businesses = rt_sorted.map(lambda row: row[0]).take(10)
    return top_businesses

def task_2d(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the number of reviews per year.
    """
    rt_grouped_by_year = rt_rdd.groupBy(lambda row: datetime.fromtimestamp(int(float(row[4]))).year)
    rt_reviews_per_year = rt_grouped_by_year.mapValues(len)
    return sorted(rt_reviews_per_year.collect())

def task_2e(bt_rdd, rt_rdd, fg_rdd):
    """
    Returns the time and date for the first and last review.
    """
    rt_parsed = rt_rdd.map(lambda row: int(float(row[4])))
    datetime_first = datetime.fromtimestamp(rt_parsed.min())
    datetime_last = datetime.fromtimestamp(rt_parsed.max())
    return datetime_first, datetime_last

def task_2f(bt_rdd, rt_rdd, fg_rdd):
    """
    Calculates the 'Pearson correlation coefficient' between the number of reviews 
    by a user and the average number of the characters in the user’s reviews.
    """
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


def print_subtasks():
    pass

def export_to_csv():
    pass