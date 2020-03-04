from .. import utils


# Wheter to crop the output of subtask 6a due to its size
# The full file is about 1.2 GB, making uploading our 
# solutions to BlackBoard impractical.
CROP_VERY_LARGE_FILES = True

# Subtask 6a output formats:
# IF TRUE - '.txt' file of the format:
#       Inner join of 'review table' and 'business table' on 'business_id'
#       Number of rows: 883737
#       {PRINT OF TOP 20 ROWS WITH COLUMN HEADERS}
# IF FALSE - '.csv' file with the full inner joined table (about 1.2 GB):


def task_6a(bt_df, rt_df, fg_df):
    """
    Inner join review table and business table on business_id column.
    """
    return rt_df.join(bt_df, "business_id")

def task_6b(sql_context, bt_df, rt_df, fg_df):
    """
    Save the new table in a temporary table.
    """
    df = task_6a(bt_df, rt_df, fg_df)
    output = [
        "SqlContext.tables() before execution",
        sql_context.tables()._jdf.showString(20, 20, False),
    ]

    # Create temporary table
    sql_context.registerDataFrameAsTable(df, "temp_table")

    output.extend([
        "\nSqlContext.tables() after execution",
        sql_context.tables()._jdf.showString(20, 20, False),
    ])
    return '\n'.join(output)

def task_6c(bt_df, rt_df, fg_df):
    """
    Find the number of reviews for each user in the review table for 
    top 20 users with the most number of reviews sorted descendingly.
    """
    top_users = rt_df.groupBy(rt_df.user_id).count().sort('count', ascending=False).take(20)
    return list(map(lambda row: (row['user_id'], row['count']), top_users))

def run(sql_context, bt_df, rt_df, fg_df):
    print("\n --- TASK 6a --- ")
    print("Inner join of BT and RT")
    task_6a(bt_df, rt_df, fg_df).show()

    print("\n --- TASK 6b --- ")
    print(task_6b(sql_context, bt_df, rt_df, fg_df))

    print("\n --- TASK 6c --- ")
    print("Get top 20 users by review count")
    top_users = task_6c(bt_df, rt_df, fg_df)
    print(f"top_users = {top_users}")

def export(spark_context, sql_context, bt_df, rt_df, fg_df, output_dir, extension):
    results_6a = task_6a(bt_df, rt_df, fg_df)
    results_6b = task_6b(sql_context, bt_df, rt_df, fg_df)
    results_6c = task_6c(bt_df, rt_df, fg_df)

    if CROP_VERY_LARGE_FILES:
        results_6a = '\n'.join([
            "Inner join of 'review table' and 'business table' on 'business_id'\n",
            "NOTE: The output of this subtask is cropped due to its size (about 1.2 GB).",
            "      To export the entire file as '.csv', set CROP_VERY_LARGE_FILES in './tdt4305/part1/task_6.py' to False.\n",
            f"Number of rows: {results_6a.count()}",
            results_6a._jdf.showString(20, 20, False)
        ])
        path_6a = f"{output_dir}/task_6a.txt"
        print(f"Writing to '{path_6a}' ...")
        with open(path_6a, "w") as text_file:
            print(results_6a, file=text_file)
        print(f"Done writing to '{path_6a}'\n")
    else:
        path_6a = f"{output_dir}/task_6a.{extension}"
        print(f"Writing to '{path_6a}' ...")
        results_6a.coalesce(1).write.csv(path_6a)
        print(f"Done writing to '{path_6a}'\n")

    path_6b = f"{output_dir}/task_6b.txt"
    print(f"Writing to '{path_6b}' ...")
    with open(path_6b, "w") as text_file:
        print(results_6b, file=text_file)
    print(f"Done writing to '{path_6b}'\n")

    # Convert data to RDD for easy export
    results_6c_rdd = utils.to_rdd(spark_context, results_6c)
    results_6c_rdd = results_6c_rdd.map(utils.parse_row)

    path_6c = f"{output_dir}/task_6c.{extension}"
    print(f"Writing to '{path_6c}' ...")
    results_6c_rdd.coalesce(1).saveAsTextFile(path_6c)
    print(f"Done writing to '{path_6c}'\n")