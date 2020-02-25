
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
    
    print("Tables 1")
    sql_context.tables().show()
    sql_context.registerDataFrameAsTable(df, "temp_table")

    print("Tables 2")
    sql_context.tables().show()

def task_6c(bt_df, rt_df, fg_df):
    """
    Find the number of reviews for each user in the review table for 
    top 20 users with the most number of reviews sorted descendingly.
    """
    top_users = rt_df.groupBy(rt_df.user_id).count().sort('count', ascending=False).take(20)
    return list(map(lambda row: (row['user_id'], row['count']), top_users))


def print_subtasks():
    pass

def export_to_csv():
    pass