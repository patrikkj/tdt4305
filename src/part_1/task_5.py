from pyspark.sql import SQLContext, types
from pyspark.sql.functions import from_unixtime


def load_dataframes(sql_context, paths):
    """
    Loads each CSV file into separate dataframes.
    """
    _bt_struct = types.StructType([
        types.StructField("business_id", types.StringType(), True),
        types.StructField("name", types.StringType(), True),
        types.StructField("address", types.StringType(), True),
        types.StructField("city", types.StringType(), True),
        types.StructField("state", types.StringType(), True),
        types.StructField("postal_code", types.StringType(), True),
        types.StructField("latitude", types.FloatType(), True),
        types.StructField("longitude", types.FloatType(), True),
        types.StructField("stars", types.FloatType(), True),
        types.StructField("review_count", types.IntegerType(), True),
        types.StructField("categories", types.StringType(), True)
    ])

    _rt_struct = types.StructType([
        types.StructField("review_id", types.StringType(), True),
        types.StructField("user_id", types.StringType(), True),
        types.StructField("business_id", types.StringType(), True),
        types.StructField("review_text", types.StringType(), True),
        types.StructField("review_date", types.FloatType(), True)
    ])

    _fg_struct = types.StructType([
        types.StructField("src_user_id", types.StringType(), True),
        types.StructField("dst_user_id", types.StringType(), True)
    ])

    BT_PATH, RT_PATH, FG_PATH = paths
    bt_df = sql_context.read.csv(BT_PATH, schema=_bt_struct, sep='\t', header=True)
    rt_df = sql_context.read.csv(RT_PATH, schema=_rt_struct, sep='\t', header=True)
    fg_df = sql_context.read.csv(FG_PATH, schema=_fg_struct, sep=',', header=True)

    # Postprocessing
    rt_df = rt_df.withColumn("review_date", from_unixtime(rt_df.review_date))
    return bt_df, rt_df, fg_df

def task_5a(bt_df, rt_df, fg_df):
    """
    Prints the schema for each dataframe to easily verify that column names and types are correct.
    """
    print("Business table schema")
    bt_df.printSchema()
    bt_df.show()    
    
    print("\rReview table schema")
    rt_df.printSchema()    
    rt_df.show()    

    print("Friendship graph schema")
    fg_df.printSchema()    
    fg_df.show()


def print_subtasks():
    pass

def export_to_csv():
    pass