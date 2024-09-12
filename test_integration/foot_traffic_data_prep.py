from snowflake.snowpark import DataFrame
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
import json

# def valid_foot_traffic_timeseries(df: DataFrame):

#     # ## The below logic will fail because raw data has only one column 
#     # # current_columns = set(df.columns)
    
#     # ## The below logic might work
#     # row1 = df.limit(1).collect()[0]
#     # foot_traffic_json_dict = row1.as_dict()
#     # nested_json_str = foot_traffic_json_dict['JSON_DATA']
#     # nested_json = json.loads(nested_json_str)
#     # print(nested_json)
#     # # current_columns = set(nested_json.keys())

#     # minimum_expected_columns = set(['foot_traffic_count_normalized', 'state', 'timestamp'])
#     # # minimum_expected_columns.issubset(current_columns)
#     return True

def flatten_us_foot_traffic_data(df: DataFrame):
    return df\
            .with_column('country', F.cast(df.JSON_DATA['country'], T.StringType()))\
            .with_column('foot_traffic_count_normalized', F.cast(df.JSON_DATA['foot_traffic_count_normalized'], T.DoubleType()))\
            .with_column('number_of_locations_description', F.cast(df.JSON_DATA['number_of_locations_description'], T.StringType()) )\
            .with_column('state', F.cast(df.JSON_DATA['state'], T.StringType()))\
            .with_column('timestamp',  F.cast(df.JSON_DATA['timestamp'], T.TimestampType()))\
            .with_column('timestamp_interval', F.cast(df.JSON_DATA['timestamp_interval'], T.StringType()) )\
            .drop('JSON_DATA')

def prep_view_for_anomaly_detection(df: DataFrame):
    return df.select(['foot_traffic_count_normalized', 'state', 'timestamp']).order_by(F.col('timestamp'), ascending=True)
    

def split_by_date(df: DataFrame, cutoff_date: str):
    return (
        df.filter(F.to_date(F.col('TIMESTAMP')) <= F.to_date(F.lit(cutoff_date))),
        df.filter(F.to_date(F.col('TIMESTAMP')) > F.to_date(F.lit(cutoff_date))),
    )