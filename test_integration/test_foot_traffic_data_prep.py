import dotenv
from snowflake.snowpark import Session, DataFrame
import snowflake.snowpark.types as T
import pytest
from foot_traffic_data_prep import *
import os
import pandas as pd


dotenv.load_dotenv()

@pytest.fixture(scope='module')
def session(request) -> Session:
    if request.config.getoption('--snowflake-session') == 'local':
        session =  Session.builder.config('local_testing', True).create()
        yield session
        session.close()
    else:
        import json
        with open('connection.json') as f:
             connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        yield session


@pytest.fixture(scope='module')
def raw_foot_traffic_data(request, session) -> DataFrame:
    if request.config.getoption('--snowflake-session') == 'local':
        raw_data_df = session.create_dataframe([
                                '{"country": "United States","foot_traffic_count_normalized": "0.00","number_of_locations_description": "LOW","state": "WI","timestamp": "2019-03-03 07:45:00","timestamp_interval": "15 minutes"}',
                                '{"country": "United States","foot_traffic_count_normalized": "0.00", "number_of_locations_description": "LOW","state": "WV","timestamp": "2019-03-03 07:45:00","timestamp_interval": "15 minutes"}',
                                 '{"country": "United States", "foot_traffic_count_normalized": "0.00", "number_of_locations_description": "MEDIUM", "state": "CA", "timestamp": "2019-03-03 08:15:00", "timestamp_interval": "15 minutes"}'
                                ],
                                schema=T.StructType([T.StructField("RAW", T.StringType())]))
        raw_data_df = raw_data_df.select(F.parse_json("RAW").alias("JSON_DATA"))
        # session.file.put("data/us_retail_foot_traffic.json", "@in_memory_stage", auto_compress=False)
        # raw_data_df = session.read.json("@in_memory_stage/us_retail_foot_traffic.json")
        # raw_data_df.copy_into_table(table_name='us_foot_traffic_local', target_columns=['JSON_DATA'], format_type_options={'STRIP_OUTER_ARRAY': True})
    else:
        raw_data_df = session.table('FOOT_TRAFFIC_JSON_DATA').sample(n=10000)

    return raw_data_df

# def test_valid_foot_traffic_timeseries(raw_foot_traffic_data):
#     assert valid_foot_traffic_timeseries(raw_foot_traffic_data)

def test_flatten_us_foot_traffic_data(raw_foot_traffic_data):
    df = flatten_us_foot_traffic_data(raw_foot_traffic_data)
    row = df.limit(1).collect()[0]
    row_as_dict = row.as_dict()
    assert ("JSON_DATA" not in row_as_dict.keys()) & ("TIMESTAMP" in row_as_dict.keys())

def test_prep_view_for_anomaly_detection(raw_foot_traffic_data):
    df1 = flatten_us_foot_traffic_data(raw_foot_traffic_data)
    df2 = prep_view_for_anomaly_detection(df1)
    rows = df2.select(F.col('STATE')).distinct().collect()

    assert len(rows) != 0
