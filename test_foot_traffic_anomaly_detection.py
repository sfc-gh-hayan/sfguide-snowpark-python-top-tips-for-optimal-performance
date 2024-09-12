import dotenv
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.row import Row
import pytest
import os
import pandas as pd
from foot_traffic_data_prep import *
from foot_traffic_anomaly_detection import *
from unittest import mock
from functools import partial
import datetime



dotenv.load_dotenv()

@pytest.fixture(scope='module')
def session(request) -> Session:
    if request.config.getoption('--snowflake-session') == 'local':
        session = Session.builder.config('local_testing', True).create()
        yield session
        session.close()
    else:
        import json
        with open('connection.json') as f:
             connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        session.sql(f'ALTER WAREHOUSE {session.get_current_warehouse()} SET WAREHOUSE_SIZE=MEDIUM').collect()

        yield session

        session.sql(f'ALTER WAREHOUSE {session.get_current_warehouse()} SET WAREHOUSE_SIZE=XSMALL').collect()
        session.close()


@pytest.fixture(scope='module')
def raw_foot_traffic_data(request, session) -> DataFrame:
    if request.config.getoption('--snowflake-session') == 'local':
        # session.file.put("data/us_retail_foot_traffic.json", "@in_memory_stage", auto_compress=False)
        # return session.read.json("@in_memory_stage/us_retail_foot_traffic.json")
        raw_data_df = session.create_dataframe([
                                '{"country": "United States","foot_traffic_count_normalized": "0.00","number_of_locations_description": "LOW","state": "WI","timestamp": "2019-03-03 07:45:00","timestamp_interval": "15 minutes"}',
                                '{"country": "United States","foot_traffic_count_normalized": "0.00", "number_of_locations_description": "LOW","state": "WV","timestamp": "2019-03-03 07:45:00","timestamp_interval": "15 minutes"}',
                                 '{"country": "United States", "foot_traffic_count_normalized": "0.00", "number_of_locations_description": "MEDIUM", "state": "CA", "timestamp": "2019-03-03 08:15:00", "timestamp_interval": "15 minutes"}'
                                ],
                                schema=T.StructType([T.StructField("RAW", T.StringType())]))
        raw_data_df = raw_data_df.select(F.parse_json("RAW").alias("JSON_DATA"))
    else: 
        return session.table('FOOT_TRAFFIC_JSON_DATA')
@pytest.mark.skipif(
    condition="config.getvalue('--snowflake-session') == 'local'",
    reason="Test skipped because it contains ANOMALY_DETECTION class which is only available on server side"
)
def test_detect_abnormal_foot_traffic(session, raw_foot_traffic_data):
    raw_foot_traffic_data.write.mode('overwrite').save_as_table('FOOT_TRAFFIC_JSON_DATA_TEMP', table_type='temporary')

    # Run against complete data within a Snowflake table

    res = detect_abnormal_foot_traffic(session=session
                            ,db=session.get_current_database()
                            ,scm=session.get_current_schema()
                            ,src='FOOT_TRAFFIC_JSON_DATA_TEMP'
                            ,dest='us_foot_traffic_anomalies'
                            )


    assert not res.get('error')
