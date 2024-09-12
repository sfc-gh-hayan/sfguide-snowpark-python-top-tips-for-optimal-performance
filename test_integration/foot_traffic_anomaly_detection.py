from snowflake.snowpark.session import Session
import foot_traffic_data_prep as foot_traffic_data_prep

def detect_abnormal_foot_traffic(session: Session, db:str, scm:str, src:str, dest:str) -> dict:

    try:
        session.use_database(db)
        session.use_schema(scm)

        us_foot_traffic_df = session.table(f'{src}')
        
        us_foot_traffic_flattened_df = foot_traffic_data_prep.flatten_us_foot_traffic_data(us_foot_traffic_df)
        
        prepped_foot_traffic_dataset = foot_traffic_data_prep.prep_view_for_anomaly_detection(us_foot_traffic_flattened_df)

        df_training, df_test = foot_traffic_data_prep.split_by_date(prepped_foot_traffic_dataset, '2020-12-31')

        df_training.write.mode('overwrite').save_as_table(table_name=f"tbl_foot_traffic_timeseries_training",
                                        mode='overwrite',
                                        table_type='temporary'
                                    )

        df_test.write.mode('overwrite').save_as_table(table_name=f"tbl_foot_traffic_timeseries_test",
                                        mode='overwrite',
                                        table_type='temporary'
                                    )

        session.sql(f'''
                        CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION abnormal_foot_traffic (
                        INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'tbl_foot_traffic_timeseries_training'),
                        SERIES_COLNAME => 'STATE',
                        TIMESTAMP_COLNAME => 'TIMESTAMP',
                        TARGET_COLNAME => 'FOOT_TRAFFIC_COUNT_NORMALIZED',
                        LABEL_COLNAME => ''
                        )
                ''').collect()
        
        session.sql(f''' 
                        CALL abnormal_foot_traffic!DETECT_ANOMALIES(
                            SERIES_COLNAME => 'STATE',
                            INPUT_DATA => SYSTEM$QUERY_REFERENCE('select * from tbl_foot_traffic_timeseries_test'),
                            TIMESTAMP_COLNAME =>'TIMESTAMP',
                            TARGET_COLNAME => 'FOOT_TRAFFIC_COUNT_NORMALIZED'
                        )
                ''').collect()
        
        session.sql(f'''
                    CREATE OR REPLACE TABLE {dest} 
                    AS 
                    SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                ''').collect()

        return {
            'error': False,
            'msg': f'Detected anomalies can be found in {dest}'
        }

    except Exception as e:
        return {
            'error': True,
            'msg': str(e)
        }