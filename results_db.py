import mysql.connector

def write_to_database(results):
    conn = mysql.connector.connect(
      user='andrewp',
      password='password', 
      host='localhost', 
      database='GX_DATABASE')
    cursor = conn.cursor()
    run_time = results['run_id']['run_time']
    run_name = results['run_id']['run_name']
    for validation in results['run_results'].values():
        evaluated_expectations = validation['validation_result']['statistics']['evaluated_expectations']
        successful_expectations = validation['validation_result']['statistics']['successful_expectations']
        suite_name = validation['validation_result']['meta']['expectation_suite_name']
        data_asset = validation['validation_result']['meta']['batch_spec']['data_asset_name']
        datasource = validation['validation_result']['meta']['active_batch_definition']['datasource_name']
        sql = f"INSERT INTO checkpoint_results(run_time, run_name, successful_expectations, evaluated_expectations, suite_name, datasource, data_asset) VALUES ('{run_time}', '{run_name}', {successful_expectations},{evaluated_expectations},'{suite_name}','{datasource}', '{data_asset}')"
        cursor.execute(sql)
        conn.commit()
    conn.close()
    return None

def query_success_rate(data_asset, begin_date, end_date):
    conn = mysql.connector.connect(
      user='andrewp',
      password='password', 
      host='localhost', 
      database='GX_DATABASE')
    cursor = conn.cursor()
    sql = f"SELECT ( successful_expectations / evaluated_expectations )  FROM checkpoint_results WHERE data_asset='{data_asset}' and run_time between '{begin_date}' AND '{end_date}'"
    print("EXECUTE", sql)
    cursor.execute(sql)
    result = cursor.fetchone()
    print(float(result[0]))
    return float(result[0])


