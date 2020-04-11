from airflow.contrib.sensors.file_sensor import FileSensor

is_forex_currencies_file_available = FileSensor(
        task_id = "is_forex_currencies_file_available",
        fs_conn_id = "forex_path",
        filepath = "forex_currencies.csv",
        poke_interval = 5,
        timeout=20
    )
    
# connection:

conn_id: forex_path
conn_type: File (path)
Extra: {"path":"/usr/local/airflow/dags/files"}

# comments:
path in Extra is the path in the docker container ID
