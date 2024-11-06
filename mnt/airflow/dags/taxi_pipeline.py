from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("taxi_pipeline", start_date=datetime(2024, 11, 6), schedule_interval="@monthly", default_args=default_args, catchup=False) as dag:

    create_layer_storage = BashOperator(
        task_id="create_storage",
        bash_command="""
            hdfs dfs -mkdir -p /data_warehouse &&
            hdfs dfs -mkdir -p /raw
        """
    )

    download_and_put_hdfs = BashOperator(
        task_id='download_and_put_hdfs',
        bash_command="""
        # year={{ execution_date.strftime('%Y') }}
        # month={{ execution_date.strftime('%m') }}
        year=2024
        month=03
        hdfs dfs -mkdir -p /raw/green/${year}/${month}
        hdfs dfs -mkdir -p /raw/yellow/${year}/${month}

        # URLs for yellow and green taxi data
        yellow_url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${year}-${month}.parquet"
        green_url="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_${year}-${month}.parquet"
        
        check_url() {
            if curl --head --silent --fail "$1"; then
                echo "URL $1 exists."
            else
                echo "URL $1 does not exist. Exiting."
                exit 1
            fi
        }

        # Check if each URL exists
        check_url $yellow_url
        check_url $green_url

        wget -O /tmp/yellow_tripdata_${year}_${month}.parquet $yellow_url
        wget -O /tmp/green_tripdata_${year}_${month}.parquet $green_url

        hdfs dfs -put -f /tmp/yellow_tripdata_${year}_${month}.parquet /raw/yellow/${year}/${month}/yellow_tripdata_${year}-${month}.parquet
        hdfs dfs -put -f /tmp/green_tripdata_${year}_${month}.parquet /raw/green/${year}/${month}/green_tripdata_${year}-${month}.parquet

        # Optional: Remove temporary files
        rm /tmp/yellow_tripdata_${year}_${month}.parquet
        rm /tmp/green_tripdata_${year}_${month}.parquet

        """
    )

    creating_hive_database = HiveOperator(
        task_id="creating_hive_database",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE DATABASE IF NOT EXISTS warehouse_db;
        """
    )

    transform_and_load_task = SparkSubmitOperator(
        task_id="transform_and_load_task",
        application="/opt/airflow/dags/scripts/transform_to_silver.py",
        application_args=[
            # "{{ execution_date.strftime('%Y') }}",
            # "{{ execution_date.strftime('%m') }}"
            "2024","03"
        ],
        conn_id="spark_conn",
        executor_memory="6G",
        packages="org.postgresql:postgresql:42.2.23",
        verbose=False
    )

create_layer_storage >> download_and_put_hdfs >> creating_hive_database >> transform_and_load_task
