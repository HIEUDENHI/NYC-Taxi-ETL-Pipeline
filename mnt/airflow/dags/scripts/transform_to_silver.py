import sys
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types
def main(year,month):
# Initialize Spark session
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("Jupyter-Spark") \
        .enableHiveSupport() \
        .getOrCreate()


    df_green = spark.read.parquet(f'/raw/green/{year}/{month}/green_tripdata_{year}-{month}.parquet')
    df_yellow = spark.read.parquet(f'/raw/yellow/{year}/{month}/yellow_tripdata_{year}-{month}.parquet')
    
   

    # Rename datetime columns for consistency
    df_green = df_green \
        .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

    df_yellow = df_yellow \
        .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

    common_columns = [
        'VendorID',
        'pickup_datetime',
        'dropoff_datetime',
        'store_and_fwd_flag',
        'RatecodeID',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'payment_type',
        'congestion_surcharge'
    ]
    # Select common columns and add service_type
    df_green_sel = df_green \
        .select(common_columns) \
        .withColumn('service_type', F.lit('green'))

    df_yellow_sel = df_yellow \
        .select(common_columns) \
        .withColumn('service_type', F.lit('yellow'))

    # # Combine the data
    df_trips_data = df_green_sel.unionByName(df_yellow_sel)
  

    df_trips_data.write.option("path", "/warehouse/") \
                        .mode("append").saveAsTable("warehouse_db.trip_data")
    

    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark_job.py <year> <month>")
        sys.exit(-1)
    
    year = sys.argv[1]
    month = sys.argv[2]
    
    main(year, month)