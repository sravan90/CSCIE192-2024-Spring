from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
import pyspark.sql.functions as f
from utils import parse_commandline, read_input_csv, write_output_csv


# data transformation function
def transform_data(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    rows_df=rows_df.withColumn("Total_fees",
                   f.col("extra") +
                   f.col("mta_tax") +
                   f.col("improvement_surcharge") +
                   f.col("congestion_surcharge")  )
    result_df = rows_df.groupBy('VendorID').agg(f.max('Fare_amount') , f.max('Total_fees') , f.avg('Fare_amount'))

    return result_df
def transform_data_pickup(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    pickup_count = rows_df.groupBy(f.col("PULocationID")).count().alias("pickup_count")
    #top_10_pickup = pickup_count.orderBy(f.desc("count")).limit(10)

    return pickup_count

def transform_data_pickup_top10(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    pickup_count = rows_df.groupBy(f.col("PULocationID")).count().alias("pickup_count")
    top_10_pickup = pickup_count.orderBy(f.desc("count")).limit(10)

    return top_10_pickup

def transform_data_dropoff(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    drop_count = rows_df.groupBy(f.col("DOLocationID")).count().alias("drop_count")
    #top_10_drop = drop_count.orderBy(f.desc("count")).limit(10)

    return drop_count

def transform_data_dropoff_top10(rows_df: SparkDataFrame, **kvargs) -> SparkDataFrame:
    drop_count = rows_df.groupBy(f.col("DOLocationID")).count().alias("drop_count")
    top_10_drop = drop_count.orderBy(f.desc("count")).limit(10)

    return top_10_drop
# spark job pipeline
def run_pipeline(input_path:str, output_path:str) -> None:
    # 1. create spark session
    spark = SparkSession.builder \
        .appName("BatchTransformTaxi") \
        .getOrCreate()
 #       .config('spark.local.dir', '/Users/ESumitra/tmp/spark-localdir') \

    try:
        input_df = read_input_csv(spark, input_path) # 2. read input DataFrame
        result_df = transform_data(input_df)         # 3. transform data
        write_output_csv(result_df, output_path)     # 4. write output from DataFrame
        pickup_df = transform_data_pickup(input_df)
        write_output_csv(pickup_df,output_path)
        pickup_df_top10 = transform_data_pickup_top10(input_df)
        write_output_csv(pickup_df_top10, output_path)
        drop_df = transform_data_dropoff(input_df)
        write_output_csv(drop_df, output_path)
        drop_df_top10=transform_data_dropoff_top10(input_df)
        write_output_csv(drop_df_top10, output_path)
    except Exception as e:
        print(f'error running pipeline\n{e}')
    finally:
        spark.stop()

# main entry point
if __name__ == "__main__":
    """
        Usage: 
        ./bin/spark-submit $SCRIPT_PATH/batch_transform_taxi.py --input_path $INPUT_PATH/taxi_tripdata_sample.csv --output_path $OUTPUT_PATH
    """
    args = parse_commandline()
    run_pipeline('/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/input/taxi_tripdata.csv'
                 ,'/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/output' )



