from pyspark.sql import SparkSession
import logging
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

def get_filesystem_manager(spark_context):
    java_import(spark_context._gateway.jvm, 'org.apache.hadoop.fs.Path')
    java_import(spark_context._gateway.jvm, 'org.apache.hadoop.fs.permission.FsPermission')
    FileSystem = spark_context._jvm.org.apache.hadoop.fs.FileSystem
    return FileSystem.get(spark_context._jsc.hadoopConfiguration())

def read_parquet_files(spark, files):
    return spark.read.parquet(*files)

def write_parquet_file(df, output_dir):
    df.coalesce(1).write.mode("overwrite").parquet(output_dir)

def get_hive_count(spark, database, table, partition_key, partition_value):
    query = f"SELECT COUNT(*) FROM {database}.{table} WHERE {partition_key}='{partition_value}'"
    return spark.sql(query).collect()[0][0]

def repair_table(spark, database, table):
    spark.sql(f"MSCK REPAIR TABLE {database}.{table}")

def delete_old_files(fs, spark_context, files):
    for file in files:
        fs.delete(spark_context._gateway.jvm.Path(file), True)

def rename_file(fs, spark_context, src, dst):
    fs.rename(spark_context._gateway.jvm.Path(src), spark_context._gateway.jvm.Path(dst))

def process_single_partition(spark, fs, database, table, partition_key, partition_value, partition_path):
    spark_context = spark.sparkContext

    try:
        # Run hive query to get pre-count
        pre_count = get_hive_count(spark, database, table, partition_key, partition_value)
        logging.info(f"Pre-count for partition {partition_path}: {pre_count}")

        # Get a list of existing Parquet files within the partition
        existing_parquet_files = [f.getPath().toString() for f in fs.listStatus(spark_context._gateway.jvm.Path(partition_path)) if f.getPath().getName().endswith(".parquet")]

        # Read existing Parquet files into a Spark DataFrame
        df = read_parquet_files(spark, existing_parquet_files)

        # Write the DataFrame into one Parquet file in the partition
        temp_output_dir = partition_path + "/coalesced_temp"
        write_parquet_file(df, temp_output_dir)

        # Get the path of the coalesced Parquet file
        temp_parquet_file = [f.getPath().toString() for f in fs.listStatus(spark_context._gateway.jvm.Path(temp_output_dir)) if f.getPath().getName().endswith(".parquet")][0]
        final_parquet_file = partition_path + "/coalesced_parquet.parquet"

        # Delete the older Parquet files
        delete_old_files(fs, spark_context, existing_parquet_files)

        # Rename the coalesced Parquet file
        rename_file(fs, spark_context, temp_parquet_file, final_parquet_file)
        fs.delete(spark_context._gateway.jvm.Path(temp_output_dir), True)

        # Repair the table
        repair_table(spark, database, table)

        # Run hive query to get post-count
        post_count = get_hive_count(spark, database, table, partition_key, partition_value)
        logging.info(f"Post-count for partition {partition_path}: {post_count}")

        # Check if counts match
        if pre_count == post_count:
            logging.info(f"Counts match for partition {partition_path}.")
        else:
            logging.error(f"Counts do not match for partition {partition_path}.")
            # Handle error: Rollback if necessary (requires implementing rollback logic)

    except Exception as e:
        logging.error(f"Error processing partition {partition_path}: {e}")

def main():
    spark = create_spark_session("TestSingleTablePartition")
    fs = get_filesystem_manager(spark.sparkContext)

    # Test parameters for a single table and partition
    database = "your_database"
    table = "your_table"
    partition_key = "partition_date"
    partition_value = "2024-01-01"
    partition_path = f"hdfs://namenode:8020/path/to/partitioned/data/{partition_key}={partition_value}"

    process_single_partition(spark, fs, database, table, partition_key, partition_value, partition_path)

    spark.stop()

if __name__ == "__main__":
    main()
