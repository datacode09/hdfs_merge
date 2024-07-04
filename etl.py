from pyspark.sql import SparkSession
import os
import shutil
from pathlib import Path
import tempfile
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_filesystem_manager(spark_context):
    FileSystem = spark_context._jvm.org.apache.hadoop.fs.FileSystem
    return FileSystem.get(spark_context._jsc.hadoopConfiguration())

def get_partition_dirs(fs, hdfs_path):
    return [d for d in fs.listStatus(Path(hdfs_path)) if d.getPath().getName().startswith("partition_date=")]

def get_existing_permissions(fs, path):
    status = fs.getFileStatus(Path(path))
    return status.getPermission().toString()

def create_temp_dir(fs, base_path, permissions):
    temp_dir = Path(base_path) / str(uuid.uuid4())
    fs.mkdirs(temp_dir)
    fs.setPermission(temp_dir, fs.Permission(permissions))
    return temp_dir

def move_files_to_temp(fs, files, temp_dir):
    for file in files:
        fs.rename(Path(file), temp_dir / Path(file).name)

def read_parquet_files(spark, files):
    return spark.read.parquet(*files)

def write_parquet_file(df, output_dir):
    df.coalesce(1).write.mode("overwrite").parquet(output_dir)

def get_hive_count(spark, query):
    return spark.sql(query).collect()[0][0]

def repair_table(spark, database, table):
    spark.sql(f"MSCK REPAIR TABLE {database}.{table}")

def delete_temp_dir(fs, temp_dir):
    fs.delete(temp_dir, True)

def restore_files(fs, files, temp_dir):
    for file in files:
        fs.rename(temp_dir / Path(file).name, Path(file))

def run_workflow(spark, fs, hdfs_paths_and_tables, temp_base_path):
    for hdfs_path, table_details in hdfs_paths_and_tables.items():
        partition_dirs = get_partition_dirs(fs, hdfs_path)

        for partition_dir in partition_dirs:
            partition_path = partition_dir.getPath().toString()
            try:
                # 1. Get existing permissions
                permissions = get_existing_permissions(fs, partition_path)

                # 2. Run hive query to get pre-count
                pre_count_query = f"SELECT COUNT(*) FROM {table_details['database']}.{table_details['table']} WHERE partition_date='{partition_path.split('=')[1]}'"
                pre_count = get_hive_count(spark, pre_count_query)
                logging.info(f"Pre-count for partition {partition_path}: {pre_count}")

                # 3. Create temp dir with the same permissions
                temp_dir = create_temp_dir(fs, temp_base_path, permissions)

                # 4. Get a list of existing Parquet files within the partition
                existing_parquet_files = [f.getPath().toString() for f in fs.listStatus(Path(partition_path)) if f.getPath().getName().endswith(".parquet")]

                # 5. Move existing Parquet files to temp dir
                move_files_to_temp(fs, existing_parquet_files, temp_dir)

                # 6. Read existing Parquet files into a Spark DataFrame
                df = read_parquet_files(spark, existing_parquet_files)

                # 7. Write the DataFrame into one Parquet file in the partition
                write_parquet_file(df, partition_path)

                # 8. Repair the table
                repair_table(spark, table_details['database'], table_details['table'])

                # 9. Run hive query to get post-count
                post_count = get_hive_count(spark, pre_count_query)
                logging.info(f"Post-count for partition {partition_path}: {post_count}")

                # 10. Check if counts match
                if pre_count == post_count:
                    delete_temp_dir(fs, temp_dir)
                    logging.info(f"Counts match for partition {partition_path}. Temp directory deleted.")
                else:
                    restore_files(fs, existing_parquet_files, temp_dir)
                    fs.delete(Path(partition_path) / "coalesced_parquet.parquet", True)
                    repair_table(spark, table_details['database'], table_details['table'])
                    logging.error(f"Counts do not match for partition {partition_path}. Restored original files and repaired the table.")

            except Exception as e:
                logging.error(f"Error processing partition {partition_path}: {e}")
                if 'temp_dir' in locals():
                    restore_files(fs, existing_parquet_files, temp_dir)
                    fs.delete(Path(partition_path) / "coalesced_parquet.parquet", True)
                    repair_table(spark, table_details['database'], table_details['table'])
                    logging.error(f"Exception occurred. Restored original files and repaired the table for partition {partition_path}.")
hdfs_paths_and_tables = {
    "hdfs://namenode:8020/path/to/partitioned/data1": {
        "database": "your_database1",
        "table": "your_table1"
    },
    "hdfs://namenode:8020/path/to/partitioned/data2": {
        "database": "your_database2",
        "table": "your_table2"
    }
}

temp_base_path = "hdfs://namenode:8020/path/to/temp/base"

def main():
    spark = ew.create_spark_session("CoalesceParquetFilesInPartitions")
    fs = ew.get_filesystem_manager(spark.sparkContext)

    ew.run_workflow(spark, fs, hdfs_paths_and_tables, temp_base_path)

    spark.stop()

if __name__ == "__main__":
    main()

