from pyspark.sql import SparkSession
import uuid
import logging
from py4j.java_gateway import java_import

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()

def get_filesystem_manager(spark_context):
    java_import(spark_context._gateway.jvm, 'org.apache.hadoop.fs.Path')
    FileSystem = spark_context._jvm.org.apache.hadoop.fs.FileSystem
    return FileSystem.get(spark_context._jsc.hadoopConfiguration())

def get_partition_dirs(fs, hdfs_path):
    partition_dirs = []
    for status in fs.listStatus(spark_context._gateway.jvm.Path(hdfs_path)):
        if status.getPath().getName().startswith("partition_date="):
            partition_dirs.append(status.getPath().toString())
    return partition_dirs

def get_existing_permissions(fs, path):
    status = fs.getFileStatus(spark_context._gateway.jvm.Path(path))
    return status.getPermission().toString(), status.getOwner(), status.getGroup()

def create_temp_dir(fs, base_path, permissions, owner, group):
    temp_dir_name = str(uuid.uuid4())
    temp_dir = base_path + "/" + temp_dir_name
    hadoop_temp_dir = spark_context._gateway.jvm.Path(temp_dir)
    fs.mkdirs(hadoop_temp_dir)
    fs.setPermission(hadoop_temp_dir, fs.Permission(permissions))
    fs.setOwner(hadoop_temp_dir, owner, group)
    return hadoop_temp_dir

def move_files_to_temp(fs, files, temp_dir):
    for file in files:
        fs.rename(spark_context._gateway.jvm.Path(file), temp_dir)

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
        fs.rename(temp_dir, spark_context._gateway.jvm.Path(file))

def run_workflow(spark, fs, hdfs_paths_and_tables, temp_base_path):
    for hdfs_path, table_details in hdfs_paths_and_tables.items():
        partition_dirs = get_partition_dirs(fs, hdfs_path)

        for partition_dir in partition_dirs:
            partition_path = partition_dir
            try:
                # 1. Get existing permissions
                permissions, owner, group = get_existing_permissions(fs, partition_path)

                # 2. Run hive query to get pre-count
                partition_value = partition_path.split('=')[-1]
                pre_count_query = f"SELECT COUNT(*) FROM {table_details['database']}.{table_details['table']} WHERE partition_date='{partition_value}'"
                pre_count = get_hive_count(spark, pre_count_query)
                logging.info(f"Pre-count for partition {partition_path}: {pre_count}")

                # 3. Create temp dir with the same permissions
                temp_dir = create_temp_dir(fs, temp_base_path, permissions, owner, group)

                # 4. Get a list of existing Parquet files within the partition
                existing_parquet_files = [f.getPath().toString() for f in fs.listStatus(spark_context._gateway.jvm.Path(partition_path)) if f.getPath().getName().endswith(".parquet")]

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
                    fs.delete(spark_context._gateway.jvm.Path(partition_path) / "coalesced_parquet.parquet", True)
                    repair_table(spark, table_details['database'], table_details['table'])
                    logging.error(f"Counts do not match for partition {partition_path}. Restored original files and repaired the table.")

            except Exception as e:
                logging.error(f"Error processing partition {partition_path}: {e}")
                if 'temp_dir' in locals():
                    restore_files(fs, existing_parquet_files, temp_dir)
                    fs.delete(spark_context._gateway.jvm.Path(partition_path) / "coalesced_parquet.parquet", True)
                    repair_table(spark, table_details['database'], table_details['table'])
                    logging.error(f"Exception occurred. Restored original files and repaired the table for partition {partition_path}.")
