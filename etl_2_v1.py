from pyspark.sql import SparkSession
import json
import os
import sys
import logging
import traceback
import argparse
import unittest

def setup_logging(log_dir, mode):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, f'{mode}_mode.log')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        logging.info(f"Config loaded from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def get_directories(hdfs_path):
    try:
        cmd = f"hdfs dfs -ls {hdfs_path}"
        result = os.popen(cmd).read()
        directories = [line.split()[-1] for line in result.strip().split('\n') if line.startswith('d')]
        logging.info(f"Found directories in {hdfs_path}: {directories}")
        return directories
    except Exception as e:
        logging.error(f"Failed to list directories in {hdfs_path}: {e}")
        logging.error(traceback.format_exc())
        return []

def process_directory(spark, dir_path):
    try:
        df = spark.read.parquet(dir_path)
        temp_path = dir_path + "_temp"
        repartition_and_save(df, temp_path)
        overwrite_original_directory(dir_path, temp_path)
    except Exception as e:
        logging.error(f"Failed to process directory {dir_path}: {e}")
        logging.error(traceback.format_exc())

def repartition_and_save(df, temp_path):
    try:
        df.coalesce(1).write.mode("overwrite").parquet(temp_path)
        logging.info(f"Data coalesced and saved to temporary path {temp_path}")
    except Exception as e:
        logging.error(f"Failed to repartition and save data to {temp_path}: {e}")
        logging.error(traceback.format_exc())

def overwrite_original_directory(original_path, temp_path):
    try:
        os.system(f"hdfs dfs -rm -r {original_path}")
        os.system(f"hdfs dfs -mv {temp_path} {original_path}")
        logging.info(f"Overwritten original directory {original_path} with {temp_path}")
    except Exception as e:
        logging.error(f"Failed to overwrite original directory {original_path} with {temp_path}: {e}")
        logging.error(traceback.format_exc())

def create_dummy_parquet(spark, path):
    data = [("John", 30), ("Jane", 28), ("Jake", 35), ("Jill", 22)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    try:
        df.repartition(4).write.mode("overwrite").parquet(path)
        logging.info(f"Dummy parquet file created at {path}")
    except Exception as e:
        logging.error(f"Failed to create dummy parquet file at {path}: {e}")
        logging.error(traceback.format_exc())

def create_test_directories(spark, base_path, num_dirs):
    try:
        for i in range(num_dirs):
            dir_path = os.path.join(base_path, f"dummy_dir_{i}")
            create_dummy_parquet(spark, dir_path)
        logging.info(f"Created {num_dirs} dummy directories at {base_path}")
    except Exception as e:
        logging.error(f"Failed to create test directories at {base_path}: {e}")
        logging.error(traceback.format_exc())

def process_hdfs_path(spark, hdfs_path):
    directories = get_directories(hdfs_path)
    for dir_path in directories:
        process_directory(spark, dir_path)

def main(config_path, mode, test_base_path=None, num_test_dirs=3):
    script_path = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_path, 'logs')
    setup_logging(log_dir, mode)

    spark = SparkSession.builder.appName("HDFS Parquet Processor").getOrCreate()
    logging.info("Spark session created")

    config = load_config(config_path)
    hdfs_paths = config.get('hdfs_paths', [])

    try:
        if mode == 'test':
            if not test_base_path:
                logging.error("Test base path must be provided in test mode.")
                sys.exit(1)
            create_test_directories(spark, test_base_path, num_test_dirs)
            process_hdfs_path(spark, test_base_path)
            # Run unit tests in test mode
            run_tests()
        else:
            for hdfs_path in hdfs_paths:
                process_hdfs_path(spark, hdfs_path)
    except Exception as e:
        logging.error(f"Error occurred in main processing: {e}")
        logging.error(traceback.format_exc())
    finally:
        spark.stop()
        logging.info("Spark session stopped")

def run_tests():
    class TestHDFSParquetProcessor(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.spark = SparkSession.builder.appName("Test HDFS Parquet Processor").getOrCreate()
            cls.tmp_path = os.path.join(os.getcwd(), "tmp")

        @classmethod
        def tearDownClass(cls):
            cls.spark.stop()
            if os.path.exists(cls.tmp_path):
                for root, dirs, files in os.walk(cls.tmp_path, topdown=False):
                    for name in files:
                        os.remove(os.path.join(root, name))
                    for name in dirs:
                        os.rmdir(os.path.join(root, name))
                os.rmdir(cls.tmp_path)

        def setUp(self):
            if not os.path.exists(self.tmp_path):
                os.makedirs(self.tmp_path)

        def tearDown(self):
            for root, dirs, files in os.walk(self.tmp_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))

        def test_load_config(self):
            test_load_config()

        def test_get_directories(self):
            test_get_directories(self.mocker)

        def test_process_directory(self):
            test_process_directory(self.spark, self.tmp_path)

        def test_repartition_and_save(self):
            test_repartition_and_save(self.spark, self.tmp_path)

        def test_overwrite_original_directory(self):
            test_overwrite_original_directory(self.mocker, self.tmp_path)

        def test_create_dummy_parquet(self):
            test_create_dummy_parquet(self.spark, self.tmp_path)

        def test_create_test_directories(self):
            test_create_test_directories(self.spark, self.tmp_path)

    unittest.main(argv=[''], exit=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HDFS Parquet Processor")
    parser.add_argument('mode', choices=['prod', 'test'], help="Mode to run the script in")
    parser.add_argument('--config', required=True, help="Path to the configuration file")
    parser.add_argument('--test_base_path', help="Base path for creating test directories (required for test mode)")
    parser.add_argument('--num_test_dirs', type=int, default=3, help="Number of test directories to create (default: 3)")

    args = parser.parse_args()

    main(args.config, args.mode, args.test_base_path, args.num_test_dirs)
