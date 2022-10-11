""" Script that runs all parts of the pipeline
This is for demonstation purposes. In a production setting,
the steps could instead be e.g. executed by an Airflow pipeline that
uses PythonExecutors calling the functions separately
"""
from argparse import ArgumentParser
import logging
from pathlib import Path
import sys
from typing import Union

from pyspark.sql import DataFrame, SparkSession

from ingestion.extract import (
    read_raw_data,
    parse_json_data,
    flatten_structs,
)
from ingestion.transform import (
    EMPTY_COLS,
    remove_columns,
    parse_message_field,
)

from ingestion.spark_queries import SparkSQLQueries

import ingestion.load as load

logger = logging.getLogger()


def extract_raw_data(spark: SparkSession, path: Union[str, Path]) -> DataFrame:
    """The extract step, loading and flattening the raw data to Spark

    Args:
        spark (SparkSession): Needs to be initialized before and passed
        path (Union[str, Path]): Path to raw data file

    Returns:
        DataFrame: Raw data in columnar format
    """
    df = read_raw_data(spark, path)
    logger.info("Parsing JSON data")
    df = parse_json_data(df)
    logger.info("Flattening data")
    df = flatten_structs(df)

    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Applying transformations like cleaning and parsing additional fields

    Args:
        df (DataFrame):

    Returns:
        DataFrame:
    """
    logger.info("Removing columns")
    df = remove_columns(df, EMPTY_COLS)

    logger.info("Parsing message field")
    df = parse_message_field(df)

    return df


def load_data(sparkqueries: SparkSQLQueries, create_dbs=True, create_dim_tables=True):
    if create_dbs:
        load.create_databases(sparkqueries)

    if create_dim_tables:
        load.create_dimension_tables(sparkqueries)

    load.insert_rawdata(sparkqueries)
    load.insert_dimensions(sparkqueries)
    load.create_fact_table(sparkqueries)
    load.insert_fact_table(sparkqueries)


def run(args):
    logger.info("Creating SparkSession")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    logger.info("Starting raw data processing")
    df = extract_raw_data(spark, args.input_data_path)

    logger.info("Starting transformation")
    df = transform_data(df)

    logger.info("Creating SparkSQLQueries")
    sparkqueries = SparkSQLQueries(spark, args.warehouse_path, df)

    logger.info("Loading into data warehouse")
    load_data(sparkqueries)

    spark.sql('SELECT * from dw.fact_kafka_logs').show()
    return spark


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Parsing input arguments")

    parser = ArgumentParser()
    parser.add_argument("input_data_path", type=Path, help="Path to raw data")
    parser.add_argument("warehouse_path", type=Path, help="Path to Spark DW")

    args = parser.parse_args()

    run(args)
