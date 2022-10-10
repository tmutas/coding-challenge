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
    load_raw_data,
    parse_json_data,
    flatten_structs,
)
from ingestion.transform import (
    EMPTY_COLS,
    remove_columns,
    parse_message_field,
)

logger = logging.getLogger()


def extract_raw_data(spark: SparkSession, path: Union[str, Path]) -> DataFrame:
    """The extract step, loading and flattening the raw data to Spark

    Args:
        spark (SparkSession): Needs to be initialized before and passed
        path (Union[str, Path]): Path to raw data file

    Returns:
        DataFrame: Raw data in columnar format
    """
    df = load_raw_data(spark, path)
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


def run(args):
    logger.info("Creating SparkSession")
    spark = SparkSession.builder.getOrCreate()

    logger.info("Starting raw data processing")
    df = extract_raw_data(spark, args.input_data_path)
    df = transform_data(df)

    df.show(10)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info("Parsing input arguments")

    parser = ArgumentParser()
    parser.add_argument("input_data_path", type=Path)
    args = parser.parse_args()

    run(args)
