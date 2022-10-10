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


logger = logging.getLogger()


def extract_raw_data(spark: SparkSession, path: Union[str, Path]) -> DataFrame:
    df = load_raw_data(spark, path)
    logger.info("Parsing JSON data")
    df = parse_json_data(df)
    logger.info("Flattening data")
    df = flatten_structs(df)

    return df



def run(args):
    logger.info("Creating SparkSession")
    spark = SparkSession.builder.getOrCreate()

    logger.info("Starting raw data processing")
    df = extract_raw_data(spark, args.input_data_path)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.info("Parsing input arguments")
    parser = ArgumentParser()
    parser.add_argument("input_data_path", type=Path)
    args = parser.parse_args()

    run(args)
