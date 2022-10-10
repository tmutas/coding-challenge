"""Functions to handle extracting and preparing raw data"""
from pathlib import Path
from typing import Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import schema_of_json, from_json, from_unixtime


def load_raw_data(
    spark_session: SparkSession, file_path: Union[Path, str]
) -> DataFrame:
    """Load the raw (multline) CSV data into Spark DataFrame

    Args:
        file_path (Union[Path, str]): Path to the CSV file

    Returns:
        DataFrame:
    """

    raw_df = spark_session.read.csv(
        str(file_path),
        header=True,
        escape='"',
        multiLine=True,
    )

    return raw_df


def parse_json_data(df: DataFrame) -> DataFrame:
    """Parse JSON contents into StructType

    Args:
        df (DataFrame): DataFrame with columns containing JSON string

    Returns:
        DataFrame: JSON contents as StructType
    """
    json_options = {"multiLine": "true"}
    for column_name in df.columns:
        # Taking the first value as schema for the column
        json_sample = df.select(column_name).limit(1).collect()[0][column_name]

        # Parsing the JSON content into a struct
        df = df.withColumn(
            column_name,
            from_json(
                column_name, schema_of_json(json_sample, json_options), json_options
            ),
        )

    return df


def flatten_structs(df: DataFrame) -> DataFrame:
    """Flatten the struct columns. Assumes all columns are structs

    Args:
        df (DataFrame):

    Returns:
        DataFrame:
    """
    # Need to unpack the iterator f
    df = df.select([f"{column_name}.*" for column_name in df.columns])

    # There was one more struct with a single field to unpack in the metadata
    df = df.withColumn("host", df.colRegex("key.host")).drop("key")
    return df
