from pathlib import Path
from typing import Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import schema_of_json, from_json, from_unixtime

spark = SparkSession.builder.getOrCreate()

def load_raw_data(file_path : Union[Path, str]) -> DataFrame:
    
    raw_df = spark.read.csv(
        str(file_path),
        header=True,
        escape='"',
        multiLine=True,
    )

    return raw_df

def parse_json_data(df : DataFrame) -> DataFrame:
    json_options = {"multiLine" : "true"}
    for col in df.columns:
        # Taking the first value as schema for the column
        json_sample = df.select(col).limit(1).collect()[0][col]

        # Parsing the JSON content into a struct        
        df = df.withColumn(col, from_json(col, schema_of_json(json_sample, json_options), json_options))

    return df
