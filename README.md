# Data Engineering Coding Challenge
## Introduction
The task of the challenge is to ingest a CSV file of Kafka events, transform it and load it into a data warehouse.

## Getting Started
The project is written in Python, as it's versatile in the variety of tools it can interact with and it's the language I'm most proficient in. 

#### Dependencies
I use poetry as a dependency management tool for Python. In order to use it, you first need to run `pip install poetry` in your chosen Python enviroment, and afterwards just launch `poetry install` with an optional `--without dev` flag, if you don't intend to explore the data through pandas in a jupyter notebook or similar or don't need python developement tools.

- `pyspark`
Note that you need to have at least a JDK installed. Spark does work locally without a Hadoop cluster

#### Running it
The pipeline is wrapped into a simple `main.py` script that runs the pipeline, and takes the file path as input.
For production purposes, there is obviously better choices to orchestrate the pipeline, which I didn't implement due to time constraints.
For example, Apache Airflow could be used to run the provided functions as separate Operators. 

## Data Extraction
I decided to use PySpark for the ETL task, as it can also handle large datasets in a distributed fashion. There is also the Structured Streaming extension, which could be an option in a real-life setting, where the Kafka data can be consumed directly with (hopefully) minor code changes to the rest of the ETL pipeline.

The extraction step uses the standard PySpark reader to load the multiline CSV structure, which in turn contains data serialized as a JSON string.
In the subsequent step, PySpark json functions are used to parse the strings, and load and combine the data into a flattened DataFrame. This provides a good basis for further transformations.

## Data Transformation
The transformation step is also performed in PySpark. 

Of course, this step can be as well performed in a variety of other ways than on a Spark DataFrame.
For example, by loading it into some database before, and using SQL afterwards.
I assume dbt is a suitable choice on top of that.

#### Removing empty columns
In the provided data sample, there were a couple of fields which were entirely filled with null data. 
Due to lack of domain knowledge about the data source, I decided to disregard them completely, as I don't know more about how to model a sensible dimension out of it, and reduce the complexity.
In a real-life setting, one might consider implementing a more automated approach on loading any dimensions, if those columns can expect useful data at some point.

#### Parsing message column
The message column also contained some semi-structured data, which I chose to extract using regexes.
It also contained messages generated by a test system, which was a sign for me to remove it. 
I decided to do it explicitely on finding a "test message" string, but depending on what's expected in the message, it could also be on filtering the rows where the extracted field is null. 

However, for an informed decision, more domain knowledge about the data generating system is required.
For example, different types of messages might be expected. Then there could be additional fields extracted, with others being allowed to be null, or the data can be split by a TBD "message type", and modeled with separate dimensions for the different message types.

