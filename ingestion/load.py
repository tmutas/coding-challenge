"""Functions for loading the transformed data into a Data Warehouse"""
from .queries import BaseQueries


def create_databases(queries: BaseQueries):
    queries.create_raw_db()
    queries.create_dw_db()


def create_dimension_tables(queries: BaseQueries):
    queries.create_dim_host()
    queries.create_dim_ip()


def insert_dimensions(queries: BaseQueries):
    queries.insert_dim_host()
    queries.insert_dim_ip()

def insert_rawdata(queries : BaseQueries):
    queries.insert_raw_data()