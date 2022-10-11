"""SQL Queries for data loading step"""
from abc import ABC


class BaseQueries(ABC):
    """Queries are modelled in this base class and need to be implemented
    for different RDMS. As this is an Abstract Base Class,
    it cannot be instantiated itself.

    """

    def __init__(self):
        pass

    def create_raw_db(self):
        pass

    def create_dw_db(self):
        pass

    def create_dim_host(self):
        pass

    def insert_raw_data(self):
        pass

    def insert_dim_host(self):
        pass

    def create_dim_ip(self):
        pass

    def insert_dim_ip(self):
        pass

    def create_dim_kafka(self):
        pass

    def insert_dim_kafka(self):
        pass

    def create_dim_raw_msg(self):
        pass

    def insert_dim_raw_msg(self):
        pass

    def create_dim_dhcp(self):
        pass

    def insert_dim_dhcp(self):
        pass
