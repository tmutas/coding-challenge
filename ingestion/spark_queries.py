from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .queries import BaseQueries


class SparkSQLQueries(BaseQueries):
    """Implementation

    Args:
        Queries (_type_): _description_
    """

    def __init__(
        self,
        spark: SparkSession,
        warehouse_path: str,
        raw_data: DataFrame
    ):
        self.engine = spark.sql
        self.raw_data = raw_data

        self.warehouse_location = Path(warehouse_path).resolve()
        self.warehouse_location.mkdir(parents=True, exist_ok=True)

        self.raw_db_location = self.warehouse_location / "raw"

        self.dw_db_location = self.warehouse_location / "dw"

    def create_raw_db(self):
        self.engine(f"CREATE DATABASE raw LOCATION '{self.raw_db_location}'")

    def create_dw_db(self):
        self.engine(f"CREATE DATABASE dw LOCATION '{self.dw_db_location}'")

    def create_dim_host(self):
        self.engine("CREATE TABLE dw.dim_host (host_id INT, host STRING)")
        self.engine("INSERT INTO dw.dim_host VALUES (-1, '')")

    def insert_raw_data(self):
        self.raw_data.write.saveAsTable('raw.raw_data')

    def insert_dim_host(self):
        self.engine(
            """
            WITH 
            vals as (SELECT distinct host from raw.raw_data),
            host_dim as (
                SELECT row_number() over (ORDER BY host) as host_id, host FROM vals
            )
            INSERT INTO dw.dim_host SELECT * FROM host_dim
            """
        )

    def create_dim_ip(self):
        self.engine(
            """
            CREATE TABLE dw.dim_ip (
                ip_id INT, 
                ip STRING,
                byte1 INT, 
                byte2 INT,
                byte3 INT,
                byte4 INT
            )
            """
        )
        self.engine(
            """
            INSERT INTO dw.dim_ip VALUES (
                -1, 
                "0.0.0.0",
                0, 
                0,
                0,
                0
            )
            """
        )

    def insert_dim_ip(self):
        # Here I take all IP address from the two columns that contain an IP.
        # I furthermore split the IP by the four bytes,
        # which can be used to filter by subnet for example,
        # and by that filter out invalid IPs
        self.engine(
            """
            WITH all_ips AS (
                SELECT dhcp_lease_ip_addr AS ip FROM raw.raw_data UNION
                SELECT remoteAddress AS ip FROM raw.raw_data 
            ),
            distinct_ips AS (
                SELECT distinct ip FROM all_ips
            ),
            ip_candidates AS (
                SELECT  
                    ip,
                    cast(split_part(ip, '.', 1) as int) as byte1,
                    cast(split_part(ip, '.', 2) as int) as byte2,
                    cast(split_part(ip, '.', 3) as int) as byte3,
                    cast(split_part(ip, '.', 4) as int) as byte4

                FROM distinct_ips
            )
            INSERT INTO dw.dim_ip 
            SELECT 
                row_number() OVER (order by byte1, byte2, byte3, byte4) AS ip_id,
                * 
            from ip_candidates
            WHERE byte1 between 0 and 255
            AND byte2 between 0 and 255
            AND byte3 between 0 and 255
            AND byte4 between 0 and 255
            """
        )
