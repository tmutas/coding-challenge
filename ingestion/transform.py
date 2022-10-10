"""Functions to cleaning and transforming extracted raw data"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_extract, col, locate

# Columns from the sample data set that contain only null values
EMPTY_COLS = [
    'appName',
    'deviceEventClassId',
    'deviceProduct',
    'deviceVendor',
    'deviceVersion',
    'extension',
    'messageId',
    'name',
    'processId',
    'receivedDate',
    'severity',
    'structuredData',
    'tag',
    'version'
 ]

def remove_columns(df: DataFrame, cols: list[str] = EMPTY_COLS) -> DataFrame:
    """Remove any unnecessary columns, e.g. columns without any non-null values

    Args:
        df (DataFrame): DataFrame to remove columns from
        cols (list[str]): Columns to remove if present in the df,
            default a prepared list of columns 

    Returns:
        DataFrame:
    """
    keep_cols = [c for c in df.columns if c not in cols]
    df = df.select(keep_cols)

    return df

def parse_message_field(df : DataFrame, msg_col : str = "message") -> DataFrame:
    """Parsing contents from the message field into separate columns
    and filter test messages

    Args:
        df (DataFrame): 
        msg_col (str, optional): Name of the message column. Defaults to "message".

    Returns:
        DataFrame: With message column parsed, filtered and removed
    """ 

    extracing_regexes = {
        "dhcp_lease_ip_addr" : r"DHCP_LEASE\": ([0-9\.]*) to.*",
        "dhcp_lease_mac_addr" : r"DHCP_LEASE\": [0-9\.]* to ([0-9a-f:]*) .*",
        "dhcp_lease_client_name" : r"client name: (.*),",
        "dhcp_lease_opt82" : r"opt82: ([0-9a-f:]*)"
    }

    for col_name, regex in extracing_regexes.items():
        df = df.withColumn(col_name, regexp_extract(msg_col, regex, 1))

    """
    I noticed a number of "test messages" in the data
    It sounds like those should not be in the data warehouse.
    Alternatively (more performant perhaps), one could also remove rows
    where the extracted fields are null, 
    but I figured explicit filtering is better.
    """

    # If locate yields 0, the substring is not found

    df = df.where(locate("test message", col(msg_col)) == 0)

    # We have parsed the interesting contents from that column
    # Also the rawMessage column contains the message, so this will be kept 
    df = df.drop(msg_col)

    return df
