from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

# Define the schema for the output DataFrame to ensure proper data types and nullable settings
schema_output = StructType(
    [
        StructField("user_id", LongType(), True),
        StructField("browser_type", StringType(), True),
        StructField("history_int", LongType(), True),
        StructField("history_in_binary", StringType(), True),
    ]
)

# Function to load an existing table or create a new one if it does not exist
def load_or_create_table(spark_session, table_name):
    if table_name not in spark_session.catalog.listTables():
        empty_df = spark_session.createDataFrame([], schema_output)  # Create an empty DataFrame with the defined schema
        empty_df.write.mode("overwrite").saveAsTable(table_name)  # Save the DataFrame as a table
    return spark_session.table(table_name)  # Return the table

# SQL query to process user history data
def query_2(input_table_name: str) -> str:
    query = f"""
    WITH
    today AS (
        SELECT
            *
        FROM
            {input_table_name}
        WHERE
            date = DATE('2023-01-06')  # Filter records for a specific date
    ),
    date_list_int AS (
        SELECT
            user_id,
            browser_type,
            CAST(
                SUM(
                    CASE
                        WHEN array_contains_date(dates_active, sequence_date) THEN POW(2, 30 - DATE_DIFF(DAY, sequence_date, date))
                        ELSE 0
                    END
                ) AS BIGINT
            ) AS history_int  # Calculate the history as an integer value
        FROM
            today
            CROSS JOIN explode (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t (sequence_date)
        GROUP BY
            user_id,
            browser_type
    )
SELECT
    *,
    int_to_binary(history_int) AS history_in_binary  # Convert integer history to binary
FROM
    date_list_int
ORDER BY user_id
    """
    return query

# Function to convert integer to binary string
def int_to_binary(integer):
    return bin(integer)[2:]

# User-defined function to check if a date is in a list of dates
def array_contains_date(dates, date_to_check):
    return date_to_check in dates

# Job function to process the data and write the results to an output table
def job_2(
    spark_session: SparkSession, input_table_name: str, output_table_name: str
) -> Optional[DataFrame]:
    spark_session.udf.register("int_to_binary", int_to_binary, StringType())  # Register the integer to binary conversion function
    spark_session.udf.register(
        "array_contains_date", array_contains_date, BooleanType()  # Register the date checking function
    )
    output_df = load_or_create_table(spark_session, output_table_name)  # Load or create the output table
    output_df.createOrReplaceTempView(output_table_name)  # Temporarily register the DataFrame as a table
    return spark_session.sql(query_2(input_table_name))  # Execute the SQL query and return the result

def main():
    input_table_name: str = "devices_history"
    output_table_name: str = "history_date_list_int"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()  # Initialize Spark session
    )
    output_df = job_2(spark_session, input_table_name, output_table_name)
    output_df.write.option(
        "path", spark_session.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").insertInto(output_table_name)  # Write the DataFrame back into the warehouse directory
