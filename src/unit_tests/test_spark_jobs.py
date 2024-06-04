from chispa.dataframe_comparer import *
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from ..jobs.job_1 import job_1  # Importing job_1 function from the jobs module
from ..jobs.job_2 import job_2  # Importing job_2 function from the jobs module
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    DateType,
    StringType,
    DoubleType,
    LongType,
)
from collections import namedtuple

# Pytest fixture to create and yield a Spark session, and clean it up after tests
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-spark").getOrCreate()
    yield spark  # Yield the Spark session to the test functions
    spark.stop()  # Stop the Spark session after all tests are done

# Namedtuple definitions for structured data representation in tests
Game = namedtuple(
    "Game",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct",
)

DeviceHistory = namedtuple(
    "DeviceHistory",
    "user_id browser_type dates_active date",
)

DeviceHistoryDateInt = namedtuple(
    "DeviceHistoryDateInt", "user_id browser_type history_int history_in_binary"
)

# Test function to verify deduplication logic in job_1
def test_game_details_dedupe(spark):
    # Example input data using the Game namedtuple
    input_data = [
        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        ),
        # Duplicate record
        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        ),
    ]

    # Schema for the DataFrame used to create the DataFrame from input_data
    schema = StructType(
        [
            StructField("game_id", LongType(), True),
            StructField("team_id", LongType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_city", StringType(), True),
            StructField("player_id", LongType(), True),
            StructField("player_name", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("min", StringType(), True),
            StructField("fgm", DoubleType(), True),
            StructField("fga", DoubleType(), True),
            StructField("fg_pct", DoubleType(), True),
        ]
    )

    input_dataframe = spark.createDataFrame(input_data, schema=schema)
    input_table_name = "nba_game_details"

    # Writing the input DataFrame to a table in Spark's SQL warehouse directory
    input_dataframe.write.option(
        "path", spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").saveAsTable(input_table_name)

    # Running the deduplication job and obtaining the actual DataFrame
    actual_df = job_1(spark, "nba_game_details", "nba_game_details_dedup")
    # Expected output with duplicates removed
    expected_output = [
        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        )
    ]

    expected_df = spark.createDataFrame(expected_output, schema=schema)

    # Assertion to ensure the actual DataFrame equals the expected DataFrame, ignoring nullable properties of fields
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

# Test function to verify the calculation logic for user device history in job_2
def test_device_cumulation(spark):
    # Example input data using the DeviceHistory namedtuple
    input_data = [
        DeviceHistory(
            -1358803869, "YandexBot", [datetime.strptime("2023-01-01", "%Y-%m-%d").date()], datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
        ),
        DeviceHistory(
            -1816209818, "Googlebot", [
                datetime.strptime("2023-01-06", "%Y-%m-%d").date(),
                datetime.strptime("
