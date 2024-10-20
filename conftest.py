import pytest
from lib.Utils import get_spark_session


@pytest.fixture
def spark():
    """
    Create Spark Session
    :return: SparkSession object
    """
    # creates a spark session
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def expected_results(spark):
    """
    Create expected results for the customers and state grouping
    :param spark:
    :return:
    """
    result_schema = "state string, count int"
    result = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(result_schema) \
        .load("data/results_csv")
    return result
