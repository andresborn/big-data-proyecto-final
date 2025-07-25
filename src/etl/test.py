import pytest
from config import COUNTRIES_TO_DROP, COUNTRY_MAPPINGS, NULL_INDICATORS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from main import (
    filter_null_values,
    join_datasets,
    load_and_pivot_dev_data,
    load_and_prep_sipri_data,
    standardize_country_names,
)


@pytest.fixture(scope="module")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_sipri_data(spark_session):
    data = [("United States", 100), ("Russia", 200), ("Germany", 150)]
    return spark_session.createDataFrame(data, ["Country", "Current US$"])


@pytest.fixture
def sample_dev_data(spark_session):
    data = [
        ("United States of America", "GDP", "1000"),
        ("United Staes of America", "Population", "330"),
        ("Russian Federation", "GDP", "500"),
        ("Russian Federation", "Population", "144"),
        ("Germany", "GDP", "400"),
        ("Germany", "Population", "83"),
    ]
    return spark_session.createDataFrame(
        data, ["Country Name", "Series Name", "2023 [YR2023]"]
    )


def test_create_spark_session(spark_session):
    assert spark_session is not None
    assert spark_session.sparkContext.appName == "pytest-pyspark"


def test_load_and_prepare_sipri_data(spark_session, sample_sipri_data, tmp_path):
    # Write test data to temp file
    test_path = tmp_path / "sipri_test.csv"
    sample_sipri_data.write.csv(str(test_path), header=True, mode="overwrite")

    # Test the function
    result = load_and_prep_sipri_data(spark_session, str(test_path))
    assert isinstance(result, DataFrame)
    assert "Country" in result.columns
    assert "Current US$_Military" in result.columns


def test_load_and_pivot_dev_data(spark_session, sample_dev_data, tmp_path):
    # Write test data to temp file
    test_path = tmp_path / "dev_test.csv"
    sample_dev_data.write.csv(str(test_path), header=True, mode="overwrite")

    # Test the function
    result = load_and_pivot_dev_data(spark_session, str(test_path))
    assert isinstance(result, DataFrame)
    assert "Country" in result.columns
    assert "GDP" in result.columns
    assert "Population" in result.columns


def test_standardize_country_names(
    spark_session,
    country_mappings=COUNTRY_MAPPINGS,
    countries_to_drop=COUNTRIES_TO_DROP,
):
    data = [("Russian Federation", 100), ("United States", 200), ("Yugoslavia", 50)]
    df = spark_session.createDataFrame(data, ["Country", "Value"])

    result = standardize_country_names(df, country_mappings, countries_to_drop)

    # Check country name was changed: Russia
    russia_rows = result.filter(F.col("Country") == "Russia").collect()
    assert len(russia_rows) == 1

    # Check country name was changed: United States of America
    usa_rows = result.filter(F.col("Country") == "United States of America").collect()
    assert len(usa_rows) == 1

    # Check country was dropped
    yugoslavia_rows = result.filter(F.col("Country") == "Yugoslavia").collect()
    assert len(yugoslavia_rows) == 0


def test_join_datasets(spark_session):
    df1 = spark_session.createDataFrame(
        [("Spain", 100), ("Russia", 200), ("Afghanistan", 100)], ["Country", "Value1"]
    )
    df2 = spark_session.createDataFrame(
        [("Spain", "A"), ("Russia", "B"), ("France", "C")], ["Country", "Value2"]
    )

    result = join_datasets(df1, df2)

    assert result.count() == 3
    assert len(result.columns) == 3
    assert result.filter(F.col("Country") == "Spain").first()["Value2"] == "A"
    # Left join solo los valores de df1
    france_rows = result.filter(F.col("Country") == "France").collect()
    assert len(france_rows) == 0
    afghanistan_rows = result.filter(F.col("Country") == "Afghanistan").collect()
    assert len(afghanistan_rows) == 1


def test_filter_null_values(spark_session, null_indicators=NULL_INDICATORS):
    test_data = [
        ("Mexico", "100", "200"),
        ("Russia", "..", "300"),
        ("Germany", "...", "400"),
        ("France", "500", "0"),
        ("China", "1000", ".."),
        ("Costa Rica", "300", "1"),  # 1 también no vale
    ]
    df = spark_session.createDataFrame(test_data, ["Country", "Value1", "Value2"])

    result = filter_null_values(df, null_indicators)

    assert result.count() == 1
    assert result.first()["Country"] == "Mexico"


def test_get_country_mappings(country_mappings=COUNTRY_MAPPINGS):
    assert isinstance(country_mappings, dict)
    assert country_mappings["Russian Federation"] == "Russia"
    assert country_mappings["United States"] == "United States of America"
    assert country_mappings["North Korea"] == "Korea, North"
    assert "United States" in country_mappings


def test_get_countries_to_drop(countries_to_drop=COUNTRIES_TO_DROP):
    assert isinstance(countries_to_drop, list)
    assert "Yugoslavia" in countries_to_drop
    assert "USSR" in countries_to_drop
    assert "Czechoslovakia" in countries_to_drop
    assert "German Democratic Republic" in countries_to_drop


def test_get_null_indicators(null_indicators=NULL_INDICATORS):
    assert isinstance(null_indicators, list)
    assert ".." in null_indicators
    assert "..." in null_indicators
    assert "0" in null_indicators
    assert "1" in null_indicators
