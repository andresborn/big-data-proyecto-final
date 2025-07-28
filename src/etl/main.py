from functools import reduce
from typing import Dict, List

from config import (
    COUNTRIES_TO_DROP,
    COUNTRY_MAPPINGS,
    DB_OPTIONS,
    NULL_INDICATORS,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_and_prep_sipri_data(spark: SparkSession, file_path: str):
    sipri_df = spark.read.csv(file_path, header=True)

    # Quitar punctuación porque rompe cosas
    sipri_df = sipri_df.withColumnRenamed(
        "Share of Govt. spending", "Share of Govt spending"
    )

    # Agregar sufijo para no olvidar a que corresponde datos
    suffix = "_Military"
    sipri_df = sipri_df.select([F.col(c).alias(c + suffix) for c in sipri_df.columns])

    # Devolver a 'Country' para poder join
    return sipri_df.withColumnRenamed("Country_Military", "Country")


def load_and_pivot_dev_data(spark: SparkSession, file_path: str):
    dev_df = spark.read.csv(file_path, header=True)

    # Quitar columnas innecesarias
    dev_df = dev_df.drop("Country Code", "Series Code")

    # Pivotear Series Name
    pivoted_df = (
        dev_df.groupBy("Country Name")
        .pivot("Series Name")
        .agg(F.first("2023 [YR2023]"))
    )

    # Quitar donde "Country Name" es nulo
    pivoted_df = pivoted_df.drop("null").filter(pivoted_df["Country Name"].isNotNull())

    # Nombrar 'Country' para join después
    return pivoted_df.withColumnRenamed("Country Name", "Country")


def standardize_country_names(
    df, country_mappings: Dict[str, str], countries_to_drop: List[str]
):
    """
    BD del BM usa nombres formales de los países mientras que
    la de SIPRI usa otros nombres. Hicimos un mapeo basándonos en los de SIPRI
    porque son más cortos, entre otras razones.
    """
    df = df.replace(country_mappings, subset=["Country"])
    return df.filter(~F.col("Country").isin(countries_to_drop))


def join_datasets(sipri_df, dev_df):
    """
    Join SIPRI y World Bank Development Indicators
    datasets. Decidimos un left join con SIPRI.
    """
    return sipri_df.join(dev_df, on="Country", how="left")


def filter_null_values(df, null_indicators: List[str]):
    """
    Quitar valores nulos. Hicimos una lista de valores nulos
    que incluye otros valores que no tenían sentido. Esa lista
    está en declarada abajo.
    """
    condition = ~reduce(
        lambda a, b: a | b, [F.col(c).isin(null_indicators) for c in df.columns]
    )
    return df.filter(condition)


def write_to_db(df: DataFrame, write_options):
    df = df.withColumnsRenamed(
        {
            "Country": "country",
            "Constant (2023) US$_Military": "constant_2023_usd_military",
            "Current US$_Military": "current_usd_military",
            "Share of GDP_Military": "share_gdp_military",
            "Per capita_Military": "per_capita_military",
            "Share of Govt spending_Military": "share_govt_spend_military",
            "Exports of goods and services (% of GDP)": "exports_percent_gdp",
            "GDP per capita (constant 2015 US$)": "gdp_per_capita_2015_usd",
            "GDP per capita (current US$)": "gdp_per_capita_current_usd",
            "Gross capital formation (% of GDP)": "gcf_percent_gdp",
            "Life expectancy at birth, female (years)": "life_expectancy_female",
            "Life expectancy at birth, male (years)": "life_expectancy_male",
            "Life expectancy at birth, total (years)": "life_expectancy_total",
            "Net migration": "net_migration",
            "Trade (% of GDP)": "trade_percent_gdp",
        }
    )

    df = df.withColumn(
        "constant_2023_usd_military", df["constant_2023_usd_military"].cast("double")
    )
    df = df.withColumn(
        "current_usd_military", df["current_usd_military"].cast("double")
    )
    df = df.withColumn("share_gdp_military", df["share_gdp_military"].cast("double"))
    df = df.withColumn("per_capita_military", df["per_capita_military"].cast("double"))
    df = df.withColumn(
        "share_govt_spend_military", df["share_govt_spend_military"].cast("double")
    )
    df = df.withColumn("exports_percent_gdp", df["exports_percent_gdp"].cast("double"))
    df = df.withColumn(
        "gdp_per_capita_2015_usd", df["gdp_per_capita_2015_usd"].cast("double")
    )
    df = df.withColumn(
        "gdp_per_capita_current_usd", df["gdp_per_capita_current_usd"].cast("double")
    )
    df = df.withColumn("gcf_percent_gdp", df["gcf_percent_gdp"].cast("double"))
    df = df.withColumn(
        "life_expectancy_female", df["life_expectancy_female"].cast("double")
    )
    df = df.withColumn(
        "life_expectancy_male", df["life_expectancy_male"].cast("double")
    )
    df = df.withColumn(
        "life_expectancy_total", df["life_expectancy_total"].cast("double")
    )
    df = df.withColumn("net_migration", df["net_migration"].cast("double"))
    df = df.withColumn("trade_percent_gdp", df["trade_percent_gdp"].cast("double"))

    df.write.format("jdbc").options(**write_options).option(
        "createTableColumnTypes",
        """
                country VARCHAR(100),
                constant_2023_usd_military FLOAT,
                current_usd_military FLOAT,
                share_gdp_military FLOAT,
                per_capita_military FLOAT,
                share_govt_spend_military FLOAT,
                exports_percent_gdp FLOAT,
                gdp_per_capita_2015_usd FLOAT,
                gdp_per_capita_current_usd FLOAT,
                gcf_percent_gdp FLOAT,
                life_expectancy_female FLOAT,
                life_expectancy_male FLOAT,
                life_expectancy_total FLOAT,
                net_migration FLOAT,
                trade_percent_gdp FLOAT
            """,
    ).mode("overwrite").save()


def main():
    spark = SparkSession.builder.appName("ETL").getOrCreate()

    # Cargar y preparar datasets individualmente
    sipri_df = load_and_prep_sipri_data(spark, "src/data/sipri_clean.csv")
    pivoted_df = load_and_pivot_dev_data(spark, "src/data/development_indicators.csv")

    # Estandarizar los nombres de las bases de datos (Solo la del Banco Mundial)
    # basada en los nombres de la de SIPRI
    pivoted_df = standardize_country_names(
        pivoted_df, COUNTRY_MAPPINGS, COUNTRIES_TO_DROP
    )

    # Eliminar de la base de datos de SIPRI algunos países que
    # no estamos seguros como tratarlos
    sipri_df = sipri_df.filter(~F.col("Country").isin(COUNTRIES_TO_DROP))

    # Join datasets y drop nulls
    df = join_datasets(sipri_df, pivoted_df)
    df = df.drop("Multidimensional poverty headcount ratio (UNDP) (% of population)")
    df = filter_null_values(df, NULL_INDICATORS)

    df.write.csv("src/data/full.csv")

    # Write to DB
    write_to_db(df, DB_OPTIONS)


if __name__ == "__main__":
    main()
