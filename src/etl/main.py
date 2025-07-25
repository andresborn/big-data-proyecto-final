from functools import reduce
from typing import Dict, List

from config import (
    COUNTRIES_TO_DROP,
    COUNTRY_MAPPINGS,
    DB_WRITE_OPTIONS,
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
    df.write.format("jdbc").options(**write_options).mode("overwrite").save()


def main():
    spark = SparkSession.builder.appName("ProyectoFinal").getOrCreate()

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

    # Write to DB
    write_to_db(df, DB_WRITE_OPTIONS)


if __name__ == "__main__":
    main()
