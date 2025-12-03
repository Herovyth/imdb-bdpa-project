from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col


ARRAY_COLUMNS = {
    "types",
    "attributes",
    "directors",
    "writers",
    "primaryProfession",
    "knownForTitles"
}


def load_imdb_file(spark: SparkSession, path: str, schema) -> DataFrame:
    """
    Loads IMDb TSV.GZ file and uses schema.
    If a scheme has an array – automatically split(",").
    """

    df = (
        spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("nullValue", "\\N")
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(path)
    )

    for col_name in ARRAY_COLUMNS:
        if col_name in df.columns:
            df = df.withColumn(col_name, split(col(col_name), ","))

    return df
