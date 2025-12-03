from pyspark.sql.types import *

schema_title_akas = StructType([
    StructField("titleId", StringType()),
    StructField("ordering", IntegerType()),
    StructField("title", StringType()),
    StructField("region", StringType()),
    StructField("language", StringType()),
    StructField("types", StringType()),
    StructField("attributes", StringType()),
    StructField("isOriginalTitle", BooleanType())
])

schema_title_basics = StructType([
    StructField("tconst", StringType()),
    StructField("titleType", StringType()),
    StructField("primaryTitle", StringType()),
    StructField("originalTitle", StringType()),
    StructField("isAdult", IntegerType()),
    StructField("startYear", IntegerType()),
    StructField("endYear", IntegerType()),
    StructField("runtimeMinutes", IntegerType()),
    StructField("genres", StringType())
])

schema_title_crew = StructType([
    StructField("tconst", StringType()),
    StructField("directors", StringType()),
    StructField("writers", StringType())
])

schema_title_episode = StructType([
    StructField("tconst", StringType()),
    StructField("parentTconst", StringType()),
    StructField("seasonNumber", IntegerType()),
    StructField("episodeNumber", IntegerType())
])

schema_title_principals = StructType([
    StructField("tconst", StringType()),
    StructField("ordering", IntegerType()),
    StructField("nconst", StringType()),
    StructField("category", StringType()),
    StructField("job", StringType()),
    StructField("characters", StringType())
])

schema_title_ratings = StructType([
    StructField("tconst", StringType()),
    StructField("averageRating", FloatType()),
    StructField("numVotes", IntegerType())
])

schema_name_basics = StructType([
    StructField("nconst", StringType()),
    StructField("primaryName", StringType()),
    StructField("birthYear", IntegerType()),
    StructField("deathYear", IntegerType()),
    StructField("primaryProfession", StringType()),
    StructField("knownForTitles", StringType())
])
