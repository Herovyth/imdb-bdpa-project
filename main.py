from pyspark.sql import SparkSession

from loader import load_imdb_file
from schema import *

spark = SparkSession.builder.appName("IMDb Loader").getOrCreate()

df_title_agas = load_imdb_file(spark, "imdb_dataset/title.akas.tsv.gz", schema_title_agas)
df_title_basics = load_imdb_file(spark, "imdb_dataset/title.basics.tsv.gz", schema_title_basics)
df_title_crew = load_imdb_file(spark, "imdb_dataset/title.crew.tsv.gz", schema_title_crew)
df_title_episode = load_imdb_file(spark, "imdb_dataset/title.episode.tsv.gz", schema_title_episode)
df_title_principals = load_imdb_file(spark, "imdb_dataset/title.principals.tsv.gz", schema_title_principals)
df_title_ratings = load_imdb_file(spark, "imdb_dataset/title.ratings.tsv.gz", schema_title_ratings)
df_name_basics = load_imdb_file(spark, "imdb_dataset/name.basics.tsv.gz", schema_name_basics)

df_title_agas.show(5)
df_title_basics.show(5)
df_title_crew.show(5)
df_title_episode.show(5)
df_title_principals.show(5)
df_title_ratings.show(5)
df_name_basics.show(5)
