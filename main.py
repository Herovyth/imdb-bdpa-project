from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as f

from loader import load_imdb_file
from schema import *
from save_csv import save_to_csv

spark = SparkSession.builder.appName("IMDb Loader").getOrCreate()

df_title_akas = load_imdb_file(spark, "imdb_dataset/title.akas.tsv.gz", schema_title_akas)
df_title_basics = load_imdb_file(spark, "imdb_dataset/title.basics.tsv.gz", schema_title_basics)
df_title_crew = load_imdb_file(spark, "imdb_dataset/title.crew.tsv.gz", schema_title_crew)
df_title_episode = load_imdb_file(spark, "imdb_dataset/title.episode.tsv.gz", schema_title_episode)
df_title_principals = load_imdb_file(spark, "imdb_dataset/title.principals.tsv.gz", schema_title_principals)
df_title_ratings = load_imdb_file(spark, "imdb_dataset/title.ratings.tsv.gz", schema_title_ratings)
df_name_basics = load_imdb_file(spark, "imdb_dataset/name.basics.tsv.gz", schema_name_basics)


print("title.akas schema:\n")
df_title_akas.printSchema()

print("Number of rows:", df_title_akas.count())
print("Number of columns:", len(df_title_akas.columns))
print("Columns:", df_title_akas.columns)

df_title_akas.select("title", "region", "language").describe().show()

print("title.basics schema:\n")
df_title_basics.printSchema()

print("Number of rows:", df_title_basics.count())
print("Number of columns:", len(df_title_basics.columns))
print("Columns:", df_title_basics.columns)

df_title_basics.select("startYear", "endYear", "runtimeMinutes").describe().show()

print("title.crew schema:\n")
df_title_crew.printSchema()

print("Number of rows:", df_title_crew.count())
print("Number of columns:", len(df_title_crew.columns))
print("Columns:", df_title_crew.columns)

df_title_crew.select("directors", "writers").describe().show()

print("title.episode schema:\n")
df_title_episode.printSchema()

print("Number of rows:", df_title_episode.count())
print("Number of columns:", len(df_title_episode.columns))
print("Columns:", df_title_episode.columns)

df_title_episode.select("seasonNumber", "episodeNumber").describe().show()

print("title.principals schema:\n")
df_title_principals.printSchema()

print("Number of rows:", df_title_principals.count())
print("Number of columns:", len(df_title_principals.columns))
print("Columns:", df_title_principals.columns)

df_title_principals.select("category", "job", "characters").describe().show()

print("title.ratings schema:\n")
df_title_ratings.printSchema()

print("Number of rows:", df_title_ratings.count())
print("Number of columns:", len(df_title_ratings.columns))
print("Columns:", df_title_ratings.columns)

df_title_ratings.select("averageRating", "numVotes").describe().show()

print("name.basics schema:\n")
df_name_basics.printSchema()

print("Number of rows:", df_name_basics.count())
print("Number of columns:", len(df_name_basics.columns))
print("Columns:", df_name_basics.columns)

df_name_basics.select("birthYear", "primaryProfession", "knownForTitles").describe().show()

print("Business questions:\n")

# Знайти всі фільми після 2015 року, що мають жанр 'Action'. (filter)
print("All movies after 2015 that have the genre Action")
df_new_action = df_title_basics \
    .filter((df_title_basics.startYear >= 2015) & df_title_basics.genres.contains("Action"))
save_to_csv(df_new_action, "decades_action_movies")

# Знайти людей, які мають професію 'actor'. (filter)
print("People who have profession of an actor")
df_actors = df_name_basics.filter(
    f.array_contains(df_name_basics.primaryProfession, "actor")
)
df_actors = df_actors.withColumn(
    "primaryProfession", f.concat_ws(",", "primaryProfession")
)
df_actors = df_actors.select("primaryName", "primaryProfession")
save_to_csv(df_actors, "actors")

# Отримати всі фільми, доступні китайською мовою. (filter)
print("All the movies that are available in Chinese")
df_zh_titles = df_title_akas.filter(df_title_akas.language == "zh") \
    .select("titleId", "title", "region", "language")
save_to_csv(df_zh_titles, "zh_titles")

# Отримати назви фільмів та їхній рейтинг. (join)
print("Get movie titles and their ratings")
df_movie_ratings = df_title_basics.join(df_title_ratings, "tconst", "inner") \
    .select("primaryTitle", "averageRating", "numVotes")
save_to_csv(df_movie_ratings, "title_ratings")

# Знайти середній рейтинг для кожного жанру. (join + group by)
print("An average rating for each genre")
df_genre_rating = df_title_basics.join(df_title_ratings, "tconst") \
    .withColumn("genre", f.explode(f.split("genres", ","))) \
    .groupBy("genre") \
    .agg(f.avg("averageRating").alias("avg_rating"))
save_to_csv(df_genre_rating, "genre_avg_rating")

# Кількість фільмів у кожному жанрі. (group by)
print("Number of movies per genre")
df_movies_by_genre = df_title_basics.filter(df_title_basics.titleType == "movie") \
    .withColumn("genre", f.explode(f.split("genres", ","))) \
    .groupBy("genre") \
    .agg(f.count("*").alias("movie_count")) \
    .orderBy(f.desc("movie_count"))
save_to_csv(df_movies_by_genre, "movies_count_by_genre")

# Показати топ 5 найрейтинговіших фільмів у кожному році. (window function)
print("Top 5 highest-rated movies in each year")
w = Window.partitionBy("startYear").orderBy(f.desc("averageRating"))

df_year_top = df_title_basics.join(df_title_ratings, "tconst") \
    .filter(df_title_basics.startYear.isNotNull()) \
    .withColumn("rank", f.row_number().over(w)) \
    .filter("rank <= 5")

save_to_csv(df_year_top, "top5_year")

# Обчислити кумулятивну кількість голосів у фільмах у межах року. (window function)
print("Cumulative number of votes in films within a year")
w2 = Window.partitionBy("startYear").orderBy("numVotes")

df_votes_cum = df_title_basics.join(df_title_ratings, "tconst") \
    .withColumn("cumulative_votes", f.sum("numVotes").over(w2))

save_to_csv(df_votes_cum, "votes_cumulative")
