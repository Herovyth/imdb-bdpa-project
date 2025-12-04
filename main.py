from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator

from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier

from loader import load_imdb_file
from schema import *

spark = SparkSession.builder.appName("IMDb Models").getOrCreate()

# 1. Завантажування даних
df_title_basics = load_imdb_file(spark, "imdb_dataset/title.basics.tsv.gz", schema_title_basics)
df_ratings = load_imdb_file(spark, "imdb_dataset/title.ratings.tsv.gz", schema_title_ratings)

# Об'єднання таблиць
df = df_title_basics.join(df_ratings, df_title_basics.tconst == df_ratings.tconst, "inner") \
                    .select(df_title_basics.tconst,
                            col("startYear").cast("int"),
                            col("runtimeMinutes").cast("int"),
                            col("numVotes").cast("int"),
                            "averageRating",
                            "titleType")

# Видалення Nan
df = df.dropna()

# 2. Препроцесинг
# Створення бінарних позначок
df = df.withColumn("isGoodMovie", when(col("averageRating") >= 7, 1).otherwise(0))

# Кодування категорувальних
indexer = StringIndexer(inputCol="titleType", outputCol="titleType_index")

# Вектор фічей
assembler_reg = VectorAssembler(
    inputCols=["startYear", "runtimeMinutes", "numVotes", "titleType_index"],
    outputCol="features"
)

assembler_clf = VectorAssembler(
    inputCols=["startYear", "runtimeMinutes", "numVotes", "titleType_index"],
    outputCol="features"
)

# Розділити вибірку
train, test = df.randomSplit([0.8, 0.2], seed=42)

print("Train size:", train.count())
print("Test size:", test.count())

# 3. Регресійні моделі
reg_models = [
    ("RandomForestRegressor", RandomForestRegressor(featuresCol="features", labelCol="averageRating")),
    ("GBTRegressor", GBTRegressor(featuresCol="features", labelCol="averageRating")),
    ("LinearRegression", LinearRegression(featuresCol="features", labelCol="averageRating"))
]

reg_results = []

evaluator_rmse = RegressionEvaluator(labelCol="averageRating", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="averageRating", metricName="r2")

for name, model in reg_models:
    pipeline = Pipeline(stages=[indexer, assembler_reg, model])
    fitted = pipeline.fit(train)

    fitted.save(f"output/models/regression/{name}")
    print(f"\nSaved regression model to output/models/regression/{name}.")

    predictions = fitted.transform(test)

    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    reg_results.append((name, rmse, r2))
    print(f"\n{name}: RMSE={rmse:.4f}, R2={r2:.4f}")

# 4. Класифікаційні моделі
clf_models = [
    ("RandomForestClassifier", RandomForestClassifier(featuresCol="features", labelCol="isGoodMovie")),
    ("GBTClassifier", GBTClassifier(featuresCol="features", labelCol="isGoodMovie")),
    ("LogisticRegressionClassifier", LogisticRegression(featuresCol="features", labelCol="isGoodMovie"))
]

clf_results = []

evaluator_acc = MulticlassClassificationEvaluator(labelCol="isGoodMovie", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="isGoodMovie", metricName="f1")
evaluator_precision = MulticlassClassificationEvaluator(labelCol="isGoodMovie", metricName="weightedPrecision")
evaluator_recall = MulticlassClassificationEvaluator(labelCol="isGoodMovie", metricName="weightedRecall")

for name, model in clf_models:
    pipeline = Pipeline(stages=[indexer, assembler_clf, model])
    fitted = pipeline.fit(train)

    fitted.save(f"output/models/classification/{name}")
    print(f"\nSaved classification model to output/models/classification/{name}.")

    predictions = fitted.transform(test)

    acc = evaluator_acc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)

    clf_results.append((name, acc, precision, recall, f1))
    print(f"\n{name}: ACC={acc:.4f}, Precision={precision:.4f}, Recall={recall:.4f}, F1={f1:.4f}")

# 5. Результати
print("\nRegression results:")
for name, rmse, r2 in reg_results:
    print(f"{name}: RMSE={rmse:.4f}, R2={r2:.4f}")

print("\nClassification results:")
for name, acc, prec, rec, f1 in clf_results:
    print(f"{name}: Acc={acc:.4f}, Prec={prec:.4f}, Recall={rec:.4f}, F1={f1:.4f}")
