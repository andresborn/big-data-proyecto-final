from datetime import datetime

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import FloatType, StringType, StructField, StructType

DB_OPTIONS = {
    "url": "jdbc:postgresql://host.docker.internal:5433/postgres",
    "user": "postgres",
    "password": "testPassword",
    "dbtable": "ms_di",
}

spark = SparkSession.builder.appName("ML").getOrCreate()

df = spark.read.format("jdbc").options(**DB_OPTIONS).load()

schema = StructType(
    [
        StructField("country", StringType(), True),
        StructField("constant_2023_usd_military", FloatType(), True),
        StructField("current_usd_military", FloatType(), True),
        StructField("share_gdp_military", FloatType(), True),
        StructField("per_capita_military", FloatType(), True),
        StructField("share_govt_spend_military", FloatType(), True),
        StructField("exports_percent_gdp", FloatType(), True),
        StructField("gdp_per_capita_2015_usd", FloatType(), True),
        StructField("gdp_per_capita_current_usd", FloatType(), True),
        StructField("gcf_percent_gdp", FloatType(), True),
        StructField("life_expectancy_female", FloatType(), True),
        StructField("life_expectancy_male", FloatType(), True),
        StructField("life_expectancy_total", FloatType(), True),
        StructField("net_migration", FloatType(), True),
        StructField("trade_percent_gdp", FloatType(), True),
    ]
)

df = spark.createDataFrame(df.rdd, schema=schema)


# Regresión Lineal

# Crear modelo

feature_cols_lr = [
    "share_gdp_military",
    "share_govt_spend_military",
    "gdp_per_capita_current_usd",
    "gcf_percent_gdp",
    "life_expectancy_total",
    "net_migration",
    "trade_percent_gdp",
]

# Variables predictivas
assembler = VectorAssembler(inputCols=feature_cols_lr, outputCol="features")

# Dividir datos en entrenamiento y prueba
train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

# Crear y entrenar modelo de regresión lineal
lr = LinearRegression(
    featuresCol="features",
    labelCol="per_capita_military",
    predictionCol="pred_per_capita_military",
)

# Pipeline
pipeline = Pipeline(stages=[assembler, lr])

# Entrenar modelo
lr_model = pipeline.fit(train_data)

# Hacer predicciones
lr_predictions = lr_model.transform(test_data)

# Evaluar modelo
evaluator = RegressionEvaluator(
    labelCol="per_capita_military",
    predictionCol="pred_per_capita_military",
    metricName="rmse",
)

rmse = evaluator.evaluate(lr_predictions)
print(f"Root Mean Squared Error (RMSE) para Regresión lineal: {rmse}")

# Mostrar algunas predicciones
lr_predictions.select(
    "country", "per_capita_military", "pred_per_capita_military"
).show(5)

# Validar modelo de Regresión Lineal con validación cruzada

# Definir la cuadrícula de parámetros a probar
param_grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.01, 0.1, 0.5])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .addGrid(lr.maxIter, [10, 20])
    .build()
)

# Configurar el CrossValidator
cross_val = CrossValidator(
    estimator=pipeline,  # Nuestro pipeline original
    estimatorParamMaps=param_grid,
    evaluator=RegressionEvaluator(
        labelCol="per_capita_military",
        predictionCol="pred_per_capita_military",
        metricName="rmse",
    ),
    numFolds=3,  # Número de folds para validación cruzada
    seed=42,
)

# Ejecutar la validación cruzada
cv_model = cross_val.fit(train_data)

# Obtener el mejor modelo
best_lr_model = cv_model.bestModel

# Ver los mejores parámetros encontrados
print(f"Mejor parámetro regParam: {best_lr_model.stages[-1].getRegParam()}")
print(
    f"Mejor parámetro elasticNetParam: {best_lr_model.stages[-1].getElasticNetParam()}"
)
print(f"Mejor parámetro maxIter: {best_lr_model.stages[-1].getMaxIter()}")

# Features más significantes
coefficients = best_lr_model.stages[-1].coefficients

# Crear DataFrame con los coeficientes
coeff_df = spark.createDataFrame(
    [(feature_cols_lr[i], float(coefficients[i])) for i in range(len(feature_cols_lr))],
    ["feature", "coefficient"],
)

# Evaluar el mejor modelo en los datos de prueba
cv_predictions = best_lr_model.transform(test_data)
rmse = evaluator.evaluate(cv_predictions)
print(f"RMSE del mejor modelo: {rmse}")

# Modelo Random Forest

# Crear variable categórica para clasificación
quantiles = df.approxQuantile("share_gdp_military", [0.33, 0.66], 0.01)
low_threshold = quantiles[0]
high_threshold = quantiles[1]

classification_df = df.withColumn(
    "military_level",
    when(df["share_gdp_military"] < low_threshold, "low")
    .when(df["share_gdp_military"] < high_threshold, "medium")
    .otherwise("high"),
)


# Indexar la variable categórica
indexer = StringIndexer(inputCol="military_level", outputCol="label")

feature_cols_rf = [
    # "share_gdp_military", # Sin esta porque es la variable de donde sacamos la clasificación
    "share_govt_spend_military",  # Esta variable tiene alta correlación con share_gdp_military e influye mucho
    "gdp_per_capita_current_usd",
    "gcf_percent_gdp",
    "life_expectancy_total",
    "net_migration",
    "trade_percent_gdp",
]

class_assembler = VectorAssembler(inputCols=feature_cols_rf, outputCol="features")

# Dividir datos
class_train_data, class_test_data = classification_df.randomSplit([0.7, 0.3], seed=42)

# Crear modelo Random Forest
rf = RandomForestClassifier(
    featuresCol="features", labelCol="label", numTrees=20, maxDepth=5, seed=42
)

# Pipeline
class_pipeline = Pipeline(stages=[indexer, class_assembler, rf])

# Entrenar modelo
rf_model = class_pipeline.fit(class_train_data)

# Hacer predicciones
rf_predictions = rf_model.transform(class_test_data)

# Evaluar modelo
class_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
accuracy = class_evaluator.evaluate(rf_predictions)
print(f"Accuracy for Random Forest Classifier: {accuracy}")

# Mostrar matriz de confusión
rf_predictions.groupBy("label", "prediction").count().show()

# Definir la cuadrícula de parámetros para Random Forest
rf_param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [10, 20, 30])
    .addGrid(rf.maxDepth, [3, 5, 7])
    .addGrid(rf.maxBins, [20, 32])
    .addGrid(rf.impurity, ["gini", "entropy"])
    .build()
)

# Validación cruzada para Random Forest
# Configurar el CrossValidator
rf_cross_val = CrossValidator(
    estimator=class_pipeline,  # Nuestro pipeline de clasificación
    estimatorParamMaps=rf_param_grid,
    evaluator=MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    ),
    numFolds=3,  # Número de folds para validación cruzada
    seed=42,
)

# Ejecutar la validación cruzada
rf_cv_model = rf_cross_val.fit(class_train_data)

# Obtener el mejor modelo
best_rf_model = rf_cv_model.bestModel

# Ver los mejores parámetros encontrados
best_rf = best_rf_model.stages[-1]  # El RandomForest es el último stage del pipeline
print(f"Mejor número de árboles: {best_rf.getNumTrees}")
print(f"Mejor profundidad máxima: {best_rf.getMaxDepth()}")
print(f"Mejor número de bins: {best_rf.getMaxBins()}")
print(f"Mejor criterio de impureza: {best_rf.getImpurity()}")

# DataFrame con features más importantes
feature_importances = best_rf.featureImportances
importance_df = spark.createDataFrame(
    [
        (feature_cols_rf[i], float(feature_importances[i]))
        for i in range(len(feature_cols_rf))
    ],
    ["feature", "importance"],
)
importance_df.orderBy("importance", ascending=False).show(truncate=False)

# Evaluar el mejor modelo
rf_cv_predictions = best_rf_model.transform(class_test_data)
accuracy = class_evaluator.evaluate(rf_cv_predictions)
print(f"Accuracy del mejor modelo: {accuracy}")


# Crear texto con resultados y salvar a archivo
results_content = f"""
Resultados de modelos de predicción
========================================
Fecha: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

1. Modelo de regresión lineal
----------------------------------------
RMSE inicial: {rmse}
Parámetros optimizados:
- regParam: {best_lr_model.stages[-1].getRegParam()}
- elasticNetParam: {best_lr_model.stages[-1].getElasticNetParam()}
- maxIter: {best_lr_model.stages[-1].getMaxIter()}
RMSE optimizado: {evaluator.evaluate(cv_predictions)}

Lista de features: {feature_cols_lr}
Target: {"per_capita_military"}

Feature Coefficients:
{coeff_df.orderBy("coefficient", ascending=False).toPandas().to_string()}

2. Clasificador Random Forest
----------------------------------------
Accuracy inicial: {accuracy}
Mejores parámetros:
- numTrees: {best_rf.getNumTrees}
- maxDepth: {best_rf.getMaxDepth()}
- maxBins: {best_rf.getMaxBins()}
- impurity: {best_rf.getImpurity()}
Accuracy optimizada: {class_evaluator.evaluate(rf_cv_predictions)}

Lista de features: {feature_cols_rf}
Target: {"military_level"}

Features más importantes:
{importance_df.orderBy("importance", ascending=False).toPandas().to_string()}


Notas:
- share_govt_spend_military tiene una alta correlación con share_gdp_military
posiblemente porque la mayoría del gasto militar proviene del gobierno.
Esto hace que el mejor modelo tenga una precisión muy alta, cuando tiene 
ese parámetro, y cuando no, baja considerablemente.
"""

output_filename = "analisis_de_resultados.txt"
with open(output_filename, "w") as f:
    f.write(results_content)

print(f"Results successfully saved to {output_filename}")
