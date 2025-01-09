# Import necessary libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session for Big Data processing
spark = SparkSession.builder.appName("GooglePlaystoreDataAnalysis").getOrCreate()

# Load the dataset
df = spark.read.csv("google_playstore.csv", header=True, inferSchema=True)

# Clean and preprocess data (handling missing values, data types, etc.)
df_cleaned = df.dropna(subset=["App", "Category", "Rating", "Reviews", "Size", "Installs", "Type", "Price"])

# Task 1: Top 10 reviews given to the apps
top_10_reviews = df_cleaned.orderBy(col("Reviews").desc()).limit(10)
top_10_reviews.show()

# Task 2: Top 10 installed apps and distribution of type (free / paid)
df_cleaned = df_cleaned.withColumn("Installs", col("Installs").cast("int"))
top_10_installed_apps = df_cleaned.orderBy(col("Installs").desc()).limit(10)

# Distribution of app types (Free / Paid)
type_distribution = df_cleaned.groupBy("Type").count()

top_10_installed_apps.show()
type_distribution.show()

# Task 3: Category-wise distribution of installed apps
category_distribution = df_cleaned.groupBy("Category").agg(
    {"Installs": "sum"}).orderBy("sum(Installs)", ascending=False)

category_distribution.show()

# Task 4: Top paid apps (Price column processing and filtering)
df_cleaned = df_cleaned.withColumn("Price", col("Price").substr(2, 10).cast("float"))
top_paid_apps = df_cleaned.filter(df_cleaned["Price"] > 0).orderBy(col("Price").desc()).limit(10)
top_paid_apps.show()

# Task 5: Top paid rating apps (Paid apps with high ratings)
top_paid_rating_apps = df_cleaned.filter((df_cleaned["Price"] > 0) & (df_cleaned["Rating"].isNotNull()))
top_paid_rating_apps = top_paid_rating_apps.orderBy(col("Rating").desc()).limit(10)
top_paid_rating_apps.show()

# Stop the Spark session
spark.stop()
