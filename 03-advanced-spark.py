#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark aggregation functions") \
    .getOrCreate()


# In[3]:


listings = spark.read.csv("data/listings.csv", 
    header=True,
    inferSchema=True,
    sep=",", 
    quote='"',
    escape='"', 
    multiLine=True,
    mode="PERMISSIVE" 
)
listings.printSchema()


# In[4]:


reviews = spark.read.csv("data/reviews.csv", 
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)
reviews.printSchema()


# In[6]:


# 1. For each listing compute string category depending on its price, and add it as a new column.
# A category is defined in the following way:
#
# * price < 50 -> "Budget"
# * 50 <= price < 150 -> "Mid-range"
# * price >= 150 -> "Luxury"
# 
# Only include listings where the price is not null.
# Count the number of listings in each category

from pyspark.sql.functions import col, regexp_replace, udf
from pyspark.sql.types import StringType

# 1) Price -> numeric
listings_num = listings.withColumn(
    "price_numeric",
    regexp_replace(col("price"), "[$,]", "").cast("double")
)

# 2) UDF for price category
def price_category(p):
    if p is None:
        return None
    if p < 50:
        return "Budget"
    elif p < 150:
        return "Mid-range"
    else:
        return "Luxury"

price_category_udf = udf(price_category, StringType())

# 3) Filter out null prices and apply the UDF
with_category = (
    listings_num
    .filter(col("price_numeric").isNotNull())
    .withColumn("price_category", price_category_udf(col("price_numeric")))
)

# 4) Count number of listings per category (optional order)
from pyspark.sql.functions import when as sf_when

category_counts = (
    with_category.groupBy("price_category").count()
    .withColumn(
        "order_key",
        sf_when(col("price_category") == "Budget", 0)
        .when(col("price_category") == "Mid-range", 1)
        .otherwise(2)
    )
    .orderBy("order_key")
    .drop("order_key")
)

# Quick check
with_category.select("id", "price", "price_numeric", "price_category").show(10, truncate=False)
category_counts.show(truncate=False)


# In[ ]:


# 2. In this task you will need to compute a santiment score per review, and then an average sentiment score per listing.
# A santiment score indicates how "positive" or "negative" a review is. The higher the score the more positive it is, and vice-versa.
#
# To compute a sentiment score per review compute the number of positive words in a review and subtract the number of negative
# words in the same review (the list of words is already provided)
#
# To complete this task, compute a DataFrame that contains the following fields:
# * name - the name of a listing
# * average_sentiment - average sentiment of reviews computed using the algorithm described above
from pyspark.sql.types import FloatType

# Lists of positive and negative words
positive_words = {'good', 'great', 'excellent', 'amazing', 'fantastic', 'wonderful', 'pleasant', 'lovely', 'nice', 'enjoyed'}
negative_words = {'bad', 'terrible', 'awful', 'horrible', 'disappointing', 'poor', 'hate', 'unpleasant', 'dirty', 'noisy'}

# TODO: Implement the UDF
def sentiment_score(comment):
    pass

sentiment_score_udf = udf(sentiment_score, FloatType())

reviews_with_sentiment = reviews \
  .withColumn(
    'sentiment_score',
    sentiment_score_udf(reviews.comments)
  )

# TODO: Create a final DataFrame


# In[ ]:


# 3. Rewrite the following code from the previous exercise using SparkSQL:
#
# ```
# from pyspark.sql.functions import length, avg, count
# 
# reviews_with_comment_length = reviews.withColumn('comment_length', length('comments'))
# reviews_with_comment_length \
#   .join(listings, reviews_with_comment_length.listing_id == listings.id, 'inner') \
#   .groupBy('listing_id').agg(
#       avg(reviews_with_comment_length.comment_length).alias('average_comment_length'),
#       count(reviews_with_comment_length.id).alias('reviews_count')
#   ) \
#   .filter('reviews_count >= 5') \
#   .orderBy('average_comment_length', ascending=False) \
#   .show()
# ```
# This was a solution for the the task:
#
# "Get top five listings with the highest average review comment length. Only return listings with at least 5 reviews"

reviews.createOrReplaceTempView("reviews")
listings.createOrReplaceTempView("listings")

# Write the SQL query
sql_query = """
...
"""

spark \
  .sql(sql_query) \
  .show()


# In[ ]:


# 4. [Optional][Challenge]
# Calculate an average time passed from the first review for each host in the listings dataset. 
# To implmenet a custom aggregation function you would need to use "pandas_udf" function to write a custom aggregation function.
#
# Documentation about "pandas_udf": https://spark.apache.org/docs/3.4.2/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html 
#
# To use "pandas_udf" you would need to install two additional dependencies in the virtual environment you use for PySpark:
# Run these commands:
# ```
# pip install pandas
# pip install pyarrow
# ```

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import PandasUDFType
import pandas as pd

@pandas_udf(DoubleType(), functionType=PandasUDFType.GROUPED_AGG)
def average_days_since_first_review_udf(first_review_series) -> float:
    # TODO: Implement the UDF
    pass

listings \
  .filter(
    listings.first_review.isNotNull()
  ) \
  .groupBy('host_id') \
  .agg(
    average_days_since_first_review_udf(listings.first_review).alias('average_days_since_first_review_days')
  ) \
  .show()

