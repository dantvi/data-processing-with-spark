#!/usr/bin/env python
# coding: utf-8

# In[11]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark aggregation functions") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# In[2]:


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


# In[3]:


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


# In[4]:


# 1. Count the number of reviews per listing using the "reviews" dataset
from pyspark.sql.functions import col

# Count reviews per listing_id
reviews_per_listing = reviews.groupBy("listing_id").count() \
    .withColumnRenamed("count", "number_of_reviews") \
    .orderBy(col("number_of_reviews").desc())

print("Number of listings with at least one review:", reviews_per_listing.count())
reviews_per_listing.show(10, truncate=False)


# In[12]:


# 2. Compute the total number of listings and average review score per host
from pyspark.sql.functions import countDistinct, avg

# Compute number of listings and average review score per host
host_stats = listings.groupBy("host_id").agg(
    countDistinct("id").alias("total_listings"),
    avg("review_scores_rating").alias("avg_review_score")
).orderBy(col("total_listings").desc())

print("Number of hosts with at least one listing:", host_stats.count())
host_stats.show(10, truncate=False)


# In[6]:


# 3: Find the top ten listings with the highest number of reviews


# In[7]:


# 4. Find the top five neighborhoods with the most listings


# In[8]:


# 5. Get a data frame with the following four columns:
# * Listing's ID
# * Listing's name
# * Reviewer's name
# * Review's comment
# Use "join" to combine data from two datasets


# In[9]:


# 6.Get top five listings with the highest average review comment length. Only return listings with at least 5 reviews
# Use the "length" function from the "pyspark.sql.functions" to get a lenght of a review


# In[10]:


# 7. Using the "join" operator find listings without reviews.
# Hint: Use "left_join" or "left_anti" join type when implementing this


# In[ ]:




