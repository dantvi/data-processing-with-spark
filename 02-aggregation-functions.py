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


# In[5]:


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

# Count reviews per listing_id and join to get listing name
top10_reviews = (
    reviews.groupBy("listing_id").count()
    .withColumnRenamed("count", "number_of_reviews")
    .join(
        listings.select(col("id").alias("listing_id"), "name"),
        on="listing_id",
        how="left"
    )
    .orderBy(col("number_of_reviews").desc(), col("listing_id").asc())
)

print("Top 10 listings by number of reviews:")
top10_reviews.select("listing_id", "name", "number_of_reviews").show(10, truncate=False)


# In[7]:


# 4. Find the top five neighborhoods with the most listings

top5_neighbourhoods = (
    listings
        .filter(col("neighbourhood_cleansed").isNotNull())
        .groupBy("neighbourhood_cleansed")
        .count()
        .withColumnRenamed("count", "listings_count")
        .orderBy(col("listings_count").desc(), col("neighbourhood_cleansed").asc())
)

print("Top 5 neighbourhoods by listings:")
top5_neighbourhoods.show(5, truncate=False)


# In[12]:


# 5. Get a data frame with the following four columns:
# * Listing's ID
# * Listing's name
# * Reviewer's name
# * Review's comment
# Use "join" to combine data from two datasets
from pyspark.sql.functions import trim

# Select only needed columns
listings_sel = listings.select(col("id").alias("listing_id"), "name")
reviews_sel = reviews.select("listing_id", "reviewer_name", "comments") \
    .filter(col("comments").isNotNull() & (trim(col("comments")) != ""))

# Inner join on listing_id
joined_df = listings_sel.join(reviews_sel, on="listing_id", how="inner")

# Final selection
result_df = joined_df.select("listing_id", "name", "reviewer_name", "comments")

print("Sample of listing/review combinations:")
result_df.show(10, truncate=False)


# In[13]:


# 6.Get top five listings with the highest average review comment length. Only return listings with at least 5 reviews
# Use the "length" function from the "pyspark.sql.functions" to get a lenght of a review
from pyspark.sql.functions import length, count

# Use only non-empty comments
reviews_len = reviews.select("listing_id", "comments") \
    .filter(col("comments").isNotNull() & (trim(col("comments")) != "")) \
    .withColumn("comment_length", length(col("comments")))

# Aggregate per listing: average length + review count
agg_len = reviews_len.groupBy("listing_id").agg(
    avg("comment_length").alias("avg_comment_length"),
    count("*").alias("review_count")
).filter(col("review_count") >= 5)

# Join listing names and get top 5 by avg length
top5_longest = agg_len.join(
    listings.select(col("id").alias("listing_id"), "name"),
    on="listing_id",
    how="left"
).orderBy(col("avg_comment_length").desc(), col("listing_id").asc())

print("Top 5 listings by average review comment length (min 5 reviews):")
top5_longest.select("listing_id", "name", "avg_comment_length", "review_count").show(5, truncate=False)


# In[10]:


# 7. Using the "join" operator find listings without reviews.
# Hint: Use "left_join" or "left_anti" join type when implementing this


# In[ ]:




