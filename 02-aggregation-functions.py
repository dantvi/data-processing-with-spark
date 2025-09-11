#!/usr/bin/env python
# coding: utf-8

# In[9]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark aggregation functions") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# In[10]:


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


# In[11]:


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


# In[12]:


# 1. Count the number of reviews per listing using the "reviews" dataset


# In[13]:


# 2. Compute the total number of listings and average review score per host


# In[14]:


# 3: Find the top ten listings with the highest number of reviews


# In[15]:


# 4. Find the top five neighborhoods with the most listings


# In[16]:


# 5. Get a data frame with the following four columns:
# * Listing's ID
# * Listing's name
# * Reviewer's name
# * Review's comment
# Use "join" to combine data from two datasets


# In[17]:


# 6.Get top five listings with the highest average review comment length. Only return listings with at least 5 reviews
# Use the "length" function from the "pyspark.sql.functions" to get a lenght of a review


# In[18]:


# 7. Using the "join" operator find listings without reviews.
# Hint: Use "left_join" or "left_anti" join type when implementing this


# In[ ]:




