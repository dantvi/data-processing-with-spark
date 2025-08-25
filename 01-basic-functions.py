#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Inside Airbnb data") \
    .getOrCreate()


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
print("Rows:", listings.count())


# In[3]:


listings.printSchema()


# In[4]:


# 1. Get a non-null picture URL for any property ("picture_url" field)
# Select any non-null picture URL
from pyspark.sql.functions import col

non_null_pictures = listings.filter(col("picture_url").isNotNull()) \
                            .select("picture_url")

non_null_pictures.show(10, truncate=False)


# In[16]:


# 2. Get number of properties that get more than 10 reviews per month
num_props = listings.filter(col("reviews_per_month") > 10).count()
print("Number of properties with > 10 reviews/month:", num_props)


# In[17]:


# 3. Get properties that have more bathrooms than bedrooms
more_bath_than_bed = listings.filter(col("bathrooms") > col("bedrooms"))

print("Properties with more bathrooms than bedrooms:", more_bath_than_bed.count())
more_bath_than_bed.select("id", "name", "bathrooms", "bedrooms").show(10, truncate=False)


# In[20]:


listings.select("price").where(col("price").isNotNull()).show(10, truncate=False)


# In[21]:


# 4. Get properties where the price is greater than 5,000. Collect the result as a Python list
# Remember to convert a price into a number first!
from pyspark.sql.functions import regexp_replace

# Convert price from string to float
listings_num = listings.withColumn(
    "price_num",
    regexp_replace(col("price"), "[$,]", "").cast("double")
)

# Filter on price > 5000
expensive_props = listings_num.filter(col("price_num") > 5000)

# Collect results as a Python list
result = expensive_props.select("id", "name", "price_num").collect()

print("Number of properties with price > 5000:", len(result))
print(result[:5])


# In[8]:


# 5. Get a list of properties with the following characteristics:
# * price < 150
# * more than 20 reviews
# * review_scores_rating > 4.5
# Consider using the "&" operator


# In[9]:


# 6. Get a list of properties with the following characteristics:
# * price < 150 OR more than one bathroom
# Use the "|" operator to implement the OR operator


# In[10]:


# 7. Get the highest listing price in this dataset
# Consider using the "max" function from "pyspark.sql.functions"


# In[11]:


# 8. Get the name and a price of property with the highest price
# Try to use "collect" method to get the highest price first, and then use it in a "filter" call 


# In[12]:


# 9. Get the number of hosts in the dataset


# In[13]:


# 10. Get listings with a first review in 2024
# Consider using the "year" function from "pyspark.sql.functions"


# In[ ]:




