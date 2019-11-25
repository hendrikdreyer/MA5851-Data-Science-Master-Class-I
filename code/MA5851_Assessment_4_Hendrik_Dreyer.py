# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 08:19:49 2019

@author: driku
"""

# Subject: MA5851 - Data Science Master Class I
# Author: Hendrik A. Dreyer
# Due Date: 3 December 2019

# Read data from file 'filename.csv' 
# (in the same directory that your python script is based)
import os

script_dir = os.getcwd()  #<-- absolute dir the script is in
rel_path = "..\data\hacker_news_post_process_tmp.csv"
abs_file_path = os.path.join(script_dir, rel_path)

#Load the post process data from Assessment 3 into a pandas dataframe
import pandas as pd

# Control delimiters, rows, column names with read_csv (see later) 
data = pd.read_csv(abs_file_path) 

# Preview the first 5 lines of the loaded data 
data.head()

#Shape of the loaded post-procssed data
data.shape

# Prepare the text corpus from the link_title field



#Initiate spark
import findspark
findspark.init()

#Import the neccessary libs
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Create a spark session
#spark = SparkSession.builder.master('yarn-client').appName('HN_Media_Titles').getOrCreate()
spark = SparkSession.builder.appName('HN_Media_Titles').getOrCreate()

schema = StructType([
    StructField("idx",          IntegerType(), True),
    StructField("id",           StringType(),  True),
    StructField("link_title",   StringType(),  True),
    StructField("web_link",     StringType(),  True),
    StructField("points",       IntegerType(), True)])

spark_file_path = abs_file_path

df = spark.read.format("csv").option("header", "true").load(spark_file_path)

data = spark.read.format("org.apache.spark.csv")\
                        .option("delimiter",",")\
                        .schema(schema)\
                        .option("mode", "PERMISSIVE")\
                        .option("inferSchema", "True")\
                        .csv(spark_file_path)
                        
df = spark.read.load("examples/src/main/resources/people.csv",
                     format="csv", 
                     sep=":", 
                     inferSchema="true", 
                     header="true")


from pyspark.sql.functions import col, lit
from functools import reduce
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import matplotlib.pyplot as plt
from wordcloud import WordCloud 
import pandas as pd
import re
import string