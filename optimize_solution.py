"""
Optimize the query plan
Suppose we want to compose query in which we get for each question also the number of answers to this question for each month.
See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, broadcast

spark = SparkSession.builder.appName('Optimize I').getOrCreate()

# base_path = os.getcwd()

# project_path = ('/').join(base_path.split('/')[0:-3])

# answers_input_path = os.path.join(project_path, 'data/answers')

# questions_input_path = os.path.join(project_path, 'output/questions-transformed')

downloads_dir_path = "C:/Users/aswa8/OneDrive - Queen's University/Desktop/Springboard/20 - Apache Spark/Spark-Mini-Projects/Optimization/data/"
questions_dd_path = downloads_dir_path + "questions/"
answers_dd_path = downloads_dir_path + "answers/"

answersDF = spark.read.option('path', answers_dd_path).load()

questionsDF = spark.read.option('path', questions_dd_path).load()

# These two dataframes will be joined in the following problem:

'''
Answers aggregation
Here we : get number of answers per question per month
'''

# Code given before improvments
start_time = time.time()

answers_month = answersDF.withColumn('month', month('creation_date'))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("-- %s seconds to process --" % (time.time() - start_time))

'''
Task:
See the query plan of the previous result and rewrite the query to optimize it.
'''

resultDF.explain()

# Improvement 1: Repartitioning

start_time = time.time()

spark.conf.set("spark.sql.adaptive.enabled","true")

answers_month = answersDF.withColumn('month', month('creation_date'))

# Repartition by month
answers_month = answers_month.repartition(col("month"))

answers_month = answers_month.groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("-- %s seconds to process --" % (time.time() - start_time))

resultDF.explain()

# Improvement 2: Cache

start_time = time.time()

spark.conf.set("spark.sql.adaptive.enabled","true")

answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

answers_month.cache()

resultDF = questionsDF.join(broadcast(answers_month), 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

print("-- %s seconds to process --" % (time.time() - start_time))

resultDF.explain()