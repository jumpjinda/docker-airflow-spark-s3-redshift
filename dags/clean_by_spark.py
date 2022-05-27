from operator import index
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *


import configparser
import logging


def clean_by_spark():
    logging.info("Reading AWS Credentials")
    config = configparser.ConfigParser()
    config.read_file(open('/opt/airflow/credentials/aws.cfg'))
    # config.read_file(open('dags/aws.cfg'))

    AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    logging.info("Creating SparkSession")
    conf = SparkConf() \
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0') \
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .set('spark.hadoop.fs.s3a.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY_ID) \
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_ACCESS_KEY) \


    spark = SparkSession.builder \
                        .master('local[*]') \
                        .config(conf=conf) \
                        .appName('test-spark') \
                        .getOrCreate()

    logging.info("Reading file")
    df = spark.read.csv('/opt/airflow/data/raw_files/DataEngineer.csv', header=True)
    # df = spark.read.csv('dags/DataEngineer.csv', header=True)

    logging.info("Checking data")
    df.limit(5).show()

    logging.info("Drop unnecessary columns")
    df = df.drop('Size') \
        .drop('Type of ownership') \
        .drop('Job Description') \
        .drop('Revenue') \
        .drop('Competitors') \
        .drop('Easy Apply') \
        .drop('Sector')

    logging.info("Rename columns to lowercase and replace white space to underscore")
    df = df.toDF(*(c.replace(' ', '_').lower() for c in df.columns))

    logging.info("Split city and state code from location, headquarters")
    df = df.withColumn('location_city', split(df['location'], ',').getItem(0)) \
                .withColumn('location_state', split(df['location'], ',').getItem(1)) \
                .withColumn('hq_city', split(df['headquarters'], ',').getItem(0)) \
                .withColumn('hq_state', split(df['headquarters'], ',').getItem(1)) \
                .drop('location', 'headquarters')

    logging.info("Rework salary_estimate column")
    df = df.withColumn('salary_estimate', regexp_replace('salary_estimate', '[$a-jl-zA-JL-Z.()]', '')) \
                .withColumn('salary_min', split('salary_estimate', '-').getItem(0)) \
                .withColumn('salary_min', regexp_replace('salary_min', 'K', '000')) \
                .withColumn('salary_max', split('salary_estimate', '-').getItem(1)) \
                .withColumn('salary_max', regexp_replace('salary_max', 'K', '000')) \
                .withColumn('salary_max', regexp_replace('salary_max', ' ', '')) \
                .drop('salary_estimate')

    df = df.withColumn('salary_min', df.salary_min.cast('int')) \
                .withColumn('salary_max', df.salary_max.cast('int')) \
                .withColumn('rating', df.rating.cast('float')) \
                .withColumn('founded', df.founded.cast('int'))

    logging.info("Remove rating from company_name column (last 4 characters)")
    df = df.withColumn('company_name',expr('substring(company_name, 1, length(company_name)-4)'))

    logging.info("Reorder columns")
    df = df.select('job_title', 'salary_min', 'salary_max', 'company_name', 'rating', 'location_city', 'location_state', \
                        'hq_city', 'hq_state', 'founded', 'industry')

    logging.info("Check and remove Null values")
    df = df.filter('job_title IS NOT NULL AND \
                salary_min IS NOT NULL AND \
                salary_max IS NOT NULL AND \
                company_name IS NOT NULL AND \
                rating IS NOT NULL AND \
                location_city IS NOT NULL AND \
                location_state IS NOT NULL AND \
                hq_city IS NOT NULL AND \
                hq_state IS NOT NULL AND \
                founded IS NOT NULL AND \
                industry IS NOT NULL')

    logging.info("Remove invalid values in columns (values are -1)")
    df = df.filter(df.job_title != '-1')
    df = df.filter(df.salary_min != -1)
    df = df.filter(df.salary_max != -1)
    df = df.filter(df.company_name != '-1')
    df = df.filter(df.rating != -1.0)
    df = df.filter(df.location_city != '-1')
    df = df.filter(df.location_state != '-1')
    df = df.filter(df.hq_city != '-1')
    df = df.filter(df.hq_state != '-1')
    df = df.filter(df.founded != -1)
    df = df.filter(df.industry != '-1')

    logging.info("Data quality check")
    logging.info("Check Null values")
    columns_list = df.columns
    print(columns_list)

    for column in columns_list:
        result = df.filter(column + ' is Null').count()
        if result == 0:
            print(f"{column} column has no Null value")
        else:
            print(f"{column} column has {result} Null value.")

    logging.info("Check invalid values (-1)")
    for column in columns_list:
        if column == 'salary_min':
            result = df.filter(f"{column} == -1").count()
            print(f"{column} column has {result} invalid value")
        if column == 'salary_max':
            result = df.filter(f"{column} == -1").count()
            print(f"{column} column has {result} invalid value")
        if column == 'rating':
            rresult = df.filter(f"{column} == -1.0").count()
            print(f"{column} column has {result} invalid value")
        if column == 'founded':
            result = df.filter(f"{column} == -1").count()
            print(f"{column} column has {result} invalid value")
        else:
            result = df.filter(f"{column} == '-1'").count()
            print(f"{column} column has {result} invalid value")

    logging.info("Save file")
    df.toPandas().to_csv('data/clean_files/DataEngineer.csv', index=False)

    logging.info("Check file")
    df = spark.read.csv('data/clean_files/DataEngineer.csv', header=True)

    df.count()

    df.limit(5).show()

    logging.info("Done clean_by_spark")
