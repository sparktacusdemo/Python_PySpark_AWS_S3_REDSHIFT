'''
Created on Nov 18, 2020
@author: root
'''

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.types import *
import csv

#-------------------
if __name__ == "__main__":
    print('s3 pyspark read write data\n')

    myconf = (SparkConf()\
        .setMaster("spark://pclocalhost.home:7077")\
        .setAppName("pysparks3")\
        .set("spark.executor.memory","2g"))

    myconf.set("spark.driver.memory","1g")
    
    myconf.set("spark.jars.packages","com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3")
    #com.amazon.redshift:redshift-jdbc42:1.2.43.1067
    myconf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    myconf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    
    #com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.47.1071
    #com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.43.1067,
    #com.amazonaws:aws-java-sdk:1.7.4,com.amazonaws:aws-java-sdk:1.11.120,com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,
    
    #myconf.set("spark.jars.packages","hdfs://localhost:9000/aws-java-sdk-s3-1.11.903.jar")
    #com.amazonaws:aws-java-sdk-s3:1.11.863,
    #spark = SparkSession\
     #   .builder\
      #  .config(conf=myconf)\
       # .getOrCreate()

    #aws key
    sc = SparkContext(conf = myconf)
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    
    #sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    #sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIATVDQCDRS5SJAV3FH")
    #sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "")
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "AKIATVDQCDRS5SJAV3FH")
    hadoopConf.set("fs.s3a.secret.key", "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")
    hadoopConf.set("fs.s3a.endpoint", "s3.eu-west-3.amazonaws.com")
    hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    #hadoopConf.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")

    
    #df = SQLContext(sc).read.parquet('s3a://mybucket-test2/users_info/userdata1.parquet')
    #df = spark.read.option("header",True).csv('hdfs://localhost:9000/iris.csv')
    sql = SparkSession(sc)

    dfschema = StructType([
        StructField("client_id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", IntegerType(), True)])
    #print(dfschema)
    #df = sql.read.option("header",True).csv("s3a://mybucket-test2/users_info/user_info.csv")
    df = sql.read.option("header",True).csv("s3a://mybucket-test2/users_info/user_info.csv")
    

    df2schema = StructType([
        StructField("id", StringType(), True),
        StructField("sales", IntegerType(), True),])
    
    #df2 = sql.read.parquet("s3a://mybucket-test2/users_info/userdata1.parquet")
    df2 = sql.read.option("header",True).csv("s3a://mybucket-test2/sales/sales_per_user.csv")
    #df2 = sql.read.schema(df2schema).csv()
    #df2=sql.read.format('csv').options(header='false').schema(df2schema).load("s3a://mybucket-test2/sales/sales_per_user-3.csv")
    df3= sql.read.option("header",True).csv("s3a://mybucket-test2/purchases/")
    #print(df)
    df.show()
    df2.show()
    df3.show()
    
    df.createOrReplaceTempView("dftemp")
    df2.createOrReplaceTempView("df2temp")
    df3.createOrReplaceTempView("df3temp")
    
    df_join = sql.sql("SELECT A.client_id, A.username, A.city, A.country, A.phone, IFNull(B.sales,0) AS sales, IFNull(C.purchases,0) AS purchases FROM dftemp A LEFT JOIN df2temp B ON A.client_id = B.client_id LEFT JOIN df3temp C ON A.client_id = C.client_id")
    df_join.show()
    
    #save dataframe data in S3 bucket , csv format
    #df_join.coalesce(1).write.csv("s3a://mybucket-test2/output_11/output/",header=True) 
    
    df_join.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://kleevirkun-cluster-201120.cubhkxle5woj.eu-west-3.redshift.amazonaws.com:5439/dev") \
        .option("dbtable","pyspark.users_sales_purchases") \
        .option("UID","awsuser") \
        .option("PWD","Admin20!") \
        .option("driver","com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()
        
    
    print('-- ok')
