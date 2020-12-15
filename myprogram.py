'''
Created on Nov 18, 2020
@author: sparktacus4real
'''


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *


#-------------------
if __name__ == "__main__":
    print('s3 Redshift - read write data using Pyspark \n')

    myconf = (SparkConf()\
        .setMaster("spark://pclocalhost.home:7077")\
        .setAppName("app_pysparks3redshift")\
        .set("spark.executor.memory","2g"))

    myconf.set("spark.driver.memory","1g")
    myconf.set("spark.jars.packages","com.amazon.redshift:redshift-jdbc42-no-awssdk:1.2.45.1069,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-auth:2.7.4,org.apache.hadoop:hadoop-common:2.7.4,com.google.code.findbugs:jsr305:3.0.2,asm:asm:3.2,org.slf4j:slf4j-api:1.7.30,org.xerial.snappy:snappy-java:1.1.7.5,org.slf4j:slf4j-log4j12:1.7.30,org.apache.hadoop:hadoop-aws:2.7.3")
    myconf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    myconf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")

    sc = SparkContext(conf = myconf)
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
    
    #aws key - setup
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", "<your_aws_access_key>")
    hadoopConf.set("fs.s3a.secret.key", "<your_aws_SecretAccess_key>")
    hadoopConf.set("fs.s3a.endpoint", "<your_aws_region>")
    hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    #create SparkSession
    sql = SparkSession(sc)
    
    #data collect: dataset1 'users informations'
    dfschema = StructType([
        StructField("client_id", StringType(), True),
        StructField("username", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", IntegerType(), True)])
    
    df = sql.read.option("header",True).csv("s3a://mybucket-test2/users_info/user_info.csv")
    
    #data collect: dataset2 'sales'
    df2schema = StructType([
        StructField("id", StringType(), True),
        StructField("sales", IntegerType(), True),])
    
    df2 = sql.read.option("header",True).csv("s3a://mybucket-test2/sales/sales_per_user.csv")
    
    #data collect: dataset2 'purchases'
    df3= sql.read.option("header",True).csv("s3a://mybucket-test2/purchases/")
    
    #data Aggregation and Transformation
    df.createOrReplaceTempView("dftemp")
    df2.createOrReplaceTempView("df2temp")
    df3.createOrReplaceTempView("df3temp")
    df_join = sql.sql("SELECT A.client_id, A.username, A.city, A.country, A.phone, IFNull(B.sales,0) AS sales, IFNull(C.purchases,0) AS purchases FROM dftemp A LEFT JOIN df2temp B ON A.client_id = B.client_id LEFT JOIN df3temp C ON A.client_id = C.client_id")
    
    #write data to aws DB Redshift table
    df_join.write \
        .format("jdbc") \
        .option("url", "jdbc:redshift://<your_redshift_clustername>.eu-west-3.redshift.amazonaws.com:<port>/<your_redshift_database_name>") \
        .option("dbtable","pyspark.users_sales_purchases") \
        .option("UID","<redshift_db_username>") \
        .option("PWD","<redshift_db_password>") \
        .option("driver","com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()
        
        
    print('---job done successfully!--')
