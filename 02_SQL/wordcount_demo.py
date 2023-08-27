# coding:utf8
# 演示sparksql wordcount
from pyspark.sql import SparkSession
# 导入StructType对象
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    #SQL风格处理
    rdd = sc.textFile("hdfs://node1:8020/input/words.txt").\
        flatMap(lambda x : x.split(" ")).\
        map(lambda y : [y])  #必须弄成中括号套一个的这些格式

    #转换RDD到df 设置列名
    df = rdd.toDF(["word"])

    #df注册一个临时视图（表）
    df.createTempView("words")

    spark.sql("select word,count(*) as cnt from words group by word order by cnt desc ").show()


    #DSL风格
    df = spark.read.format("text").load("hdfs://node1:8020/input/words.txt")

    # 通过withColumn方法 对一个列进行操作
    # 方法功能: 对老列执行操作, 返回一个全新列, 如果列名一样就替换, 不一样就拓展一个列
    # 重命名函数withColumnRenamed
    df2 = df.withColumn("value",F.explode(F.split(df['value']," ")))
    df2.groupby("value").\
        count(). \
        withColumnRenamed("value","word").\
        withColumnRenamed("count","cnt").\
        orderBy("cnt",ascending=False).\
        show()

