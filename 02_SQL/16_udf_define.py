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

    #构建rdd
    rdd = sc.parallelize([1,2,3,4,5,6,7]).map(lambda x : [x])

    df = rdd.toDF(["num"])

    #TOOO 1:方式1 ： sparksession.udf.register(参数1，参数2，参数3)
    def num_ride_10(num):
        return num * 10

    #参数1 ： 注册的udf的名称，仅可以用于sql风格
    #参数2 ： udf的处理逻辑，一个单独的方法
    #参数3 ： udf的返回值类型
    #返回值对象udf2 ：仅可以用于dsl语句

    udf2 = spark.udf.register("udf1",num_ride_10,IntegerType())

    #sql风格中的使用
    #selectExpr()以sql风格的表达式执行（字符串）
    #select方法：接受普通的字符串字符名，或返回值是Column对象的计算
    df.selectExpr("udf1(num)").show()

    #DSL风格
    #返回值对象UDF对象，传入的参数一定是Column对象
    df.select(udf2(df["num"])).show()

    # TOOO 2:方式2 ： F.udf(参数1， 参数2)
    #这里只有udf3也只能用于wsl风格
    udf3 = F.udf(num_ride_10,IntegerType())

    df.select(udf3(df["num"])).show()