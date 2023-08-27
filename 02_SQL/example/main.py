#coding : utf8

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.storagelevel import StorageLevel
"""
需求1：各省销售额统计
需求2：Top3销售省中，有多少店铺达到销售额1000+
需求3：Top3销售省中，各省平均单价
需求4：Top3销售省中，各省支付类型比例

receivable 订单金额
storeProvince 店铺省份
dataTs 订单销售日期
payType 支付类型
storeID 店铺ID
"""

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("SparkSQL Example").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions","2").\
        getOrCreate()

    #1.读取数据  清洗数据
    #省份信息过滤，缺失值过滤，同时省份信息中，会有‘null’字符串
    #订单的金额，订单金额超过10000的，属于测试数据
    #列值截取（SparkSQL会自动优化）
    df = spark.read.format("json").load("hdfs://node1:8020/input/mini.json").\
        dropna(thresh=1,subset=['storeProvince']).\
        filter("storeProvince != 'null'").\
        filter("receivable < 10000").\
        select("storeProvince","storeID","receivable","dataTS","payType")

    #TOOD1 需求1：各省 销售额统计
    province_sale_df = df.groupby("storeProvince").sum("receivable").\
        withColumnRenamed("sum(receivable)","money").\
        withColumn("money",F.round("money",2)).\
        orderBy("money",ascending=False)
    # truncate=False 显示全部
    province_sale_df.show(truncate=False)

    #TOOD2 需求2 ： Top3省份中，有多少店铺达到日销售额1000
    #2.1先找到Top3省份
    top3_province_df = province_sale_df.limit(3).select("storeProvince").\
        withColumnRenamed("storeProvince" , "top3_province")  #起个别名，防止歧义

    #2.2 和原始表关联，显示需要的数据，即top3省份的数据全部留下
    top3_province_df_joined = df.join(top3_province_df,on=df['storeProvince'] == top3_province_df['top3_province'])

    #这个变量还要有，所以缓存一下
    top3_province_df_joined.persist(StorageLevel.MEMORY_AND_DISK)

    province_hot_sale_count_df= top3_province_df_joined.groupBy("storeProvince","storeID",  #时间戳转变为日期 from_unixtime精度是秒级，数据的精度是毫秒级，对数据进行精度的裁剪
                                    F.from_unixtime(df['dataTS'].substr(0,10),"yyyy-MM-dd").alias("day")).\
        sum("receivable").withColumnRenamed("sum(receivable)","money").\
        filter("money > 1000").\
        dropDuplicates(subset=["storeID"]).groupBy("storeProvince").count()
    #  dropDuplicates 对storeID进行去重

    province_hot_sale_count_df.show()

    #TOOD3 需求3 ： Top3省份中，各个省份的平均订单价格（单单）
    top3_province_order_avg_df=top3_province_df.groupBy("storeProvince").\
        avg("receivable").\
        withColumnRenamed("avg(receivable)","money").\
        withColumn("money",F.round("money",2)).\
        orderBy("money",ascending=False)

    top3_province_order_avg_df.show(truncate=False)
    ##降序排序

    #TOOD4 需求4 ： Top3省份中，各个省份的支出比例

    #注册一个百分数的udf
    def udf_func(percent):
        return str(round(percent * 100 , 2)) + "%"

    #注册udf,返回String类型
    my_udf = F.udf(udf_func,StringType)

    top3_province_df_joined.createTempView("province_pay")

    pay_type_df = spark.sql("""
    select storeProvince,payType ,( count(payType) / total ) as percent 
    (select storeProvince,payType , count(1) over(PARTITION By storeProvince)  as total from province_pay) as sub
    group by storeProvince,payType ,total 
    
    """).withColumn("percent",my_udf("percent"))

    pay_type_df.show()

    #变量缓存解除
    top3_province_df_joined.unpersist()