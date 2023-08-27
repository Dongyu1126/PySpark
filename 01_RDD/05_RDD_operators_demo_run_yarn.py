#codfing:utf8
from pyspark import SparkConf,SparkContext
import json
import os
os.environ['HADOOP_CONF_DIR']="/export/server/hadoop/etc/hadoop"


if __name__ == '__main__':
    #提交到yarn集群，master设置为yarn
    conf = SparkConf().setMaster("yarn").setAppName("test-yarn-1")
    sc = SparkContext(conf=conf)


    file_rdd = sc.textFile("hdfs://node1:8020/input/order.text")
    #进行rdd数据的切分，得到json数据
    jsons_rdd =file_rdd.flatMap(lambda line : line.split("|"))
    #
    #print(jsons_rdd.collect())
    # #同内置的json库，通过loads完成json字符串到字典对象的转换
    dict_rdd = jsons_rdd.map(lambda json_str : json.loads(json_str))
    #
    print(dict_rdd.collect())
    #过滤数据，只保留北京的数据
    beijing_rdd = dict_rdd.filter(lambda d : d['areaName'] == "北京")

    #组合北京和商品类型形成新的字符串
    category_rdd =  beijing_rdd.map(lambda x : x['areaName']+ "_" + x['category'])

    #对结果集进行去重操作
    result_rdd = category_rdd.distinct()

    print(result_rdd.collect())
