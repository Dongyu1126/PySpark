from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("hdfs://node1:8020/input/words.txt")
    rdd2 = rdd.flatMap(lambda x : x.split(" ")).map(lambda a : (a,1))

    #这里为什么没有收集器collect，因为这是Action算子，已经是结果了，不需要收集器了
    result = rdd2.countByKey()

    print(result)
    print(type(result))