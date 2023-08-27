from  pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    rdd = sc.wholeTextFiles("../tmp/pycharm_project_960/data/input/tiny_files")
    print(rdd.collect())