from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    #A través del sparkcontext solo manejamos el rdd

    def ejemplo0():
        #A través de sparkconf
        sc = SparkContext(conf=SparkConf().setAppName('learning').setMaster('local'))
        assert sc != None

    def ejemplo1():
        #Sin spark conf
        sc = SparkContext(master="local", appName="learning")
        assert sc != None

    ejemplo0()
    ejemplo1()
