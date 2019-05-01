# -*- coding:utf-8 -*-
#showing remote messages

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    sc = SparkContext("local[3]", "streamwordcount")

    # 创建本地的SparkContext对象，包含3个执行线程

    ssc = StreamingContext(sc, 2)
    # 创建本地的StreamingContext对象，处理的时间片间隔时间，设置为2s

    lines = ssc.socketTextStream("localhost", 9999)

    words = lines.flatMap(lambda line: line.split(" "))
    # 使用flatMap和Split对2秒内收到的字符串进行分割

    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)



wordCounts.pprint()

ssc.start() 
# 启动Spark Streaming应用

ssc.awaitTermination()
