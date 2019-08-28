import sys
import re
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


def FilteringOnCoactor(line: str):
    
    ListofActors = str(line.split(",")[2])
    if "Tyson Ritter" in ListofActors:
        return line
    else:
        return "waste"
   

def savetheresult(rdd):
   
    list_from_RDD=rdd.collect()
    with open('Streameddata.csv', 'a') as file:
        if len(list_from_RDD) != 0 and list_from_RDD[0] != "waste":
            file.write(list_from_RDD[0])
            file.write("\n")
    list_from_RDD = list()

if __name__ == '__main__':

    conf = SparkConf().setAppName("Movies").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 1)
   
    stream_data = ssc.textFileStream("file:////Users/tayyaba.younas/Desktop/sparkAssignment/data") 
    filter_data = stream_data.filter(lambda line: "Pierre Morel" in line)
    mapped_data = filter_data.map(FilteringOnCoactor)
    mapped_data.pprint()
    mapped_data.foreachRDD(savetheresult)    
ssc.start()
ssc.awaitTermination()
