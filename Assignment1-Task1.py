# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext

def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

def correctRows(p):
    if(len(p) == 17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[5])!=0 and float(p[11])!=0):
                return p
            

sc = SparkContext()
#print("SparkContext is:",sc)
lines = sc.textFile(sys.argv[1])
lines.first()
taxilines = lines.map(lambda x: x.split(','))

taxilinesCorrected = taxilines.filter(correctRows)

#correct the RDD data as per the provided code
taxilines = lines.map(lambda x: x.split(','))    
taxilinesCorrected = taxilines.filter(correctRows)
# Create a new RDD with Taxi and Driver as tuples
taxi_driver = taxilinesCorrected.map(lambda x: (x[0],x[1]))

# Size of the dataset
#print("Size of the dataset is:",taxi_driver.count())

# Create a new RDD with distinct pairs of Taxi and Driver
taxi_driver_dist = taxi_driver.distinct()
# Count of the RDD after distinct
#print("Taxi and Driver distict combination count:",taxi_driver_dist.count())

#couting number of times taxi appears for distinct drivers
taxi_count = taxi_driver_dist.map(lambda x: (x[0],1))
taxi_driver_count = taxi_count.reduceByKey(lambda x, y: x+y)

# Taxis having the highest number of drivers
top_taxis = taxi_driver_count.takeOrdered(10, key=lambda x: -x[1])
sc.parallelize(top_taxis).saveAsTextFile(sys.argv[2])
print(top_taxis)

sc.stop()
