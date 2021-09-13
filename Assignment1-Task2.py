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

#Task2
# driver time into a RDD
driver_time = taxilinesCorrected.map(lambda x: (x[1],int(x[4])))
driver_time_sum = driver_time.reduceByKey(lambda x,y: x+y)
driver_time_sum.count()

# there are a few records where time value is 0. So, filtering them out 
driver_time_sum = driver_time_sum.filter(lambda x: x[1]!=0)
driver_time_sum.count()

# Driver money into RDD
driver_money = taxilinesCorrected.map(lambda x: (x[1],float(x[16])))
driver_money_sum = driver_money.reduceByKey(lambda x,y: x+y)
driver_money_sum.count()

# Join the two RDDs to get driver time and money into one RDD
driver_money_time = driver_money_sum.join(driver_time_sum)
driver_money_time.count()

# driver money earned per minute
driver_money_per_time = driver_money_time.map(lambda x: (x[0],x[1][0]*60.0/x[1][1]))
# top 10 drivers with highest money per minute
top_drivers = driver_money_per_time.takeOrdered(10, key=lambda x: -x[1])
top_drivers = [x[0] for x in top_drivers]
sc.parallelize(top_drivers).saveAsTextFile(sys.argv[2])
print(top_drivers)

sc.stop()
