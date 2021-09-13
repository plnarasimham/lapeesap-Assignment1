# pyspark-template

This is a pyspark assignment


## Describe here your project


This is Assignment 1. There are 2 tasks and there are 2 python files corresponding to each of these tasks. The 1st task tries to find out the top 10 taxis that have the higher number of drives associated with them. The output from the 1st task is 10 tuples each containing a taxi Id and number of drivers.
The 2nd task finds the Top 10 drivers in terms of their earnings per minute. The output of the 2nd task is a list of top 10 drivers.


# How to run  

Task 1:

1) Run the task 1 by submitting the task to spark-submit on the local machine using the small version of input file

The command would be 

spark-submit Assignment1-Task1.py taxi-data-sorted-small.csv output-task1.txt

2) Run it on google cloud

THe configuration would be as follows:
Code name: gs://lapeesap_assignment1/Assignment1-Task1.py

Arguments:
gs://metcs777/taxi-data-sorted-large.csv.bz2
gs://lapeesap_assignment1/Output-Task1

Task 2:

1) Run the task 2 by submitting the task to spark-submit on the local machine using the small version of input file

The command would be 

spark-submit Assignment1-Task2.py taxi-data-sorted-small.csv output-task2.txt

2) Run it on google cloud

THe configuration would be as follows:
Code name: gs://lapeesap_assignment1/Assignment1-Task2.py

Arguments:
gs://metcs777/taxi-data-sorted-large.csv.bz2
gs://lapeesap_assignment1/Output-Task2




