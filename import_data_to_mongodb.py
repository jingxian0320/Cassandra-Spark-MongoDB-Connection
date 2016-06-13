import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient

#CSV to JSON Conversion
csvfile = open('..\data2.csv', 'r')
reader = csv.DictReader( csvfile,fieldnames= [ "CustomerID", "ProductID", "Score"])

mongo_client=MongoClient() 
db=mongo_client.test_database

db.transactions.drop()
header= [ "CustomerID", "ProductID", "Score"]

for each in reader:
    row={}
    for field in header:
        row[field]=each[field]
    db.transactions.insert_one(row)
transaction = db.transactions.find().limit(10)
for record in transaction:
    print record

print ("%d transaction records are saved into the database"%db.transactions.count())
