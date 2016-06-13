from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
import pymongo_spark

# Important: activate pymongo_spark.
def activate():
    pymongo_spark.activate()
    print 'activated'

def clear_collections():
    mongo_client= MongoClient() 
    db = mongo_client.test_database
    db.recomm_per_user.drop()
    db.recomm_per_item.drop()
    mongo_client.close()
    print 'cleared'


def save(rdd, collection):
    rdd.saveToMongoDB('mongodb://localhost:27017/'+collection)
    print 'saved'
