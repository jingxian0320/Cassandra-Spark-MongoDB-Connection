from pyspark import SparkContext, SparkConf
from pyspark_cassandra import CassandraSparkContext, Row
from pymongo import MongoClient
import pymongo_spark
import time
import recomm_cf

LOAD_DATA_FROM_DB = True
SAVE_DATA_TO_DB = True

pymongo_spark.activate()
db_out = 'test_database'
spark_cassandra_connection_host = "127.0.0.1"
dbtable_out_per_user = 'recomm_per_user'
dbtable_out_per_item = 'recomm_per_item'
    
    
def save_to_mongo(rdd, collection):
    rdd.saveToMongoDB('mongodb://localhost:27017/' + db_out + '.' + collection)
    print 'saved to ' + collection

if __name__ == "__main__":
    
    t0 = time.time()
    
    mongo_client= MongoClient()
    mongo_client.drop_database(db_out)
    mongo_client.close()
    print 'database cleared'
    
    col_tenant_id = 1
    col_user_id = 2
    col_item_id = 3

    num_to_recomm_per_user = 10
    num_to_recomm_per_item = 10
    
    
    conf = SparkConf().setAppName("PysparkCollaborativeFiltering").set("spark.cassandra.connection.host", spark_cassandra_connection_host)
    sc = CassandraSparkContext(conf=conf)
    sc.setCheckpointDir('checkpoint/')
    data = sc.cassandraTable("mykeyspace", "transactions",row_format=1).collect() # row_format: tuple
    # (id, tenant_id, user_id, item_id)
    tenant_ids = set(list(map(lambda x:x[col_tenant_id],data)))
    data_rdd = sc.parallelize(data)
    # data_rdd = sc.parallelize(data).map(list)
    
    all_results_per_user = sc.emptyRDD()
    all_results_per_item = sc.emptyRDD()
    
    for t_id in tenant_ids:
        print("\nComputing recommendation for tenant {}...\n".format(t_id))
        per_tenant_rdd = data_rdd.filter(
            lambda x: x[col_tenant_id] == t_id).map(
            lambda l: ((l[col_user_id],l[col_item_id]),1.0)).reduceByKey(
            lambda x,y: x + y).map(
            lambda x: (x[0][0],x[0][1],x[1]))
        recomm_per_user,recomm_per_item = recomm_cf.TrainAndComputeRecommendation(sc, per_tenant_rdd,
                                                                        num_to_recomm_per_user,
                                                                        num_to_recomm_per_item)

        formatted_rdd_per_user = recomm_per_user.map(lambda row: ((t_id,row[0]),row[1]))
        all_results_per_user = all_results_per_user.union(formatted_rdd_per_user)
        formatted_rdd_per_item = recomm_per_item.map(lambda row: ((t_id,row[0]),row[1]))
        all_results_per_item = all_results_per_item.union(formatted_rdd_per_item)
        
    save_to_mongo(all_results_per_user,dbtable_out_per_user)
    save_to_mongo(all_results_per_item,dbtable_out_per_item)

            
    elapsed = (time.time() - t0) 
    print ("\nIt took %.2fsec to complete" % elapsed) 

