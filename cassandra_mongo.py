from pyspark import SparkContext, SparkConf
from pyspark_cassandra import CassandraSparkContext, Row
from pymongo import MongoClient
from cassandra.cluster import Cluster
import pymongo_spark
import time
import recomm_cf

LOAD_DATA_FROM_DB = True
SAVE_DATA_TO_DB = True

pymongo_spark.activate()
db_out = 'test_database'
spark_cassandra_connection_host = "127.0.0.1"
cassandra_keyspace = "mykeyspace"
cassandra_table = "transactions"
dbtable_out_per_user = 'recomm_per_user'
dbtable_out_per_item = 'recomm_per_item'
    
col_tenant_id = 'tenant_id'
col_user_id = 'user_id'
col_item_id = 'item_id'

    
def save_to_mongo(rdd, collection):
    rdd.saveToMongoDB('mongodb://localhost:27017/' + db_out + '.' + collection)
    print 'saved to ' + collection

if __name__ == "__main__":
    
    t0 = time.time()
    
    mongo_client= MongoClient()
    mongo_client.drop_database(db_out)
    # print 'database cleared'


    num_to_recomm_per_user = 10
    num_to_recomm_per_item = 10
    conf = SparkConf().setAppName("PysparkCollaborativeFiltering")
    print 'conf'
    sc = CassandraSparkContext(conf=conf)
    sc.setCheckpointDir('checkpoint/')
    
    if LOAD_DATA_FROM_DB:
        
        data_rdd = sc.cassandraTable(cassandra_keyspace, cassandra_table) # row_format: Row
        # print data

        t1 = time.time()
        tenant_ids = data_rdd.map(lambda trans:trans[col_tenant_id]).distinct().collect()
        elapsed = (time.time() - t0)
        print ("\nIt took %.2fsec to complete" % elapsed)

        t1 = time.time()
        cluster = Cluster()
        session = cluster.connect(cassandra_keyspace)
        string = 'SELECT DISTINCT ' + col_tenant_id + ' from ' +  cassandra_table
        tenant_ids = ession.execute(string)
        elapsed = (time.time() - t0)
        print ("\nIt took %.2fsec to complete" % elapsed)

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

            formatted_rdd_per_user = recomm_per_user.map(lambda row: (t_id,row[0],row[1]))
            all_results_per_user = all_results_per_user.union(formatted_rdd_per_user)
            formatted_rdd_per_item = recomm_per_item.map(lambda row: (t_id,row[0],row[1]))
            all_results_per_item = all_results_per_item.union(formatted_rdd_per_item)
                
        if SAVE_DATA_TO_DB:
            save_to_mongo(all_results_per_user,dbtable_out_per_user)
            save_to_mongo(all_results_per_item,dbtable_out_per_item)
        else:
            print("%d recommendations per user:" % num_to_recomm_per_user)
            print(all_results_per_user.collect())
            print("%d recommendations per item:" % num_to_recomm_per_item)
            print(all_results_per_user.collect())
    else:
        print ('loading...')
        datafile = sc.textFile("data2.csv").map(lambda l: l.split(','))
        recomm_per_user,recomm_per_item = recomm_cf.TrainAndComputeRecommendation(sc, datafile,
                                                                        num_to_recomm_per_user,
                                                                        num_to_recomm_per_item)

        if SAVE_DATA_TO_DB:
            save_to_mongo(recomm_per_user,dbtable_out_per_user)
            save_to_mongo(recomm_per_item,dbtable_out_per_item)
        else:
            print("%d recommendations per user:" % num_to_recomm_per_user)
            print(recomm_per_user.collect())
            print("%d recommendations per item:" % num_to_recomm_per_item)
            print(recomm_per_item.collect())
            
    elapsed = (time.time() - t0)
    sc.stop()
    print ("\nIt took %.2fsec to complete" % elapsed) 

