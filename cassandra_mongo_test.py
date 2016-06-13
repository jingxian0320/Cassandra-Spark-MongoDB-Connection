from pyspark import SparkContext, SparkConf
from pyspark_cassandra import CassandraSparkContext, Row
from pymongo import MongoClient
import pymongo_spark
import time
import recomm_cf

LOAD_DATA_FROM_DB = False
SAVE_DATA_TO_DB = False

pymongo_spark.activate()
db_out = 'test_database'
spark_cassandra_connection_host = "127.0.0.1"
cassandra_keyspace = "mykeyspace"
cassandra_table = "transactions_test"
dbtable_out_per_user = 'recomm_per_user'
dbtable_out_per_item = 'recomm_per_item'

test_file = 'transaction_data.csv'
    
    
def save_to_mongo(rdd, collection):
    rdd.saveToMongoDB('mongodb://localhost:27017/' + db_out + '.' + collection)
    print 'saved to ' + collection



def test(data_rdd,num_to_recomm_per_user=10,num_to_recomm_per_item=10):
    results_per_user = data_rdd.map(
        lambda l:((l[col_tenant_id],l[col_user_id]),l[col_item_id])).reduceByKey(
        lambda x,y: x.append(y)).map(
        lambda l:(l[0][0],l[0][1],l[1]))
    results_per_item = data_rdd.map(
        lambda l:(l[col_tenant_id],l[col_item_id]),[])
    return results_per_user,results_per_item



    
if __name__ == "__main__":
    
    t0 = time.time()
    
    mongo_client= MongoClient()
    mongo_client.drop_database(db_out)
    print 'database cleared'

    
    col_tenant_id = 1
    col_user_id = 2
    col_item_id = 3

    num_to_recomm_per_user = 10
    num_to_recomm_per_item = 10
    conf = SparkConf().setAppName("PysparkCollaborativeFiltering").set("spark.cassandra.connection.host", spark_cassandra_connection_host)
    print ('conf')
    sc = CassandraSparkContext(conf=conf)
    sc.setCheckpointDir('checkpoint/')
    
    if LOAD_DATA_FROM_DB:
        
        data = sc.cassandraTable(cassandra_keyspace, cassandra_table, row_format=1).collect() # row_format: tuple
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
            recomm_per_user,recomm_per_item = test(sc, per_tenant_rdd,num_to_recomm_per_user,num_to_recomm_per_item)

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
        data_rdd = sc.textFile(test_file).map(lambda l: l.split(','))
        recomm_per_user,recomm_per_item = test(data_rdd, num_to_recomm_per_user,num_to_recomm_per_item)

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

