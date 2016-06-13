import pymongo
import time
import random
import string
from random import randint
import json
import csv

key_tenant_id = 'tenant_id'
key_user_id = 'user_id'
key_item_id = 'item_id'
key_recomm_list = 'recomm_list'

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def generate_big_query_data(n_tenants,n_rows,n_recomm,target_user_index,target_item_index):

    t_target_user = ''
    t_target_item = ''

    data = []
    data_csv = []

    for i in range(n_rows):
        t_id = 'tenant' + str(random.randrange(n_tenants))
        recomm = []
        for j in range(n_recomm):
            recomm.append(generate_random_string(10))

        random_user_or_item = generate_random_string(10)
        if i%2 == 0:
            key_user_or_item = key_user_id
            if i == target_user_index:
                t_target_user = t_id
                target_user = random_user_or_item
        else:
            key_user_or_item = key_item_id
            if i == target_item_index:
                t_target_item = t_id
                target_item = random_user_or_item

        data.append({key_tenant_id:t_id, key_user_or_item:random_user_or_item, key_recomm_list: recomm})
        data_csv.append([t_id, random_user_or_item, recomm])
        if i % 10000 == 0:
            print str(i) + ' rows generated. -> ' + str(int(i*100/n_rows)) + '%'

    return data,data_csv,(t_target_user,target_user),(t_target_item,target_item)

def write_to_json(data):
    with open('recomm_result_test_data.json', 'w') as outfile:
        for row in data:
            line = json.dumps(row)
            outfile.write(line+'\n')

def write_to_csv(data_csv):
    with open('recomm_result_test_data.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(data_csv)

def insert_big_query_data(data,collection):
    collection.drop()
    time0 = time.time()
    collection.insert_many(data)
    print '{} recommendations have been be inserted. -> {} seconds'.format(len(data),time.time()-time0)

def query(collection,t_target_user,target_user,t_target_item,target_item):

    time0 = time.time()
    collection.find_one({key_tenant_id:t_target_user,key_user_id:target_user,key_item_id:{'$exists':False}})
    print 'given user_id: found within {} seconds'.format(time.time()-time0)

    time0 = time.time()
    collection.find_one({key_tenant_id:t_target_item,key_user_id:{'$exists':False},key_item_id:target_item})
    print 'given item_id: found within {} seconds'.format(time.time()-time0)


if __name__ == '__main__':

    num_recomm = 10
    num_tenant = 10
    num_product_or_user = 5
    target_user_index = 2
    target_item_index = target_user_index+1

    db_out = 'test_database'
    dbtable_out = 'recomm'

    mongo_client = pymongo.MongoClient()
    db = mongo_client[db_out]
    collection = db[dbtable_out]

    print 'Generating data...'
    data,data_csv,user_tuple,item_tuple = generate_big_query_data(num_tenant, num_product_or_user, num_recomm, target_user_index, target_item_index)
    # print data
    # time.sleep(1000)
    write_to_json(data)
    write_to_csv(data_csv)
    print 'Inserting data...'
    #insert_big_query_data(data,collection)

    print 'Query - no compound index:'
    query(collection,user_tuple[0],user_tuple[1],item_tuple[0],item_tuple[1])
    print 'Query - compound index:'
    collection.create_index([(key_tenant_id, pymongo.ASCENDING),(key_user_id, pymongo.ASCENDING),(key_item_id, pymongo.ASCENDING)])
    query(collection,user_tuple[0],user_tuple[1],item_tuple[0],item_tuple[1])
    collection.drop_indexes()
    mongo_client.close()
