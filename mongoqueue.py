#%%
import pymongo
from pymongo import MongoClient
import sys
# from settings import MONGO_DB_URL,MONGO_DB_NAME, DB

MONGO_DB_NAME = 'MongoQueue'
MONGO_DB_URL = 'localhost'
CLIENT = MongoClient(MONGO_DB_URL, 27017)
DB = CLIENT[MONGO_DB_NAME]


#%%
class mongoQueue:
    def __init__(self,coll_name):
        self.coll = DB[coll_name]
        self.coll_name = coll_name

    def Enqueue(self,coll,query):
        self.eq_id = coll.insert(query,check_keys=False)
        print('Enqueud for object ID:::',self.eq_id)

    def Dequeue(self):
        print(self.coll_name)
        fetch_query = {'process_state':'Not Processed'}
        self.results = self.coll.find(fetch_query)
        return self.results


#%%
        

# class fetchingQuery:
#     def process_sets(self, db, coll, query):
#         self.results = coll.find(query)
#         return self.results
#     def fetch_serverNames(self, db, coll):
#         self.resp = coll.find()
#         return self.resp
#     def filter_query(self, db, coll, q1, q2):
#         self.results = coll.find(q1, q2)
#         return self.results


# class DeleteData:
#     def remove_data(self, db, coll, query):
#         self.results = coll.remove(query)
#         return self.results

# def insert_data(query, collection):
#     c_db = settingupDb(query, collection)
#     coll = c_db.construct_Db()
#     c_db.push_into_db(DB, coll, query)

# def fetchData(collection, query):
#     c_db = settingupDb(query, collection)
#     coll = c_db.construct_Db()
#     f_db = fetchingQuery()
#     res = f_db.process_sets(DB, coll, query)
#     return res

# def filteredData(collection, q1, q2):
#     c_db = settingupDb(q1, collection)
#     coll = c_db.construct_Db()
#     f_db = fetchingQuery()
#     res = f_db.filter_query(DB, coll, q1, q2)
#     return res

# def update_info(query, newVal, collection):
#     c_db = settingupDb(query, collection)
#     coll = c_db.construct_Db()
#     c_db.updates_info(DB, coll, query, newVal)

#%%