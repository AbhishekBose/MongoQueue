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
        self.eq_id = self.coll.insert(query,check_keys=False)
        print('Enqueud for object ID:::',self.eq_id)
        self.msg = "Records is inserted !!"
        return self.msg


    def Dequeue(self):
        print(self.coll_name)
        fetch_query = {'process_state':'Not Processed'}
        self.results = self.coll.find_one(fetch_query)
        return self.results

    def getAllProcessing(self):
        print(self.coll_name)
        fetch_query = {'process_state':'Processing'}
        self.results = self.coll.find(fetch_query)
        return self.results
    
    def getAllProcessed(self):
        print(self.coll_name)
        fetch_query = {'process_state':'Processed'}
        self.results = self.coll.find(fetch_query)
        return self.results
    
    def setAsProcessing(self,objectId):
        print('ObjectId to be set as Processing::',objectId)
        # self.results = self.coll.find_one_and_update(query={"_id":objectId},update={"$set": {"process_state": "Processing"}})
        self.results = self.coll.find_one_and_update({"_id":objectId},{"$set": {"process_state": "Processing"}})
        # self.results = self.coll.update_one({"_id": objectId}, {"$set": {"process_state": "Processing"}})
        print(self.results)
        return self.results  


    def setAsProcessed(self,objectId):
        print('ObjectId to be set as Processed::',objectId)
        self.results = self.coll.find_one_and_update({"_id":objectId},{"$set": {"process_state": "Processed"}})
        return self.results        



def insertData(coll_name, query):
    mq = mongoQueue(coll_name)
    msg = mq.Enqueue(coll_name, query)
    return msg