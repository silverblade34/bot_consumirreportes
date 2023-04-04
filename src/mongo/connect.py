from multiprocessing import connection
from pymongo import MongoClient   
from bson.binary import UuidRepresentation 
class ConnectionMongo:

    def __init__(self):
        #_ NAME DB
        db = "dbprecisogps"
        connection = MongoClient('mongodb://root:sys4log44sa@137.184.146.205:27017/',uuidRepresentation='standard')
        self.con = connection[db]

