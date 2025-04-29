from passlib.hash import pbkdf2_sha256
from pymongo import MongoClient
from bson.objectid import ObjectId


class User: 
       
    user_details = {
                    "name": '',
                    "email": '',
                    "password": '',
                    "type":'employee', 
                    "permissions":{
                                    'sales':0,
                                    'operations':0,
                                    'finance':0,
                                    'vendor':0,
                                    'logistics':0,
                                    'admin':0 
                                    }      
                    } 
    

    def get_collection(self):
        client = MongoClient('mongodb+srv://ana_db:4V80Kz2v5E7L6tA3@analytic-fbe1a936.mongo.ondigitalocean.com/?authMechanism=DEFAULT')  
        try:
            if client.server_info():
                print('Connection Successfull')
        except Exception as e:
            print("Unable to connect to the server. Reason : ",e) 
        
        return client 
    

    def __init__(self):
        return
    
    
    def find_One(self,email):
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        return users.find_one({'email' :email}) 
    

    def delete_One(self,email):
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        return users.delete_one({'email' :email})     

    
    def check_password(self,user_details,password):
        if pbkdf2_sha256.verify(password, user_details['password']):
            return True
        else:
            return False
    
    def register_user(self):
        self.user_details['password']=pbkdf2_sha256.encrypt(self.user_details['password'])
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        self.user_details['_id'] = ObjectId()
        users.insert_one(self.user_details) 
        
    def update_user(self,details):
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        users.find_one_and_update({'email': details['email']},
                                  {'$set': {
                                            "name":details['name'],
                                            "type":details['type'],
                                            'permissions.sales': details['permissions']['sales'],
                                            'permissions.operations': details['permissions']['operations'],
                                            'permissions.finance': details['permissions']['finance'],
                                            'permissions.vendor': details['permissions']['vendor'], 
                                            'permissions.logistics': details['permissions']['logistics'], 
                                            'permissions.admin': details['permissions']['admin'] 
                                            } 
                                    }) 
       
    def get_all_users(self):
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        cursor=users.find()
        return cursor
    
    def get_user_permissions(self,email):
        client=self.get_collection()
        sourceinfi=client.sourceinfi
        users=sourceinfi.users
        cursor=users.find_one({'email' :email})
        if cursor:
            return cursor['permissions'] 
    

