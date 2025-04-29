
import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine 
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId


class Operations: 

    def get_si_db_connection(self):        

        si_db_connection = 'mysql+pymysql://sourceinfi_analytics:zK016&fF&W(c@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com/voehoo'
        si_db = create_engine(si_db_connection) 
        return si_db     


    ############################# operations report ##########################

    def courierwise_perf(self, Start_Date, End_Date): 

        si_db=self.get_si_db_connection()  
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT c.name as Courier, s.ship_status as Ship_Status, count(s.awb_number) as AWB FROM shippings AS s LEFT JOIN users AS uv ON uv.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE s.intransit_time >= '+str(fromDate)+' AND s.intransit_time < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Courier,Ship_Status ;'

        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 
                                
        return df 
    




    def ndr_reason(self, Start_Date, End_Date): 

        si_db=self.get_si_db_connection()  
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT s.ship_status as Ship_Status, n.latest_remarks as NDR_Remarks, n.total_attempts as Total_Attempts, s.awb_number as AWB FROM shippings AS s LEFT JOIN ndr AS n ON s.id= n.shipment_id WHERE s.intransit_time >= '+str(fromDate)+' AND s.intransit_time < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") ; '

        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)                                 

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x)  
        df=df[(df.NDR_Remarks.isna()!=True) & (df.NDR_Remarks!='')]   
        df.Total_Attempts=df.Total_Attempts.fillna(0).astype(int).transform(lambda x: 'Attempt 3+' if x>3 else ('Attempt '+str(x)))

        return df  
        

    # def NDR_Remarks(self): 
    #     si_db=self.get_si_db_connection()  

    #     sql='SELECT distinct(latest_remarks) as NDR_Remarks FROM ndr;'         
    #     df=pd.read_sql(sql, con=si_db) 
    #     return df  
        

    # def Ship_Status(self): 
    #     si_db=self.get_si_db_connection()  

    #     sql='SELECT distinct(ship_status) as Ship_Status FROM ndr;'         
    #     df=pd.read_sql(sql, con=si_db) 
    #     return df  






    def cancelled_reason(self, Start_Date, End_Date): 

        si_db=self.get_si_db_connection()  
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, s.seller_id as Seller_ID, us.company_name as Seller_Name, sot.order_id as Order_ID, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d") as Order_Date, json_extract(s.booking_log, "$.message") as Reason FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id WHERE s.intransit_time >= '+str(fromDate)+' AND s.intransit_time < '+str(toDate)+' AND s.ship_status IN ("cancelled") AND sot.status IN ("failed") ;'

        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a)  
                                
        return df  





