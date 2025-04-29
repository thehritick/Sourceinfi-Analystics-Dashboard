
import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine 
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np


class Admin:  

    def get_si_db_connection(self):       

        si_db_connection = 'mysql+pymysql://hritick:nvupk@u1k62@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com'
        si_db = create_engine(si_db_connection) 
        return si_db     


    def growth_report(self, PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        PM_fromDate=int(time.mktime((datetime.strptime(PM_Start_Date,"%Y-%m-%d")).timetuple())) 
        PM_toDate=int(time.mktime((datetime.strptime(PM_End_Date,"%Y-%m-%d")).timetuple()))+86399 

        CM_fromDate=int(time.mktime((datetime.strptime(CM_Start_Date,"%Y-%m-%d")).timetuple())) 
        CM_toDate=int(time.mktime((datetime.strptime(CM_End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT id as Seller_ID, concat(fname," ",lname) as Seller_Name, company_name as Company_Name FROM users; ' 
        users=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        users.Seller_Name=users.Seller_Name.str.title()
        users.Company_Name=users.Company_Name.str.title() 

        sql='SELECT s.seller_id as Seller_ID, count(s.awb_number) as PM_Ships_Till_Date FROM shippings AS s WHERE s.created >= '+str(PM_fromDate)+' AND s.created < '+str(PM_toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_ID;'
        PM_ships=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)
        PM_ships

        sql='SELECT s.seller_id as Seller_ID, count(s.awb_number) as CM_Ships_Till_Date FROM shippings AS s WHERE s.created >= '+str(CM_fromDate)+' AND s.created < '+str(CM_toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_ID;'
        CM_ships=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)
        CM_ships

        final=users.merge(PM_ships, on='Seller_ID', how='left').merge(CM_ships, on='Seller_ID', how='left').fillna(0)
        final=final.sort_values(by='CM_Ships_Till_Date', ascending=False).drop(columns='Seller_ID') 
        final[['PM_Ships_Till_Date','CM_Ships_Till_Date']]=final[['PM_Ships_Till_Date','CM_Ships_Till_Date']].astype(int)

        # final['GOLM']=((final.CM_Ships_Till_Date/final.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'        
        final['GOLM']=((final.CM_Ships_Till_Date/final.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int)
        final.GOLM=final.GOLM.where(final.GOLM!=0, final.CM_Ships_Till_Date).astype(str)+'%' 

        final=final[~((final.PM_Ships_Till_Date==0)&(final.CM_Ships_Till_Date==0))] 

        return final   


    ################################# dashboard ################################# 
        
    # top_sellers
    
    def top_sellers(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT us.company_name as Seller_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN users AS us ON us.id= s.seller_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_Name ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 
        df=df.head(10) 

        return df 


    # top_vendors
    
    def top_vendors(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT uv.company_name as Vendor_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN users AS uv ON uv.id= s.vendor_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Vendor_Name ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 
        df=df.head(10) 

        return df 



    # top_couriers
    
    def top_couriers(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT c.name as Courier_Category, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN courier AS c ON c.id= s.courier_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Courier_Category ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 
        df=df.head(10) 

        return df 



    # top_cities
    
    def top_cities(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT so.shipping_city as Shipping_City, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Shipping_City ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 
        df=df.head(10) 

        return df 



    # order_vs_ships
    
    def order_vs_ships(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 
       
        sql='SELECT DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m") as Month, count(sot.order_id) as Orders FROM shopify_orders AS so LEFT JOIN shopify_order_items AS sot ON sot.order_id = so.id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND so.status NOT IN ("cancelled") AND sot.variant_sku like ("SI%%") GROUP BY Month ORDER BY Month asc; '

        orders=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        sql='SELECT DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m") as Month, count(s.awb_number) as Ships FROM shippings AS s WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Month ORDER BY Month asc; '

        ships=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df=orders.merge(ships, on='Month', how='right')

        return df 


    # top_products
    
    def top_products(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT concat(p.title, " - ", sot.variant_sku) as Product_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id  WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Product_Name ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a)  
        df=df.pivot_table(index=['Product_Name'], values='Ships', aggfunc='sum').reset_index().sort_values(by='Ships', ascending=False).head(10) 

        return df 



    # cod_pre
    
    def cod_pre(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT so.payment_method as Payment_Mode, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Payment_Mode ORDER BY Ships desc ;'
        
        df=pd.read_sql(sql, con=si_db)    
        print(datetime.now()-a)  

        return df 

