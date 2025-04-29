
import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine 
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId


class Vendor:  

    def get_si_db_connection(self):       

        si_db_connection = 'mysql+pymysql://sourceinfi_analytics:zK016&fF&W(c@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com/voehoo'
        si_db = create_engine(si_db_connection) 
        return si_db     


    ############################# vendor report ##########################

    def pendency_tat(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, s.seller_id as Seller_ID, us.company_name as Seller_Name, s.awb_number as AWB, sot.order_id as Order_ID, sot.variant_sku as Variant_SKU, p.title as Product_Name, s.ship_status as Ship_Status, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d") as Pickup_Time, DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") as Approved_Time FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled"); '

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)

        df.Order_ID= 'ORD-'+df.Order_ID.astype(str) 
        df['Diff_Pickup_vs_Approval']=(pd.to_datetime(df.Pickup_Time)-pd.to_datetime(df.Approved_Time)).dt.days
        df['Diff_Pickup_vs_Order']=(pd.to_datetime(df.Pickup_Time)-pd.to_datetime(df.Order_Date)).dt.days
        
        return df 
    

    def pendency_tat_vendor(self, Vendor_ID, Start_Date, End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, s.seller_id as Seller_ID, us.company_name as Seller_Name, s.awb_number as AWB, sot.order_id as Order_ID, sot.variant_sku as Variant_SKU, p.title as Product_Name, s.ship_status as Ship_Status, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d") as Pickup_Time, DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") as Approved_Time FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id WHERE s.vendor_id= '+str(Vendor_ID)+' AND so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled"); '

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)

        df.Order_ID= 'ORD-'+df.Order_ID.astype(str) 
        df['Diff_Pickup_vs_Approval']=(pd.to_datetime(df.Pickup_Time)-pd.to_datetime(df.Approved_Time)).dt.days
        df['Diff_Pickup_vs_Order']=(pd.to_datetime(df.Pickup_Time)-pd.to_datetime(df.Order_Date)).dt.days
        
        return df 










    def del_percent(self, Start_Date, End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple())) 
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT sot.variant_sku as Variant_SKU, p.title as Product_Name, pp.pricing_model as Pricing_Plan, s.awb_number as AWB, sot.margin as Product_Margin, s.ship_status as Ship_Status FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN pricing_plans as pp on pp.id=us.pricing_plan_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") ; ' 
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 

        df['Pricing_Plan']=df['Pricing_Plan'].str.title()  
        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 
        df.Product_Margin=df.Product_Margin.astype(float)  

        return df 


    def courier_performance_vendor(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection()  
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT c.name as Courier, s.ship_status as Ship_Status, count(s.awb_number) as AWB FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS uv ON uv.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Courier,Ship_Status ;'

        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 
                                
        return df 
    


    def vendor_perf(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399   

        sql='SELECT uv.company_name as Vendor_Name, c.name as Courier_Category, s.ship_status as Ship_Status, count(s.awb_number) as AWB FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Vendor_Name,Courier_Category,Ship_Status ;'
        
        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 

        return df 


    ############# AWB_Search #############

    def AWB_Search(self, AWB):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        sql='SELECT awb_number as AWB, booking_log as Reason FROM shippings where awb_number="' +str(AWB)+ '";' 
        
        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
        
        return df 

    ###################################### 


    def vendor_none(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, DATE_FORMAT(FROM_UNIXTIME(so.order_date),"%%Y-%%m-%%d") as Order_Date, s.awb_number as AWB, s.remittance_id AS Vendor_Remittance_ID, sot.grand_total as Order_Amount, vr.utr_number as UTR_Number FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN vendor_remittance as vr on s.remittance_id=vr.id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered") AND so.payment_method IN ("COD") AND s.remittance_id=0 ;'

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
         
        return df 


    def utr_none(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, DATE_FORMAT(FROM_UNIXTIME(so.order_date),"%%Y-%%m-%%d") as Order_Date, s.awb_number as AWB, s.remittance_id AS Vendor_Remittance_ID, sot.grand_total as Order_Amount, vr.utr_number as UTR_Number FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN vendor_remittance as vr on s.remittance_id=vr.id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered") AND so.payment_method IN ("COD") AND s.remittance_id!=0 AND vr.utr_number="" ;'

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
         
        return df  


    def kam_performance(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  
        
        sql='WITH Seller_Info AS (SELECT us.id as Seller_ID, us.company_name as Seller_Company, concat(us.fname," ",us.lname) as Seller_Name, concat(sm.fname," ",sm.lname) as Account_Manager, DATE_FORMAT(from_unixtime(us.created),"%%Y-%%m-%%d") as Onboard_Date FROM users AS us LEFT JOIN users as sm on us.account_manager_id=sm.id where us.account_manager_id!=0 AND us.user_type="vendor"), Orders AS (SELECT us.id as Seller_ID, count(sot.order_id) as Orders FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND us.id in (SELECT id FROM users where account_manager_id!=0 AND user_type="vendor") AND s.ship_status NOT IN ("cancelled") AND sot.variant_sku like ("SI%%") GROUP BY Seller_ID) Select SI.Seller_ID, SI.Seller_Company, SI.Seller_Name, SI.Account_Manager, SI.Onboard_Date, O.Orders FROM Seller_Info as SI LEFT JOIN Orders as O on SI.Seller_ID=O.Seller_ID;'

        df=pd.read_sql(sql, con=si_db) 
        df=df.fillna(0)   
        df.Onboard_Date=pd.to_datetime(df.Onboard_Date).dt.date 

        print(datetime.now()-a) 

        return df 



