
import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine 
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np 


class Logistics: 

    def get_si_db_connection(self):        

        si_db_connection = 'mysql+pymysql://sourceinfi_analytics:zK016&fF&W(c@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com/voehoo'
        si_db = create_engine(si_db_connection) 
        return si_db     


    ############################# logistics report ########################## 

    def courierwise_perf(self, Start_Date, End_Date): 

        si_db=self.get_si_db_connection()  
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT (CASE WHEN c.service_provider="nimbuslm" THEN concat(c.name," LM") ELSE concat(c.name," NP") END) as Courier, s.ship_status as Ship_Status, count(s.awb_number) as AWB FROM shippings AS s LEFT JOIN courier AS c ON c.id= s.courier_id WHERE s.intransit_time >= '+str(fromDate)+' AND s.intransit_time < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Courier, Ship_Status ;'

        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 
                                
        return df 
    

    def seller_perf(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT us.company_name as Seller_Name, s.ship_status as Ship_Status, count(s.awb_number) as AWB FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_Name, Ship_Status ;'
        
        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a)

        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 

        return df 


    def sku_product_del(self, Start_Date, End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, p.title as Product_Name, pp.pricing_model as Pricing_Plan, s.awb_number as AWB, sot.margin as Product_Margin, s.ship_status as Ship_Status FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN pricing_plans as pp on pp.id=us.pricing_plan_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") ; ' 
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 

        df['Pricing_Plan']=df['Pricing_Plan'].str.title() 
        df['Ship_Status']=df['Ship_Status'].str.title()
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'RTO' if x.startswith('Rto') else x) 
        df.Product_Margin=df.Product_Margin.astype(float)  

        return df 




    def shipment_export(self, Start_Date, End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT s.seller_id as Seller_ID, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, uv.company_name as Vendor_Company, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, sot.sub_total as Order_Amount, sot.quantity as Quantity, n.total_attempts as NDR_Attempts, n.latest_remarks as NDR_Remarks, st.delivery_attempt_count as Delivery_Attempts, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, p.title as Product_Name, s.ship_status as Ship_Status, (CASE WHEN c.service_provider="nimbuslm" THEN concat(c.name," LM") ELSE concat(c.name," NP") END) as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d %%H:%%m") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d %%H:%%m") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.rto_intransit_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN ndr AS n ON s.id= n.shipment_id LEFT JOIN shipment_tracking AS st ON s.id= st.shipment_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") ;'
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 

        return df 



    def weight_summary(self, Start_Date, End_Date):   

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))  
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399 

        sql='SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, s.ship_status as Ship_Status, CASE WHEN service_provider="nimbuslm" THEN concat(name," LM") ELSE concat(name," NP") END as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.hsn_code as HSN_Code, p.title as Product_Name, sot.product_gst as GST_Rate, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode, sot.product_length as Product_Length, sot.product_breadth as Product_Breadth, sot.product_height as Product_Height, round((sot.product_length * sot.product_breadth * sot.product_height)/5000, 2) as Volumetric_Weight_KG, round(sot.product_weight/1000, 2) as Dead_Weight_KG FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") ;'        
        
        df=pd.read_sql(sql, con=si_db)   
        print(datetime.now()-a) 

        df['Assigned_Weight_KG']=np.where(df.Volumetric_Weight_KG>=df.Dead_Weight_KG, df.Volumetric_Weight_KG, df.Dead_Weight_KG)
        
        return df 



    def fad_report(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  
        
        sql='SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.ship_status as Ship_Status, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, CASE WHEN service_provider="nimbuslm" THEN concat(name," LM") ELSE concat(name," NP") END as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, DATE_FORMAT(from_unixtime(s.rto_intransit_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Intransit_Time, DATE_FORMAT(from_unixtime(s.rto_delivered_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.product_name as Product_Name, n.total_attempts as NDR_Attempts, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN ndr AS n ON s.id= n.shipment_id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered","exception","rto delivered","rto in transit","out for delivery") ;' 
        
        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a) 

        df.NDR_Attempts=df.NDR_Attempts.fillna('FAD') 
        df=df[~((df.Ship_Status=='out for delivery')&(df.NDR_Attempts=='FAD'))]
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'Delivered' if x=='delivered' else 'RTO') 

        return df 



    def fad_report_seller(self, Seller_ID, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  
        
        sql='SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.ship_status as Ship_Status, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, CASE WHEN service_provider="nimbuslm" THEN concat(name," LM") ELSE concat(name," NP") END as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, DATE_FORMAT(from_unixtime(s.rto_intransit_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Intransit_Time, DATE_FORMAT(from_unixtime(s.rto_delivered_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.product_name as Product_Name, n.total_attempts as NDR_Attempts, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN ndr AS n ON s.id= n.shipment_id WHERE s.seller_id='+str(Seller_ID)+' AND so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered","exception","rto delivered","rto in transit","out for delivery") ;' 
        
        df=pd.read_sql(sql, con=si_db) 
        print(datetime.now()-a) 

        df.NDR_Attempts=df.NDR_Attempts.fillna('FAD') 
        df=df[~((df.Ship_Status=='out for delivery')&(df.NDR_Attempts=='FAD'))]
        df['Ship_Status']=df.Ship_Status.transform(lambda x:'Delivered' if x=='delivered' else 'RTO') 

        return df 

