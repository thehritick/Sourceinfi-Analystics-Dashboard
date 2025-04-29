
import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine 
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId


class Finance: 

    def get_si_db_connection(self):       

        si_db_connection = 'mysql+pymysql://sourceinfi_analytics:zK016&fF&W(c@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com/voehoo'
        si_db = create_engine(si_db_connection) 
        return si_db     


    ############################# finance report ########################## 

    def finance_report(self, Start_Date, End_Date): 

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT count(s.awb_number) as AWB, sum(sot.selling_price) as Selling_Price, sum(sot.b2b_price * sot.quantity) as Applied_B2B_Price, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Shipment_Date; '

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
         
        return df 



    def seller_none(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.seller_id as Seller_ID, us.company_name as Seller_Name, DATE_FORMAT(FROM_UNIXTIME(so.order_date),"%%Y-%%m-%%d") as Order_Date, s.awb_number as AWB, s.seller_remittance_id AS Seller_Remittance_ID, sot.grand_total as Order_Amount, sr.utr_number as UTR_Number FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN seller_remittance as sr on s.seller_remittance_id=sr.id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered") AND so.payment_method IN ("COD") AND s.seller_remittance_id=0 ; '

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
         
        return df 


    def utr_none(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.seller_id as Seller_ID, us.company_name as Seller_Name, DATE_FORMAT(FROM_UNIXTIME(so.order_date),"%%Y-%%m-%%d") as Order_Date, s.awb_number as AWB, s.seller_remittance_id AS Seller_Remittance_ID, sot.grand_total as Order_Amount, sr.utr_number as UTR_Number FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN seller_remittance as sr on s.seller_remittance_id=sr.id WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND s.ship_status IN ("delivered") AND so.payment_method IN ("COD") AND s.seller_remittance_id!=0 AND sr.utr_number="" ; '

        df=pd.read_sql(sql, con=si_db)  
        print(datetime.now()-a)
         
        return df  




    def finance_sales_summary(self, Start_Date, End_Date):  

        si_db=self.get_si_db_connection() 
        a=datetime.now()
        
        fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
        toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399  

        sql='SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, s.ship_status as Ship_Status, c.name as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.hsn_code as HSN_Code, p.title as Product_Name, sot.quantity as Quantity, sot.b2b_price as Product_B2B_Price, sot.product_gst as GST_Rate, sot.gst_price as GST_Price, sot.margin as Product_Margin, sot.selling_price as Product_Selling_Price, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode, concat(pp.pricing_model," ",pp.plan_name) as Pricing_Plan, (CASE WHEN sr.paid="1" THEN "Yes" ELSE "No" END) as Seller_Remittance, (CASE WHEN vr.paid="1" THEN "Yes" ELSE "No" END) as Vendor_Remittance, (CASE WHEN s.seller_invoice="1" THEN "Yes" ELSE "No" END) as Seller_Invoice, (CASE WHEN s.vendor_invoice="1" THEN "Yes" ELSE "No" END) as Vendor_Invoice FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN pricing_plans AS pp ON us.pricing_plan_id= pp.id LEFT JOIN seller_remittance AS sr ON s.seller_remittance_id= sr.id LEFT JOIN vendor_remittance AS vr ON s.remittance_id= vr.id WHERE s.delivered_time >= '+str(fromDate)+' AND s.delivered_time < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled"); '

        df=pd.read_sql(sql, con=si_db)  

        df['Applied_B2B_Price']=(df.Quantity.astype(float)*df.Product_B2B_Price.astype(float)).round(2)
        df['SI_Cost_Price']=(df.Applied_B2B_Price.astype(float)+df.GST_Price.astype(float)).round(2)
        df['Applied_Product_Margin']=(df.Quantity.astype(float)*df.Product_Margin.astype(float)).round(2)
        df['Applied_Selling_Price']=(df.SI_Cost_Price.astype(float)+df.Applied_Product_Margin.astype(float)).round(2) 

        Seller_ID_List=df.Seller_ID.unique().tolist()
        Vendor_ID_List=df.Vendor_ID.unique().tolist() 

        sql='SELECT id as ID, seller_id as Seller_ID, shipment_ids as Shipment_ID FROM seller_cumulative_invoice WHERE seller_id in' +str(tuple(Seller_ID_List))+ ';'

        seller_in=pd.read_sql(sql, con=si_db) 

        seller_in['Shipment_ID']=seller_in.Shipment_ID.str.split(',')
        seller_in=seller_in.explode('Shipment_ID')
        seller_in['ID']=seller_in['ID'].apply(lambda x: '{:04d}'.format(x))
        seller_in['Seller_Invoice_Number']=seller_in.Seller_ID.astype(str)+'/'+str((date.today().year)%100)+'/S'+seller_in.ID.astype(str)
        seller_in

        sql='SELECT id as ID, vendor_id as Vendor_ID, shipment_ids as Shipment_ID FROM vendor_cumulative_invoice WHERE vendor_id in' +str(tuple(Vendor_ID_List))+ ';'

        vendor_in=pd.read_sql(sql, con=si_db) 

        vendor_in['Shipment_ID']=vendor_in.Shipment_ID.str.split(',')
        vendor_in=vendor_in.explode('Shipment_ID')
        vendor_in['ID']=vendor_in['ID'].apply(lambda x: '{:04d}'.format(x))
        vendor_in['Vendor_Invoice_Number']=vendor_in.Vendor_ID.astype(str)+'/'+str((date.today().year)%100)+'/V'+vendor_in.ID.astype(str)
        vendor_in

        invoice=seller_in.merge(vendor_in, on='Shipment_ID', how='outer').fillna(0)
        invoice=invoice[['Shipment_ID','Seller_Invoice_Number','Vendor_Invoice_Number']]
        invoice=invoice[invoice.Shipment_ID!=''] 
        invoice.Shipment_ID=invoice.Shipment_ID.astype(int)
        invoice

        df=df.merge(invoice, on='Shipment_ID', how='left').fillna(0)
        df 

        print(datetime.now()-a) 
         
        return df  
