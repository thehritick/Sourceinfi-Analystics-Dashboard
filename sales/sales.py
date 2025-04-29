import os
import time
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
import pandas as pd
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
import numpy as np


class Sales:

    def get_si_db_connection(self):

        si_db_connection = "mysql+pymysql://sourceinfi_analytics:zK016&fF&W(c@sourceinfi-slave.cpsoyeuk8ajm.ap-south-1.rds.amazonaws.com/voehoo"
        si_db = create_engine(si_db_connection)
        return si_db

    ############################# sales report ##########################

    def pendency_tat(self, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            'SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, s.seller_id as Seller_ID, us.company_name as Seller_Name, s.awb_number as AWB, sot.order_id as Order_ID, sot.variant_sku as Variant_SKU, p.title as Product_Name, s.ship_status as Ship_Status, c.name as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d") as Pickup_Time, DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") as Approved_Time FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE so.order_date >= '
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND s.ship_status NOT IN ("cancelled"); '
        )

        df = pd.read_sql(sql, con=si_db)
        df.to_csv("pendency_tat_raw_dat.csv", index=False)
        print(datetime.now() - a)

        df.Order_ID = "ORD-" + df.Order_ID.astype(str)
        df["Diff_Pickup_vs_Approval"] = (
            pd.to_datetime(df.Pickup_Time) - pd.to_datetime(df.Approved_Time)
        ).dt.days
        df["Diff_Pickup_vs_Order"] = (
            pd.to_datetime(df.Pickup_Time) - pd.to_datetime(df.Order_Date)
        ).dt.days

        return df

    def pendency_tat_seller(self, Seller_ID, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            'SELECT s.vendor_id as Vendor_ID, uv.company_name as Vendor_Name, s.seller_id as Seller_ID, us.company_name as Seller_Name, s.awb_number as AWB, sot.order_id as Order_ID, sot.variant_sku as Variant_SKU, p.title as Product_Name, s.ship_status as Ship_Status, c.name as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d") as Pickup_Time, DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") as Approved_Time FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id WHERE s.seller_id= '
            + str(Seller_ID)
            + " AND so.order_date >= "
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND s.ship_status NOT IN ("cancelled"); '
        )

        df = pd.read_sql(sql, con=si_db)
        print(datetime.now() - a)

        df.Order_ID = "ORD-" + df.Order_ID.astype(str)
        df["Diff_Pickup_vs_Approval"] = (
            pd.to_datetime(df.Pickup_Time) - pd.to_datetime(df.Approved_Time)
        ).dt.days
        df["Diff_Pickup_vs_Order"] = (
            pd.to_datetime(df.Pickup_Time) - pd.to_datetime(df.Order_Date)
        ).dt.days

        return df

    def seller_perf(self, Start_Date, End_Date):
        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = """
        SELECT us.company_name AS Seller_Name, 
       c.name AS Courier_Category, 
       s.ship_status AS Ship_Status, 
       COUNT(*) AS AWB 
FROM shippings AS s 
JOIN shopify_order_items AS sot ON s.order_item_id = sot.id 
JOIN shopify_orders AS so ON sot.order_id = so.id 
JOIN users AS us ON us.id = s.seller_id 
JOIN courier AS c ON c.id = s.courier_id 
WHERE so.order_date >= %s AND so.order_date < %s 
  AND s.ship_status NOT IN ("cancelled") 
GROUP BY us.company_name, c.name, s.ship_status;

        """

        df = pd.read_sql(sql, con=si_db, params=(fromDate, toDate))
        print(datetime.now() - a)

        df["Ship_Status"] = df["Ship_Status"].str.title()
        df["Ship_Status"] = df.Ship_Status.transform(
            lambda x: "RTO" if x.startswith("Rto") else x
        )

        return df

    def avg_margin_prod(self, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            "SELECT sot.variant_sku as Variant_SKU, p.title as Product_Name, pp.pricing_model as Pricing_Plan, s.awb_number as AWB, sot.margin as Product_Margin, s.ship_status as Ship_Status FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN pricing_plans as pp on pp.id=us.pricing_plan_id WHERE so.order_date >= "
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND s.ship_status NOT IN ("cancelled") ; '
        )

        df = pd.read_sql(sql, con=si_db)
        print(datetime.now() - a)

        df["Pricing_Plan"] = df["Pricing_Plan"].str.title()
        df["Ship_Status"] = df["Ship_Status"].str.title()
        df["Ship_Status"] = df.Ship_Status.transform(
            lambda x: "RTO" if x.startswith("Rto") else x
        )
        df.Product_Margin = df.Product_Margin.astype(float)

        return df

    def referral(self, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        # Parse the Start_Date and End_Date
        start_date_obj = datetime.strptime(Start_Date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(End_Date, "%Y-%m-%d")

        # Convert dates to timestamps (start of the day for start_date and end of the day for end_date)
        fromDate = int(start_date_obj.timestamp())  # Start of the day
        toDate = int(end_date_obj.timestamp()) + 86399  # End of the day

        sql = (
            "WITH Seller_Info as (SELECT us.id as Parent_ID, us.company_name as Parent_Company, ref.id As Referral_User_ID, ref.company_name as Referral_Company FROM users AS us LEFT JOIN users as ref on us.id=ref.referral_id WHERE ref.id!=0 AND ref.created BETWEEN "
            + str(fromDate)
            + " AND "
            + str(toDate)
            + " GROUP BY Parent_ID, Referral_User_ID), Volume as (SELECT us.id as Referral_User_ID, us.company_name as Referral_Company, count(sot.order_id) as Total_Volume FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= "
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND us.id in (SELECT id FROM users WHERE referral_id!=0) AND s.ship_status NOT IN ("cancelled") AND sot.variant_sku like ("SI%%") GROUP BY Referral_User_ID), Delivered_Volume as (SELECT us.id as Referral_User_ID, us.company_name as Referral_Company, count(sot.order_id) as Delivered_Volume FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= '
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND us.id in (SELECT id FROM users WHERE referral_id!=0) AND s.ship_status IN ("delivered") AND sot.variant_sku like ("SI%%") GROUP BY Referral_User_ID) Select SI.Parent_ID, SI.Parent_Company, SI.Referral_User_ID, SI.Referral_Company, V.Total_Volume, DV.Delivered_Volume from Seller_Info as SI left join Volume as V on SI.Referral_User_ID=V.Referral_User_ID left join Delivered_Volume as DV on SI.Referral_User_ID=DV.Referral_User_ID;'.format(
                fromDate, toDate, fromDate, toDate, fromDate, toDate
            )
        )

        df = pd.read_sql(sql, con=si_db)
        df = df.fillna(0)
        print(datetime.now() - a)

        return df

    def kam_performance(self, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            'WITH Seller_Info AS (SELECT us.id as Seller_ID, us.company_name as Seller_Company, concat(us.fname," ",us.lname) as Seller_Name, concat(sm.fname," ",sm.lname) as Account_Manager, DATE_FORMAT(from_unixtime(us.created),"%%Y-%%m-%%d") as Onboard_Date FROM users AS us LEFT JOIN users as sm on us.account_manager_id=sm.id where us.account_manager_id!=0 AND us.user_type="seller"), Orders AS (SELECT us.id as Seller_ID, count(sot.order_id) as Orders FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN users AS us ON us.id= s.seller_id WHERE so.order_date >= '
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND us.id in (SELECT id FROM users where account_manager_id!=0 AND user_type="seller") AND s.ship_status NOT IN ("cancelled") AND sot.variant_sku like ("SI%%") GROUP BY Seller_ID) Select SI.Seller_ID, SI.Seller_Company, SI.Seller_Name, SI.Account_Manager, SI.Onboard_Date, O.Orders FROM Seller_Info as SI LEFT JOIN Orders as O on SI.Seller_ID=O.Seller_ID;'
        )

        df = pd.read_sql(sql, con=si_db)
        df = df.fillna(0)
        df.Onboard_Date = pd.to_datetime(df.Onboard_Date).dt.date

        print(datetime.now() - a)

        return df

    def fad_report(self, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            'SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.ship_status as Ship_Status, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, CASE WHEN service_provider="nimbuslm" THEN concat(name," LM") ELSE concat(name," NP") END as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, DATE_FORMAT(from_unixtime(s.rto_intransit_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Intransit_Time, DATE_FORMAT(from_unixtime(s.rto_delivered_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.product_name as Product_Name, n.total_attempts as NDR_Attempts, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN ndr AS n ON s.id= n.shipment_id WHERE so.order_date >= '
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND s.ship_status IN ("delivered","exception","rto delivered","rto in transit","out for delivery") ;'
        )

        df = pd.read_sql(sql, con=si_db)
        print(datetime.now() - a)

        df.NDR_Attempts = df.NDR_Attempts.fillna("FAD")
        df = df[~((df.Ship_Status == "out for delivery") & (df.NDR_Attempts == "FAD"))]
        df["Ship_Status"] = df.Ship_Status.transform(
            lambda x: "Delivered" if x == "delivered" else "RTO"
        )

        return df

    def fad_report_seller(self, Seller_ID, Start_Date, End_Date):

        si_db = self.get_si_db_connection()
        a = datetime.now()

        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        sql = (
            'SELECT s.seller_id as Seller_ID, concat(us.fname," ",us.lname) as Seller_Name, us.company_name as Seller_Company, s.vendor_id as Vendor_ID, concat(uv.fname," ",uv.lname) as Vendor_Name, uv.company_name as Vendor_Company, s.ship_status as Ship_Status, s.awb_number as AWB, s.id as Shipment_ID, concat("ORD-",sot.order_id) as Order_ID, so.order_number as Order_Number, CASE WHEN service_provider="nimbuslm" THEN concat(name," LM") ELSE concat(name," NP") END as Courier_Category, DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") as Order_Date, DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m-%%d") as Shipment_Date, DATE_FORMAT(from_unixtime(s.pickup_time),"%%Y-%%m-%%d %%H:%%m") as Pickup_Time, DATE_FORMAT(from_unixtime(s.intransit_time),"%%Y-%%m-%%d %%H:%%m") as Intransit_Time, DATE_FORMAT(from_unixtime(s.delivered_time),"%%Y-%%m-%%d %%H:%%m") as Delivered_Time, DATE_FORMAT(from_unixtime(s.rto_intransit_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Intransit_Time, DATE_FORMAT(from_unixtime(s.rto_delivered_time),"%%Y-%%m-%%d %%H:%%m") as RTO_Delivered_Time, sot.variant_sku as Variant_SKU, p.parent_sku as Product_SKU, sot.product_name as Product_Name, n.total_attempts as NDR_Attempts, sot.sub_total as Order_Amount, so.payment_method as Payment_Mode FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id LEFT JOIN users AS uv ON uv.id= s.vendor_id LEFT JOIN users AS us ON us.id= s.seller_id LEFT JOIN courier AS c ON c.id= s.courier_id LEFT JOIN ndr AS n ON s.id= n.shipment_id WHERE s.seller_id='
            + str(Seller_ID)
            + " AND so.order_date >= "
            + str(fromDate)
            + " AND so.order_date < "
            + str(toDate)
            + ' AND s.ship_status IN ("delivered","exception","rto delivered","rto in transit","out for delivery") ;'
        )

        df = pd.read_sql(sql, con=si_db)
        print(datetime.now() - a)

        df.NDR_Attempts = df.NDR_Attempts.fillna("FAD")
        df = df[~((df.Ship_Status == "out for delivery") & (df.NDR_Attempts == "FAD"))]
        df["Ship_Status"] = df.Ship_Status.transform(
            lambda x: "Delivered" if x == "delivered" else "RTO"
        )

        return df

    def jit_tat(self, Start_Date, End_Date):
        si_db = self.get_si_db_connection()
        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        # SQL query to return raw data for TAT calculations
        sql = f"""
        SELECT 
    so.id AS Order_ID,                                -- Shopify Order ID
    so.order_number AS Order_number,                  -- Shopify Order Number
    sot.id AS Order_Item_ID,
    so.user_id AS User_id,                            -- Shopify User ID
    DATE_FORMAT(from_unixtime(so.created),"%%Y-%%m-%%d") AS Order_Created_Date,                 -- SourceInfi Order Creation Date (UNIX timestamp)
    so.status AS Order_Status,                        -- Shopify Order Status
    DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") AS Order_date,                      -- Shopify Order Date
    DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") AS Order_Approved_Date,
    pom.status AS Purchase_Order_Status,               -- Purchase Order Status
    DATE_FORMAT(from_unixtime(pom.created),"%%d-%%m-%%Y") AS Purchase_Order_Created_Date,        -- Purchase Order Creation Date (UNIX timestamp)
    pom.po_id AS Purchase_Order_ID,                   -- Purchase Order ID
    DATE_FORMAT(from_unixtime(sh.created),"%%Y-%%m-%%d") AS Shipment_Date,
    sh.awb_number AS AWB,
    sh.ship_status AS Tracking_Status
FROM 
    shopify_orders AS so
LEFT JOIN 
    purchase_order_mapping AS pom ON so.id = pom.order_id
LEFT JOIN 
	shopify_order_items AS sot ON so.id = sot.order_id
LEFT JOIN 
    shippings AS sh ON sh.order_item_id = sot.id

WHERE 
    so.jit_order = 1 
    AND so.created >= {fromDate}               -- Filter by date range
    AND so.created < {toDate}
    AND so.status NOT IN ('cancelled','new')
    
ORDER BY 
    so.created ASC;    
    """

        # Run the SQL query and load the result into a DataFrame
        df = pd.read_sql(sql, con=si_db)
        df["App_to_PO_TAT"] = pd.to_datetime(
            df["Purchase_Order_Created_Date"]
        ) - pd.to_datetime(df["Order_Approved_Date"])
        df["PO_to_Ship_TAT"] = pd.to_datetime(df["Shipment_Date"]) - pd.to_datetime(
            df["Purchase_Order_Created_Date"]
        )
        df["App_to_ship_TAT"] = pd.to_datetime(df["Shipment_Date"]) - pd.to_datetime(
            df["Order_Approved_Date"]
        )

        df.to_csv("sales/sales_reports/jit_tat_raw_data.csv", index=False)

        print(sql)
        print(fromDate, " :  ", toDate)
        print(Start_Date, " :  ", End_Date)
        print(df.info())

        return df

    def user_remarks(self, Start_Date, End_Date):
        si_db = self.get_si_db_connection()
        fromDate = int(
            time.mktime((datetime.strptime(Start_Date, "%Y-%m-%d")).timetuple())
        )
        toDate = (
            int(time.mktime((datetime.strptime(End_Date, "%Y-%m-%d")).timetuple()))
            + 86399
        )

        # SQL query to return raw data for TAT calculations
        sql = f"""
        SELECT 
    so.id AS Order_ID,                                -- Shopify Order ID
    so.order_number AS Order_number,                  -- Shopify Order Number
    sot.id AS Order_Item_ID,
    so.user_id AS User_id,                            -- Shopify User ID
    DATE_FORMAT(from_unixtime(so.created),"%%Y-%%m-%%d") AS Order_Created_Date,                 -- SourceInfi Order Creation Date (UNIX timestamp)
    so.status AS Order_Status,                        -- Shopify Order Status
    DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m-%%d") AS Order_date,                      -- Shopify Order Date
    DATE_FORMAT(from_unixtime(sot.approved_time),"%%Y-%%m-%%d") AS Order_Approved_Date,
    pom.status AS Purchase_Order_Status,               -- Purchase Order Status
    DATE_FORMAT(from_unixtime(pom.created),"%%d-%%m-%%Y") AS Purchase_Order_Created_Date,        -- Purchase Order Creation Date (UNIX timestamp)
    pom.po_id AS Purchase_Order_ID,                   -- Purchase Order ID
    DATE_FORMAT(from_unixtime(sh.created),"%%Y-%%m-%%d") AS Shipment_Date,
    sh.awb_number AS AWB,
    sh.ship_status AS Tracking_Status
FROM 
    shopify_orders AS so
LEFT JOIN 
    purchase_order_mapping AS pom ON so.id = pom.order_id
LEFT JOIN 
	shopify_order_items AS sot ON so.id = sot.order_id
LEFT JOIN 
    shippings AS sh ON sh.order_item_id = sot.id

WHERE 
    so.jit_order = 1 
    AND so.created >= {fromDate}               -- Filter by date range
    AND so.created < {toDate}
    AND so.status NOT IN ('cancelled','new')
    
ORDER BY 
    so.created ASC;    
    """

        # Run the SQL query and load the result into a DataFrame
        df = pd.read_sql(sql, con=si_db)
        df["App_to_PO_TAT"] = pd.to_datetime(
            df["Purchase_Order_Created_Date"]
        ) - pd.to_datetime(df["Order_Approved_Date"])
        df["PO_to_Ship_TAT"] = pd.to_datetime(df["Shipment_Date"]) - pd.to_datetime(
            df["Purchase_Order_Created_Date"]
        )
        df["App_to_ship_TAT"] = pd.to_datetime(df["Shipment_Date"]) - pd.to_datetime(
            df["Order_Approved_Date"]
        )

        df.to_csv("sales/sales_reports/jit_tat_raw_data.csv", index=False)

        print(sql)
        print(fromDate, " :  ", toDate)
        print(Start_Date, " :  ", End_Date)
        print(df.info())

        return df

    # def growth_report(self, PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     PM_fromDate=int(time.mktime((datetime.strptime(PM_Start_Date,"%Y-%m-%d")).timetuple()))
    #     PM_toDate=int(time.mktime((datetime.strptime(PM_End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     CM_fromDate=int(time.mktime((datetime.strptime(CM_Start_Date,"%Y-%m-%d")).timetuple()))
    #     CM_toDate=int(time.mktime((datetime.strptime(CM_End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT id as Seller_ID, concat(fname," ",lname) as Seller_Name, company_name as Company_Name FROM users; '
    #     users=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)

    #     users.Seller_Name=users.Seller_Name.str.title()
    #     users.Company_Name=users.Company_Name.str.title()

    #     sql='SELECT s.seller_id as Seller_ID, count(s.awb_number) as PM_Ships_Till_Date FROM shippings AS s WHERE s.created >= '+str(PM_fromDate)+' AND s.created < '+str(PM_toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_ID;'
    #     PM_ships=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     PM_ships

    #     sql='SELECT s.seller_id as Seller_ID, count(s.awb_number) as CM_Ships_Till_Date FROM shippings AS s WHERE s.created >= '+str(CM_fromDate)+' AND s.created < '+str(CM_toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_ID;'
    #     CM_ships=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     CM_ships

    #     final=users.merge(PM_ships, on='Seller_ID', how='left').merge(CM_ships, on='Seller_ID', how='left').fillna(0)
    #     final=final.sort_values(by='CM_Ships_Till_Date', ascending=False).drop(columns='Seller_ID')
    #     final[['PM_Ships_Till_Date','CM_Ships_Till_Date']]=final[['PM_Ships_Till_Date','CM_Ships_Till_Date']].astype(int)

    #     final['GOLM']=((final.CM_Ships_Till_Date/final.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'
    #     final=final[~((final.PM_Ships_Till_Date==0)&(final.CM_Ships_Till_Date==0))]

    #     return final

    # ################################# dashboard #################################

    # # top_sellers

    # def top_sellers(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT us.company_name as Seller_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN users AS us ON us.id= s.seller_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Seller_Name ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     df=df.head(10)

    #     return df

    # # top_vendors

    # def top_vendors(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT uv.company_name as Vendor_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN users AS uv ON uv.id= s.vendor_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Vendor_Name ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     df=df.head(10)

    #     return df

    # # top_couriers

    # def top_couriers(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT c.name as Courier_Category, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN courier AS c ON c.id= s.courier_id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Courier_Category ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     df=df.head(10)

    #     return df

    # # top_cities

    # def top_cities(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT so.shipping_city as Shipping_City, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Shipping_City ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     df=df.head(10)

    #     return df

    # # order_vs_ships

    # def order_vs_ships(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m") as Month, count(so.id) as Orders FROM shopify_orders AS so WHERE so.order_date >= '+str(fromDate)+' AND so.order_date < '+str(toDate)+' AND so.status NOT IN ("cancelled") GROUP BY Month ORDER BY Month asc; '

    #     # all
    #     # sql='SELECT DATE_FORMAT(from_unixtime(so.order_date),"%%Y-%%m") as Month, count(so.id) as Orders FROM shopify_orders AS so WHERE so.status NOT IN ("cancelled") GROUP BY Month ORDER BY Month asc; '

    #     orders=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)

    #     sql='SELECT DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m") as Month, count(s.awb_number) as Ships FROM shippings AS s WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Month ORDER BY Month asc; '

    #     # all
    #     # sql='SELECT DATE_FORMAT(from_unixtime(s.created),"%%Y-%%m") as Month, count(s.awb_number) as Ships FROM shippings AS s WHERE s.ship_status NOT IN ("cancelled") GROUP BY Month ORDER BY Month asc; '

    #     ships=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)

    #     df=orders.merge(ships, on='Month', how='right')

    #     return df

    # # top_products

    # def top_products(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT concat(p.title, " - ", sot.variant_sku) as Product_Name, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id LEFT JOIN product_variants AS pv ON pv.si_sku = sot.variant_sku LEFT JOIN products AS p ON p.id = pv.product_id  WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Product_Name ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)
    #     df=df.pivot_table(index=['Product_Name'], values='Ships', aggfunc='sum').reset_index().sort_values(by='Ships', ascending=False).head(10)

    #     return df

    # # cod_pre

    # def cod_pre(self, Start_Date, End_Date):

    #     si_db=self.get_si_db_connection()
    #     a=datetime.now()

    #     fromDate=int(time.mktime((datetime.strptime(Start_Date,"%Y-%m-%d")).timetuple()))
    #     toDate=int(time.mktime((datetime.strptime(End_Date,"%Y-%m-%d")).timetuple()))+86399

    #     sql='SELECT so.payment_method as Payment_Mode, count(s.awb_number) as Ships FROM shippings AS s LEFT JOIN shopify_order_items AS sot ON s.order_item_id = sot.id LEFT JOIN shopify_orders AS so ON sot.order_id = so.id WHERE s.created >= '+str(fromDate)+' AND s.created < '+str(toDate)+' AND s.ship_status NOT IN ("cancelled") GROUP BY Payment_Mode ORDER BY Ships desc ;'

    #     df=pd.read_sql(sql, con=si_db)
    #     print(datetime.now()-a)

    #     return df
