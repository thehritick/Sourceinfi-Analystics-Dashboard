
from operator import index
from logistics.logistics import Logistics
import pandas as pd
from users import User
from flask import Flask, Blueprint, render_template,request,send_file, redirect, url_for, Response,session
from datetime import date, timedelta, datetime
import time
import numpy as np
import pytz 



logisticsapp = Blueprint('logisticsapp', __name__, template_folder='templates', static_folder='static', url_prefix='/logisticsapp')



@logisticsapp.route('/courierwise_perf', methods = ['POST', 'GET']) 
def courierwise_perf():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Logistics().courierwise_perf(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Courier'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Grand_Total_%']=((vendor_perf['Grand_Total']/vendor_perf['Grand_Total'].sum())*200).astype(int).astype(str)+'%'

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True)
                        vendor_perf=vendor_perf.set_index(['Courier','Grand_Total','Grand_Total_%'] 
                                                                            ).reset_index().sort_values(by='Grand_Total', ascending=False)
                        
                        vendor_perf.to_csv('logistics/logistics_reports/courierwise_perf.csv', index=False)  

                        return render_template('log_courierwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)      


            else: 

                Start_Date = str(date.today()-timedelta(7))     
                End_Date = str(date.today())   

                df=Logistics().courierwise_perf(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Courier'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Grand_Total_%']=((vendor_perf['Grand_Total']/vendor_perf['Grand_Total'].sum())*200).astype(int).astype(str)+'%'

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf 

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True) 
                        vendor_perf=vendor_perf.set_index(['Courier','Grand_Total','Grand_Total_%'] 
                                                                            ).reset_index().sort_values(by='Grand_Total', ascending=False)
                        

                        vendor_perf.to_csv('logistics/logistics_reports/courierwise_perf.csv', index=False)  

                        return render_template('log_courierwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )          
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)      


            
    return redirect('/')


@logisticsapp.route('/courierwise_perf_csv', methods = ['POST', 'GET'])   
def courierwise_perf_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/courierwise_perf.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')




@logisticsapp.route('/', methods = ['POST', 'GET'])  
@logisticsapp.route('/sellerwise_perf', methods = ['POST', 'GET']) 
def seller_perf():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Logistics().seller_perf(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        seller_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Seller_Name'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_status['Delivered_%']=((seller_status['Delivered']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Exception_%']=((seller_status['Exception']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['In_Transit_%']=((seller_status['In Transit']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Out_For_Delivery_%']=((seller_status['Out For Delivery']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['RTO_%']=((seller_status['RTO']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_status=seller_status.set_index(['Seller_Name','Grand_Total']).reset_index().sort_values(by='Grand_Total', ascending=False)

                        seller_status.to_csv('logistics/logistics_reports/sellerwise_perf_csv.csv', index=False)  

                        return render_template('log_sellerwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_status.values, d1col=seller_status.columns.values,   
                                            )      
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_sellerwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_sellerwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)         


            else:   

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Logistics().seller_perf(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        seller_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Seller_Name'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_status['Delivered_%']=((seller_status['Delivered']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Exception_%']=((seller_status['Exception']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['In_Transit_%']=((seller_status['In Transit']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Out_For_Delivery_%']=((seller_status['Out For Delivery']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['RTO_%']=((seller_status['RTO']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_status=seller_status.set_index(['Seller_Name','Grand_Total']).reset_index().sort_values(by='Grand_Total', ascending=False)

                        seller_status.to_csv('logistics/logistics_reports/sellerwise_perf_csv.csv', index=False)  

                        return render_template('log_sellerwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_status.values, d1col=seller_status.columns.values,   
                                            )      

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_sellerwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_sellerwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)            



    return redirect('/') 


@logisticsapp.route('/sellerwise_perf_csv', methods = ['POST', 'GET'])   
def seller_courier_merge_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/sellerwise_perf_csv.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@logisticsapp.route('/sku_product_del', methods = ['POST', 'GET']) 
def sku_product_del():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Logistics().sku_product_del(Start_Date, End_Date)  

                if df.shape[0]>0:
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Variant_SKU','Product_SKU'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='count', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True) 
                        vendor_perf=vendor_perf.set_index(['Variant_SKU','Product_SKU','Grand_Total']).reset_index().sort_values(by='Grand_Total', ascending=False)

                        vendor_perf.to_csv('logistics/logistics_reports/sku_product_del.csv', index=False)  

                        return render_template('sku_product_del.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,   
                                            )           
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('sku_product_del.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('sku_product_del.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Logistics().sku_product_del(Start_Date, End_Date)  

                if df.shape[0]>0: 
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Variant_SKU','Product_SKU'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='count', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True) 
                        vendor_perf=vendor_perf.set_index(['Variant_SKU','Product_SKU','Grand_Total']).reset_index().sort_values(by='Grand_Total', ascending=False)

                        vendor_perf.to_csv('logistics/logistics_reports/sku_product_del.csv', index=False)  

                        return render_template('sku_product_del.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,   
                                            )           

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('sku_product_del.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('sku_product_del.html', Start_Date=Start_Date, End_Date=End_Date)       


    return redirect('/')


@logisticsapp.route('/sku_product_del_csv', methods = ['POST', 'GET'])   
def sku_product_del_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/sku_product_del.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')





@logisticsapp.route('/shipment_export', methods = ['POST', 'GET']) 
def shipment_export():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Logistics().shipment_export(Start_Date, End_Date)  

                if df.shape[0]>0:
                    try:

                        df.to_csv('logistics/logistics_reports/logistics_shipment_export.csv', index=False)  
                        df=df.head(1000)  

                        return render_template('shipment_export.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )           
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('shipment_export.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('shipment_export.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Logistics().shipment_export(Start_Date, End_Date)  

                if df.shape[0]>0: 
                    try:
                        
                        df.to_csv('logistics/logistics_reports/logistics_shipment_export.csv', index=False)  
                        df=df.head(1000) 

                        return render_template('shipment_export.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )           

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('shipment_export.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('shipment_export.html', Start_Date=Start_Date, End_Date=End_Date)       


    return redirect('/')


@logisticsapp.route('/shipment_export_csv', methods = ['POST', 'GET'])   
def shipment_export_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/logistics_shipment_export.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@logisticsapp.route('/weight_summary', methods = ['POST', 'GET']) 
def weight_summary():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Logistics().weight_summary(Start_Date, End_Date)  

                if df.shape[0]>0:
                    try:

                        df.to_csv('logistics/logistics_reports/weight_summary.csv', index=False)   
                        df=df.head(1000)  

                        return render_template('weight_summary.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )           
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('weight_summary.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('weight_summary.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Logistics().weight_summary(Start_Date, End_Date)  

                if df.shape[0]>0: 
                    try:
                        
                        df.to_csv('logistics/logistics_reports/weight_summary.csv', index=False)  
                        df=df.head(1000) 

                        return render_template('weight_summary.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )           

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('weight_summary.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('weight_summary.html', Start_Date=Start_Date, End_Date=End_Date)       


    return redirect('/')


@logisticsapp.route('/weight_summary_csv', methods = ['POST', 'GET'])   
def weight_summary_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/weight_summary.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')




@logisticsapp.route('/fad_report', methods = ['POST', 'GET']) 
def fad_report():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['logistics']==1: 

            if request.method=='POST':     

                Seller_ID =request.form.get('Seller_ID') 
                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                if Seller_ID=='':
                    df=Logistics().fad_report(Start_Date, End_Date)   
                else:
                    df=Logistics().fad_report_seller(Seller_ID,Start_Date, End_Date)  


                if df.shape[0]>0: 
                    try:

                        total=df.pivot_table(index=['Seller_ID','Product_Name','Courier_Category'], values='AWB', aggfunc='count', 
                                    fill_value=0, margins=True, margins_name='Grand_Total').reset_index()
                        total.rename(columns={'AWB':'Grand_Total'}, inplace=True)

                        if 'FAD' in df.NDR_Attempts.values:
                            FAD=df[df.NDR_Attempts=='FAD'].pivot_table(index=['Seller_ID','Product_Name','Courier_Category'], columns='Ship_Status', values='AWB', 
                                        aggfunc='count', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()
                            FAD.rename(columns={'Grand_Total':'FAD'}, inplace=True) 

                        else:
                            FAD=pd.DataFrame(columns=['Seller_ID','Product_Name','Courier_Category']) 
                            FAD['Delivered']=0
                            FAD['FAD']=0 

                        final=total.merge(FAD, on=['Seller_ID','Product_Name','Courier_Category'], how='left').fillna(0)

                        final['Delivered_%']=((final.Delivered/final.Grand_Total)*100).astype(int).astype(str)+'%'
                        final['FAD_%']=((final.FAD/final.Grand_Total)*100).astype(int).astype(str)+'%'

                        if 'RTO' in final.columns:
                            final['RTO_%']=((final.RTO/final.Grand_Total)*100).astype(int).astype(str)+'%'
                        else:
                            final['RTO']=0
                            final['RTO_%']='0%' 

                        final=final.set_index(['Seller_ID','Product_Name','Courier_Category','Grand_Total','FAD','FAD_%']).sort_index(ascending=True, axis=1).reset_index()
                        final.drop(columns=['FAD','FAD_%'], inplace=True) 

                        final.to_csv('logistics/logistics_reports/fad_report.csv', index=False)   

                        return render_template('log_fad_report.html', 
                                            Start_Date=Start_Date, End_Date=End_Date, Seller_ID=Seller_ID, 
                                            d1=final.values, d1col=final.columns.values,   
                                            )         
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_fad_report.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_fad_report.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Logistics().fad_report(Start_Date, End_Date)  
                print(df.shape) 

                if df.shape[0]>0: 
                    try:

                        total=df.pivot_table(index=['Seller_ID','Product_Name','Courier_Category'], values='AWB', aggfunc='count', 
                                    fill_value=0, margins=True, margins_name='Grand_Total').reset_index()
                        total.rename(columns={'AWB':'Grand_Total'}, inplace=True)

                        if 'FAD' in df.NDR_Attempts.values:
                            FAD=df[df.NDR_Attempts=='FAD'].pivot_table(index=['Seller_ID','Product_Name','Courier_Category'], columns='Ship_Status', values='AWB', 
                                        aggfunc='count', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()
                            FAD.rename(columns={'Grand_Total':'FAD'}, inplace=True) 

                        else:
                            FAD=pd.DataFrame(columns=['Seller_ID','Product_Name','Courier_Category']) 
                            FAD['Delivered']=0
                            FAD['FAD']=0 

                        final=total.merge(FAD, on=['Seller_ID','Product_Name','Courier_Category'], how='left').fillna(0)

                        final['Delivered_%']=((final.Delivered/final.Grand_Total)*100).astype(int).astype(str)+'%'
                        final['FAD_%']=((final.FAD/final.Grand_Total)*100).astype(int).astype(str)+'%'

                        if 'RTO' in final.columns:
                            final['RTO_%']=((final.RTO/final.Grand_Total)*100).astype(int).astype(str)+'%'
                        else:
                            final['RTO']=0
                            final['RTO_%']='0%' 

                        final=final.set_index(['Seller_ID','Product_Name','Courier_Category','Grand_Total','FAD','FAD_%']).sort_index(ascending=True, axis=1).reset_index()
                        final.drop(columns=['FAD','FAD_%'], inplace=True) 

                        final.to_csv('logistics/logistics_reports/fad_report.csv', index=False)   

                        return render_template('log_fad_report.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=final.values, d1col=final.columns.values,   
                                            )         

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('log_fad_report.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('log_fad_report.html', Start_Date=Start_Date, End_Date=End_Date)       


    return redirect('/')  


@logisticsapp.route('/fad_report_csv', methods = ['POST', 'GET'])   
def fad_report_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['logistics']==1: 
            return send_file('logistics/logistics_reports/fad_report.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )   

    return redirect('/') 

