
from operator import index
from vendor.vendor import Vendor
import pandas as pd
from users import User
from flask import Flask, Blueprint, render_template,request,send_file, redirect, url_for, Response,session
from datetime import date, timedelta, datetime
import time
import numpy as np
import pytz 



vendorapp = Blueprint('vendorapp', __name__, template_folder='templates', static_folder='static', url_prefix='/vendorapp')


@vendorapp.route('/AWB_Search', methods = ['POST', 'GET']) 
def AWB_Search():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1:                      
                    
            if request.method=='POST':     

                # AWB =request.form.get('AWB') 

                jsond = request.get_json() 
                print(jsond)
                AWB=jsond["AWB"] 

                df=Vendor().AWB_Search(AWB)     

                if df.shape[0]>0:
                    try:
                        data={'AWB':df.AWB[0], 'Reason':df.Reason[0]} 
                        return data 
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return 0    

                return 0 

            else:  
                return 0 
            
    return redirect('/')




@vendorapp.route('/', methods = ['POST', 'GET'])   
@vendorapp.route('/del_percent', methods = ['POST', 'GET']) 
def del_percent():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1:                      
                    
            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Vendor().del_percent(Start_Date, End_Date)   

                if df.shape[0]>0:
                    try:

                        product_margin=df.pivot_table(index=['Variant_SKU','Product_Name','Pricing_Plan'], values=['AWB', 'Product_Margin'], aggfunc={'AWB':'count', 'Product_Margin':'mean'}, 
                                                    fill_value=0, margins=True, margins_name='Grand_Total').reset_index().sort_values(by='AWB', ascending=False)

                        product_margin.rename(columns={'AWB':'Ships', 'Product_Margin':'Avg_Product_Margin'}, inplace=True)

                        Delivered=df[df.Ship_Status=='Delivered'].pivot_table(index=['Variant_SKU','Product_Name','Pricing_Plan'], values='AWB', aggfunc='count', 
                                                                    margins=True, margins_name='Grand_Total').reset_index()
                        Delivered.rename(columns={'AWB':'Delivered'}, inplace=True)

                        product_margin=product_margin.merge(Delivered, on=['Variant_SKU','Product_Name','Pricing_Plan'], how='left').fillna(0)

                        product_margin['Delivered_%']=round((product_margin['Delivered']/product_margin['Ships'])*100,2).astype(str)+'%'

                        product_margin.insert(6, 'Avg_Product_Margin', product_margin.pop('Avg_Product_Margin'))

                        product_margin['Avg_Product_Margin']=product_margin['Avg_Product_Margin'].astype(int)
                        product_margin['Delivered']=product_margin['Delivered'].astype(int)

                        product_margin.to_csv('vendor/vendor_reports/del_percent.csv', index=False)  

                        return render_template('del_percent.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=product_margin.values, d1col=product_margin.columns.values,   
                                            )         
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('del_percent.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('del_percent.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Vendor().del_percent(Start_Date, End_Date)  

                if df.shape[0]>0:
                    try:

                        product_margin=df.pivot_table(index=['Variant_SKU','Product_Name','Pricing_Plan'], values=['AWB', 'Product_Margin'], aggfunc={'AWB':'count', 'Product_Margin':'mean'}, 
                                                    fill_value=0, margins=True, margins_name='Grand_Total').reset_index().sort_values(by='AWB', ascending=False)

                        product_margin.rename(columns={'AWB':'Ships', 'Product_Margin':'Avg_Product_Margin'}, inplace=True)

                        Delivered=df[df.Ship_Status=='Delivered'].pivot_table(index=['Variant_SKU','Product_Name','Pricing_Plan'], values='AWB', aggfunc='count', 
                                                                    margins=True, margins_name='Grand_Total').reset_index()
                        Delivered.rename(columns={'AWB':'Delivered'}, inplace=True)

                        product_margin=product_margin.merge(Delivered, on=['Variant_SKU','Product_Name','Pricing_Plan'], how='left').fillna(0)

                        product_margin['Delivered_%']=round((product_margin['Delivered']/product_margin['Ships'])*100,2).astype(str)+'%'

                        product_margin.insert(6, 'Avg_Product_Margin', product_margin.pop('Avg_Product_Margin')) 

                        product_margin['Avg_Product_Margin']=product_margin['Avg_Product_Margin'].astype(int)
                        product_margin['Delivered']=product_margin['Delivered'].astype(int) 

                        product_margin.to_csv('vendor/vendor_reports/del_percent.csv', index=False)  

                        return render_template('del_percent.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=product_margin.values, d1col=product_margin.columns.values,   
                                            )           

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('del_percent.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('del_percent.html', Start_Date=Start_Date, End_Date=End_Date)                
            
    return redirect('/')



@vendorapp.route('/del_percent_csv', methods = ['POST', 'GET'])    
def del_percent_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/del_percent.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@vendorapp.route('/pendency_tat', methods = ['POST', 'GET'])   
def pendency_tat():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1: 

            if request.method=='POST':     

                Vendor_ID =request.form.get('Vendor_ID')  
                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                if Vendor_ID=='':
                    df=Vendor().pendency_tat(Start_Date, End_Date)  
                else:
                    df=Vendor().pendency_tat_vendor(Vendor_ID,Start_Date, End_Date)   

                if df.shape[0]>0: 
                    try:

                        df.to_csv('vendor/vendor_reports/pendency_tat_raw_data.csv', index=False)  
                        
                        try:
                            pick_vs_approval=df.pivot_table(index='Diff_Pickup_vs_Approval', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total').reset_index().rename(columns={'AWB':'Shipments'})
                            pick_vs_approval['Ship_%']=round((pick_vs_approval['Shipments']/pick_vs_approval['Shipments'].sum())*200,2).astype(str)+'%'

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            pick_vs_approval=pd.DataFrame([]) 


                        try:
                            pick_vs_order=df.pivot_table(index='Diff_Pickup_vs_Order', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total').reset_index().rename(columns={'AWB':'Shipments'})
                            pick_vs_order['Ship_%']=round((pick_vs_order['Shipments']/pick_vs_order['Shipments'].sum())*200,2).astype(str)+'%'

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            pick_vs_order=pd.DataFrame([]) 


                        try:
                            vendor_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index='Vendor_Name', columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False)

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            vendor_datewise=pd.DataFrame([])                             


                        try:
                            seller_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index='Seller_Name', columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False)

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            seller_datewise=pd.DataFrame([])                             
                        

                        try: 
                            product_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index=['Variant_SKU','Product_Name'], columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False) 

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            product_datewise=pd.DataFrame([])                              
                        

                        return render_template('vendor_pendency_tat.html', 
                                            Start_Date=Start_Date, End_Date=End_Date, Vendor_ID=Vendor_ID,
                                            d1=pick_vs_approval.values, d1col=pick_vs_approval.columns.values,   
                                            d2=pick_vs_order.values, d2col=pick_vs_order.columns.values,   
                                            d3=vendor_datewise.values, d3col=vendor_datewise.columns.values,   
                                            d4=seller_datewise.values, d4col=seller_datewise.columns.values,   
                                            d5=product_datewise.values, d5col=product_datewise.columns.values,    
                                            )      
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_pendency_tat.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_pendency_tat.html', Start_Date=Start_Date, End_Date=End_Date)      
            

            else: 

                Vendor_ID='' 
                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                if Vendor_ID=='':
                    df=Vendor().pendency_tat(Start_Date, End_Date)  
                else:
                    df=Vendor().pendency_tat_vendor(Vendor_ID,Start_Date, End_Date)  

                if df.shape[0]>0: 
                    try:

                        df.to_csv('vendor/vendor_reports/pendency_tat_raw_data.csv', index=False)  
                        
                        try:
                            pick_vs_approval=df.pivot_table(index='Diff_Pickup_vs_Approval', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total').reset_index().rename(columns={'AWB':'Shipments'})
                            pick_vs_approval['Ship_%']=round((pick_vs_approval['Shipments']/pick_vs_approval['Shipments'].sum())*200,2).astype(str)+'%'

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            pick_vs_approval=pd.DataFrame([]) 


                        try:
                            pick_vs_order=df.pivot_table(index='Diff_Pickup_vs_Order', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total').reset_index().rename(columns={'AWB':'Shipments'}) 
                            pick_vs_order['Ship_%']=round((pick_vs_order['Shipments']/pick_vs_order['Shipments'].sum())*200,2).astype(str)+'%'

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            pick_vs_order=pd.DataFrame([]) 


                        try:
                            vendor_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index='Vendor_Name', columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                                            margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False)

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            vendor_datewise=pd.DataFrame([])                             


                        try:
                            seller_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index='Seller_Name', columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False)

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            seller_datewise=pd.DataFrame([])                             
                        

                        try: 
                            product_datewise=df[df.Ship_Status=='pending pickup'].pivot_table(index=['Variant_SKU','Product_Name'], columns='Order_Date', values='AWB', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value='').reset_index().sort_values(by='Grand_Total', ascending=False)

                        except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                            product_datewise=pd.DataFrame([])                    
                        

                        return render_template('vendor_pendency_tat.html', 
                                            Start_Date=Start_Date, End_Date=End_Date, Vendor_ID=Vendor_ID, 
                                            d1=pick_vs_approval.values, d1col=pick_vs_approval.columns.values,   
                                            d2=pick_vs_order.values, d2col=pick_vs_order.columns.values,   
                                            d3=vendor_datewise.values, d3col=vendor_datewise.columns.values,   
                                            d4=seller_datewise.values, d4col=seller_datewise.columns.values,   
                                            d5=product_datewise.values, d5col=product_datewise.columns.values,    
                                            )      
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_pendency_tat.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_pendency_tat.html', Start_Date=Start_Date, End_Date=End_Date)      

    return redirect('/')




@vendorapp.route('/pendency_tat_raw_data', methods = ['POST', 'GET'])   
def pendency_tat_raw_data():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/pendency_tat_raw_data.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@vendorapp.route('/courier_performance', methods = ['POST', 'GET']) 
def courier_performance():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1:                

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Vendor().courier_performance_vendor(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Courier'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True)
                        vendor_perf=vendor_perf.set_index(['Courier','Grand_Total']
                                                                            ).reset_index().sort_values(by='Grand_Total', ascending=False)
                        

                        vendor_perf.to_csv('vendor/vendor_reports/courier_performance_vendor.csv', index=False)  

                        return render_template('courier_performance_vendor.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('courier_performance_vendor.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('courier_performance_vendor.html', Start_Date=Start_Date, End_Date=End_Date)      


            else: 

                Start_Date = str(date.today()-timedelta(7))     
                End_Date = str(date.today())   

                df=Vendor().courier_performance_vendor(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        vendor_perf=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Courier'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        vendor_perf['Delivered_%']=((vendor_perf['Delivered']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Exception_%']=((vendor_perf['Exception']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['In_Transit_%']=((vendor_perf['In Transit']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['Out_For_Delivery_%']=((vendor_perf['Out For Delivery']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'
                        vendor_perf['RTO_%']=((vendor_perf['RTO']/vendor_perf['Grand_Total'])*100).astype(int).astype(str)+'%'

                        vendor_perf

                        vendor_perf=vendor_perf.sort_index(axis=1, ascending=True)
                        vendor_perf=vendor_perf.set_index(['Courier','Grand_Total']
                                                                            ).reset_index().sort_values(by='Grand_Total', ascending=False)
                        
                        vendor_perf.to_csv('vendor/vendor_reports/courier_performance_vendor.csv', index=False)  

                        return render_template('courier_performance_vendor.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('courier_performance_vendor.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('courier_performance_vendor.html', Start_Date=Start_Date, End_Date=End_Date)      

  
            
    return redirect('/')


@vendorapp.route('/courier_performance_vendor_csv', methods = ['POST', 'GET'])   
def courier_performance_vendor_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/courier_performance_vendor.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/') 



@vendorapp.route('/vendor_perf', methods = ['POST', 'GET'])  
def vendor_perf():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1:                  


            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Vendor().vendor_perf(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        seller_courier_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Vendor_Name','Courier_Category'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_courier_status['Delivered_%']=((seller_courier_status['Delivered']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['Exception_%']=((seller_courier_status['Exception']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['In_Transit_%']=((seller_courier_status['In Transit']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['Out_For_Delivery_%']=((seller_courier_status['Out For Delivery']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['RTO_%']=((seller_courier_status['RTO']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_courier_status

                        seller_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Vendor_Name'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_status['Delivered_%']=((seller_status['Delivered']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Exception_%']=((seller_status['Exception']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['In_Transit_%']=((seller_status['In Transit']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Out_For_Delivery_%']=((seller_status['Out For Delivery']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['RTO_%']=((seller_status['RTO']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_status

                        seller_courier_merge=pd.concat([seller_status, seller_courier_status])
                        seller_courier_merge=seller_courier_merge[seller_courier_merge.Vendor_Name!='Grand_Total']
                        seller_courier_merge

                        seller_courier_merge=seller_courier_merge.sort_index(axis=1, ascending=True)
                        seller_courier_merge=seller_courier_merge.set_index(['Vendor_Name','Courier_Category','Grand_Total']).reset_index()
                        seller_courier_merge=seller_courier_merge.sort_values(by=['Vendor_Name','Courier_Category'], ascending=[True,True])

                        seller_courier_merge['Courier_Category'].fillna('Grand_Total', inplace=True)
                        seller_courier_merge 

                        seller_courier_merge.to_csv('vendor/vendor_reports/vendor_perf.csv', index=False)  

                        return render_template('vendor_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_courier_merge.values, d1col=seller_courier_merge.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_perf.html', Start_Date=Start_Date, End_Date=End_Date)         


            else:   

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Vendor().vendor_perf(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        seller_courier_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Vendor_Name','Courier_Category'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_courier_status['Delivered_%']=((seller_courier_status['Delivered']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['Exception_%']=((seller_courier_status['Exception']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['In_Transit_%']=((seller_courier_status['In Transit']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['Out_For_Delivery_%']=((seller_courier_status['Out For Delivery']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_courier_status['RTO_%']=((seller_courier_status['RTO']/seller_courier_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_courier_status

                        seller_status=df[df.Ship_Status.isin(['Delivered','Exception','In Transit','Out For Delivery','RTO'])].pivot_table(
                                                index=['Vendor_Name'], columns=['Ship_Status'], values='AWB', 
                                                aggfunc='sum', fill_value=0, margins=True, margins_name='Grand_Total').reset_index()

                        seller_status['Delivered_%']=((seller_status['Delivered']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Exception_%']=((seller_status['Exception']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['In_Transit_%']=((seller_status['In Transit']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['Out_For_Delivery_%']=((seller_status['Out For Delivery']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'
                        seller_status['RTO_%']=((seller_status['RTO']/seller_status['Grand_Total'])*100).astype(int).astype(str)+'%'

                        seller_status

                        seller_courier_merge=pd.concat([seller_status, seller_courier_status])
                        seller_courier_merge=seller_courier_merge[seller_courier_merge.Vendor_Name!='Grand_Total']
                        seller_courier_merge

                        seller_courier_merge=seller_courier_merge.sort_index(axis=1, ascending=True)
                        seller_courier_merge=seller_courier_merge.set_index(['Vendor_Name','Courier_Category','Grand_Total']).reset_index()
                        seller_courier_merge=seller_courier_merge.sort_values(by=['Vendor_Name','Courier_Category'], ascending=[True,True])

                        seller_courier_merge['Courier_Category'].fillna('Grand_Total', inplace=True)
                        seller_courier_merge 

                        seller_courier_merge.to_csv('vendor/vendor_reports/vendor_perf.csv', index=False)  

                        return render_template('vendor_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_courier_merge.values, d1col=seller_courier_merge.columns.values,   
                                            )      

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_perf.html', Start_Date=Start_Date, End_Date=End_Date)            

       
            
    return redirect('/')


@vendorapp.route('/vendor_perf_csv', methods = ['POST', 'GET'])   
def vendor_perf_csv():    
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/vendor_perf.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/') 






@vendorapp.route('/vendor_out_cod_pay', methods = ['POST', 'GET'])  
def vendor_out_cod_pay():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                vendor_none=Vendor().vendor_none(Start_Date, End_Date)  
                utr_none=Vendor().utr_none(Start_Date, End_Date)  

                if vendor_none.shape[0]>0: 
                    try:

                        vendor_none.to_csv('vendor/vendor_reports/vendor_none.csv', index=False)
                        utr_none.to_csv('vendor/vendor_reports/utr_none.csv', index=False) 

                        vendor_none=vendor_none.head(1000)
                        utr_none=utr_none.head(1000) 

                        return render_template('vendor_out_cod_pay.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_none.values, d1col=vendor_none.columns.values,  
                                            d2=utr_none.values, d2col=utr_none.columns.values,    
                                            )         
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_out_cod_pay.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_out_cod_pay.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                vendor_none=Vendor().vendor_none(Start_Date, End_Date)  
                utr_none=Vendor().utr_none(Start_Date, End_Date)  

                if vendor_none.shape[0]>0: 
                    try:

                        vendor_none.to_csv('vendor/vendor_reports/vendor_none.csv', index=False)
                        utr_none.to_csv('vendor/vendor_reports/utr_none.csv', index=False) 

                        vendor_none=vendor_none.head(1000) 
                        utr_none=utr_none.head(1000) 

                        return render_template('vendor_out_cod_pay.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_none.values, d1col=vendor_none.columns.values,  
                                            d2=utr_none.values, d2col=utr_none.columns.values,    
                                            )         
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_out_cod_pay.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_out_cod_pay.html', Start_Date=Start_Date, End_Date=End_Date)   

    return redirect('/')


@vendorapp.route('/vendor_none_csv', methods = ['POST', 'GET'])  
def vendor_none_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/vendor_none.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')


@vendorapp.route('/utr_none_csv', methods = ['POST', 'GET'])  
def utr_none_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/utr_none.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@vendorapp.route('/kam_performance', methods = ['POST', 'GET'])  
def kam_performance():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['vendor']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Vendor().kam_performance(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        ss=df[df.Orders!=0] 

                        CM=df[df.Onboard_Date>=date.today().replace(day=1)]
                        CM=CM.groupby('Account_Manager').agg({'Seller_ID':'count'}).reset_index()
                        CM.rename(columns={'Seller_ID':'Seller_Count'}, inplace=True)

                        CM_A=ss[ss.Onboard_Date>=date.today().replace(day=1)]
                        CM_A=CM_A.groupby('Account_Manager').agg({'Seller_ID':'count','Orders':'sum'}).reset_index()
                        CM_A.rename(columns={'Seller_ID':'Active_Sellers'}, inplace=True)
                        CM=CM.merge(CM_A, on='Account_Manager', how='left').fillna(0) 

                        PM=df[df.Onboard_Date<date.today().replace(day=1)]
                        PM=PM.groupby('Account_Manager').agg({'Seller_ID':'count'}).reset_index() 
                        PM.rename(columns={'Seller_ID':'Seller_Count'}, inplace=True) 

                        PM_A=ss[ss.Onboard_Date<date.today().replace(day=1)]
                        PM_A=PM_A.groupby('Account_Manager').agg({'Seller_ID':'count','Orders':'sum'}).reset_index()
                        PM_A.rename(columns={'Seller_ID':'Active_Sellers'}, inplace=True)
                        PM=PM.merge(PM_A, on='Account_Manager', how='left').fillna(0) 

                        CM.to_csv('vendor/vendor_reports/kam_performance_new_volume.csv', index=False)  
                        PM.to_csv('vendor/vendor_reports/kam_performance_existing_volume.csv', index=False)  

                        return render_template('vendor_kam_performance.html', 
                                            Start_Date=Start_Date, End_Date=End_Date, 
                                            d1=CM.values, d1col=CM.columns.values,  
                                            d2=PM.values, d2col=PM.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_kam_performance.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_kam_performance.html', Start_Date=Start_Date, End_Date=End_Date)         


            else:   

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Vendor().kam_performance(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try: 

                        ss=df[df.Orders!=0] 

                        CM=df[df.Onboard_Date>=date.today().replace(day=1)]
                        CM=CM.groupby('Account_Manager').agg({'Seller_ID':'count'}).reset_index()
                        CM.rename(columns={'Seller_ID':'Seller_Count'}, inplace=True)

                        CM_A=ss[ss.Onboard_Date>=date.today().replace(day=1)]
                        CM_A=CM_A.groupby('Account_Manager').agg({'Seller_ID':'count','Orders':'sum'}).reset_index()
                        CM_A.rename(columns={'Seller_ID':'Active_Sellers'}, inplace=True)
                        CM=CM.merge(CM_A, on='Account_Manager', how='left').fillna(0) 

                        PM=df[df.Onboard_Date<date.today().replace(day=1)]
                        PM=PM.groupby('Account_Manager').agg({'Seller_ID':'count'}).reset_index() 
                        PM.rename(columns={'Seller_ID':'Seller_Count'}, inplace=True) 

                        PM_A=ss[ss.Onboard_Date<date.today().replace(day=1)]
                        PM_A=PM_A.groupby('Account_Manager').agg({'Seller_ID':'count','Orders':'sum'}).reset_index()
                        PM_A.rename(columns={'Seller_ID':'Active_Sellers'}, inplace=True)
                        PM=PM.merge(PM_A, on='Account_Manager', how='left').fillna(0) 

                        CM.to_csv('vendor/vendor_reports/kam_performance_new_volume.csv', index=False)  
                        PM.to_csv('vendor/vendor_reports/kam_performance_existing_volume.csv', index=False)  

                        return render_template('vendor_kam_performance.html', 
                                            Start_Date=Start_Date, End_Date=End_Date, 
                                            d1=CM.values, d1col=CM.columns.values,  
                                            d2=PM.values, d2col=PM.columns.values,  
                                            )         

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('vendor_kam_performance.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('vendor_kam_performance.html', Start_Date=Start_Date, End_Date=End_Date)            


    return redirect('/') 




@vendorapp.route('/kam_performance_new_volume', methods = ['POST', 'GET'])   
def kam_performance_new_volume():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/kam_performance_new_volume.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@vendorapp.route('/kam_performance_existing_volume', methods = ['POST', 'GET'])   
def kam_performance_existing_volume():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['vendor']==1: 
            return send_file('vendor/vendor_reports/kam_performance_existing_volume.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')


