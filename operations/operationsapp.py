
from operator import index
from operations.operations import Operations
import pandas as pd
from users import User
from flask import Flask, Blueprint, render_template,request,send_file, redirect, url_for, Response,session
from datetime import date, timedelta, datetime
import time
import numpy as np
import pytz 



operationsapp = Blueprint('operationsapp', __name__, template_folder='templates', static_folder='static', url_prefix='/operationsapp')


@operationsapp.route('/', methods = ['POST', 'GET']) 
@operationsapp.route('/cod_reconcilation', methods = ['POST', 'GET']) 
def cod_reconcilation():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['operations']==1: 
                     
            return render_template('cod_reconcilation.html') 
            
    return redirect('/')


@operationsapp.route('/courierwise_perf', methods = ['POST', 'GET']) 
def courierwise_perf():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['operations']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Operations().courierwise_perf(Start_Date, End_Date) 

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
                        
                        vendor_perf.to_csv('operations/operations_reports/courierwise_perf.csv', index=False)  

                        return render_template('courierwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)      


            else: 

                Start_Date = str(date.today()-timedelta(7))     
                End_Date = str(date.today())   

                df=Operations().courierwise_perf(Start_Date, End_Date) 

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
                        

                        vendor_perf.to_csv('operations/operations_reports/courierwise_perf.csv', index=False)  

                        return render_template('courierwise_perf.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=vendor_perf.values, d1col=vendor_perf.columns.values,  
                                            )          
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('courierwise_perf.html', Start_Date=Start_Date, End_Date=End_Date)      


            
    return redirect('/')


@operationsapp.route('/courierwise_perf_csv', methods = ['POST', 'GET'])   
def courierwise_perf_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['operations']==1: 
            return send_file('operations/operations_reports/courierwise_perf.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')





@operationsapp.route('/weight_dispute', methods = ['POST', 'GET']) 
def weight_dispute():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['operations']==1: 
                     
            return render_template('weight_dispute.html')  
            
    return redirect('/')



@operationsapp.route('/ndr_reason', methods = ['POST', 'GET']) 
def ndr_reason():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['operations']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Operations().ndr_reason(Start_Date, End_Date) 

                if df.shape[0]>0:  
                    try:

                        df.to_csv('operations/operations_reports/ndr_reason.csv', index=False)  

                        df.rename(columns={'AWB':'Ship_Count'}, inplace=True)  
                        df=df.pivot_table(index=['Ship_Status','NDR_Remarks'], columns='Total_Attempts', values='Ship_Count', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value=0).reset_index().sort_values(by='Grand_Total', ascending=False)

                        df=df[df.Grand_Total>20] 
                        df=df[~((df.NDR_Remarks=='') & (df.Ship_Status!='Grand_Total'))] 

                        return render_template('ndr_reason.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('ndr_reason.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('ndr_reason.html', Start_Date=Start_Date, End_Date=End_Date)      


            else: 

                Start_Date = str(date.today()-timedelta(7))     
                End_Date = str(date.today())   

                df=Operations().ndr_reason(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        df.to_csv('operations/operations_reports/ndr_reason_raw_data.csv', index=False)  

                        df.rename(columns={'AWB':'Ship_Count'}, inplace=True)  
                        df=df.pivot_table(index=['Ship_Status','NDR_Remarks'], columns='Total_Attempts', values='Ship_Count', aggfunc='count', margins=True, 
                                    margins_name='Grand_Total', fill_value=0).reset_index().sort_values(by='Grand_Total', ascending=False)

                        df=df[df.Grand_Total>20] 
                        df=df[~((df.NDR_Remarks=='') & (df.Ship_Status!='Grand_Total'))]

                        return render_template('ndr_reason.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('ndr_reason.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('ndr_reason.html', Start_Date=Start_Date, End_Date=End_Date)      

            
    return redirect('/')


@operationsapp.route('/ndr_reason_raw_data', methods = ['POST', 'GET'])  
def ndr_reason_raw_data():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['operations']==1: 
            return send_file('operations/operations_reports/ndr_reason_raw_data.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')



@operationsapp.route('/cancelled_reason', methods = ['POST', 'GET']) 
def cancelled_reason():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['operations']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Operations().cancelled_reason(Start_Date, End_Date) 

                if df.shape[0]>0:   
                    try:

                        df.to_csv('operations/operations_reports/cancelled_reason_raw_data.csv', index=False)  

                        df=df.sort_values(by=['Order_ID','Order_Date'], ascending=[False, False]).drop_duplicates(subset='Order_ID', keep='first')
                        df=df[df.Reason.isna()!=True]

                        df.Reason=df.Reason.str.replace('"','').str.title().str.split()

                        df['Cancelled_Reason']=df.Reason.transform(lambda x: 'Non Serviceable' if 'Serviceability' in x 
                                                                    else ('Low Wallet Balance' if 'Wallet' in x
                                                                    else ('Pincode Unavailable' if 'Pincode' in x else 'Others')))

                        df=df.pivot_table(index='Cancelled_Reason', values='Order_ID', aggfunc='count', margins=True, 
                                          margins_name='Grand_Total').reset_index().sort_values(by='Order_ID', ascending=False) 
                        df=df.rename(columns={'Order_ID':'Orders'}) 

                        return render_template('cancelled_reason.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,  
                                            )         
                    
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('cancelled_reason.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('cancelled_reason.html', Start_Date=Start_Date, End_Date=End_Date)      


            else: 

                Start_Date = str(date.today()-timedelta(7))     
                End_Date = str(date.today())   

                df=Operations().cancelled_reason(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        df.to_csv('operations/operations_reports/cancelled_reason_raw_data.csv', index=False)  

                        df=df.sort_values(by=['Order_ID','Order_Date'], ascending=[False, False]).drop_duplicates(subset='Order_ID', keep='first')
                        df=df[df.Reason.isna()!=True]

                        df.Reason=df.Reason.str.replace('"','').str.title().str.split()

                        df['Cancelled_Reason']=df.Reason.transform(lambda x: 'Non Serviceable' if 'Serviceability' in x 
                                                                    else ('Low Wallet Balance' if 'Wallet' in x
                                                                    else ('Pincode Unavailable' if 'Pincode' in x else 'Others')))

                        df=df.pivot_table(index='Cancelled_Reason', values='Order_ID', aggfunc='count', margins=True, 
                                        margins_name='Grand_Total').reset_index().sort_values(by='Order_ID', ascending=False)
                        df=df.rename(columns={'Order_ID':'Orders'})  
                        
                        return render_template('cancelled_reason.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,  
                                            )         
                        
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('cancelled_reason.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('cancelled_reason.html', Start_Date=Start_Date, End_Date=End_Date)      

            
    return redirect('/')



@operationsapp.route('/cancelled_reason_raw_data', methods = ['POST', 'GET'])    
def cancelled_reason_raw_data():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['operations']==1: 
            return send_file('operations/operations_reports/cancelled_reason_raw_data.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')

