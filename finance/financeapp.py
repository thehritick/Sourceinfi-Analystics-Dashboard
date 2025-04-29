
from operator import index
from finance.finance import Finance
import pandas as pd
from users import User
from flask import Flask, Blueprint, render_template,request,send_file, redirect, url_for, Response,session
from datetime import date, timedelta, datetime
import time
import numpy as np
import pytz 



financeapp = Blueprint('financeapp', __name__, template_folder='templates', static_folder='static', url_prefix='/financeapp')


@financeapp.route('/', methods = ['POST', 'GET']) 
@financeapp.route('/p_and_l', methods = ['POST', 'GET'])  
def p_and_l():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['finance']==1: 

            if request.method=='POST':      

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                df=Finance().finance_report(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        Expense=request.form.get('Expense') 
                        Extra_Weight_Charges=request.form.get('Extra_Weight_Charges')   

                        if Expense=='':
                            Expense=0
                        if Extra_Weight_Charges=='':
                            Extra_Weight_Charges=0 

                        pnl=pd.DataFrame({'Ship_Count':[df['AWB'].sum().astype(int)],
                                        'Sales':[df['Selling_Price'].sum().astype(int)],
                                        'Purchase':[df['Applied_B2B_Price'].sum().astype(int)],
                                        'Expense':Expense,
                                        'Extra_Weight_Charges':Extra_Weight_Charges 
                                        }) 

                        pnl['Final_PL']=(pnl.Sales-pnl.Purchase-pnl.Expense.astype(int)-pnl.Extra_Weight_Charges.astype(int))   
                        pnl=pnl.round(2)  

                        return render_template('p_and_l.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            Expense=pnl['Expense'][0], Extra_Weight_Charges=pnl['Extra_Weight_Charges'][0], 
                                            d1=pnl.values, d1col=pnl.columns.values,  
                                            )                             

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('p_and_l.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('p_and_l.html', Start_Date=Start_Date, End_Date=End_Date)       


            else:  

                Start_Date = str(date.today()-timedelta(1))    
                End_Date = str(date.today())   

                df=Finance().finance_report(Start_Date, End_Date) 

                if df.shape[0]>0:
                    try:

                        pnl=pd.DataFrame({'Ship_Count':[df['AWB'].sum().astype(int)],
                                        'Sales':[df['Selling_Price'].sum().astype(int)],
                                        'Purchase':[df['Applied_B2B_Price'].sum().astype(int)],
                                        'Expense':[0],
                                        'Extra_Weight_Charges':[0] 
                                        }) 

                        pnl['Final_PL']=(pnl.Sales-pnl.Purchase-pnl.Expense-pnl.Extra_Weight_Charges)  
                        pnl=pnl.round(2)  
                        
                        return render_template('p_and_l.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                                Expense=pnl['Expense'][0], Extra_Weight_Charges=pnl['Extra_Weight_Charges'][0], 
                                            d1=pnl.values, d1col=pnl.columns.values,   
                                            )      

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('p_and_l.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('p_and_l.html', Start_Date=Start_Date, End_Date=End_Date)       


    return redirect('/')



@financeapp.route('/out_cod_pay_seller', methods = ['POST', 'GET']) 
def out_cod_pay_seller():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['finance']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                seller_none=Finance().seller_none(Start_Date, End_Date)  
                utr_none=Finance().utr_none(Start_Date, End_Date)  

                if seller_none.shape[0]>0: 
                    try:

                        seller_none.to_csv('finance/finance_reports/seller_none.csv', index=False) 
                        utr_none.to_csv('finance/finance_reports/utr_none.csv', index=False) 

                        seller_none=seller_none.head(1000) 
                        utr_none=utr_none.head(1000) 

                        return render_template('out_cod_pay_seller.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_none.values, d1col=seller_none.columns.values,  
                                            d2=utr_none.values, d2col=utr_none.columns.values,    
                                            )         
            
                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('out_cod_pay_seller.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('out_cod_pay_seller.html', Start_Date=Start_Date, End_Date=End_Date)   

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                seller_none=Finance().seller_none(Start_Date, End_Date)  
                utr_none=Finance().utr_none(Start_Date, End_Date)  

                if seller_none.shape[0]>0:
                    try:

                        seller_none.to_csv('finance/finance_reports/seller_none.csv', index=False) 
                        utr_none.to_csv('finance/finance_reports/utr_none.csv', index=False)

                        seller_none=seller_none.head(1000) 
                        utr_none=utr_none.head(1000) 

                        return render_template('out_cod_pay_seller.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=seller_none.values, d1col=seller_none.columns.values,  
                                            d2=utr_none.values, d2col=utr_none.columns.values,    
                                            )           

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('out_cod_pay_seller.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('out_cod_pay_seller.html', Start_Date=Start_Date, End_Date=End_Date)    

    return redirect('/')


@financeapp.route('/seller_none_csv', methods = ['POST', 'GET'])  
def seller_none_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['finance']==1: 
            return send_file('finance/finance_reports/seller_none.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')


@financeapp.route('/utr_none_csv', methods = ['POST', 'GET'])  
def utr_none_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['finance']==1: 
            return send_file('finance/finance_reports/utr_none.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')


@financeapp.route('/finance_sales_summary', methods = ['POST', 'GET']) 
def finance_sales_summary():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['finance']==1: 

            if request.method=='POST':     

                Start_Date =request.form.get('Start_Date') 
                End_Date =request.form.get('End_Date')    
                
                if Start_Date < str(date.today()- timedelta(60)):
                    Start_Date=str(date.today()- timedelta(60))
                else:
                    Start_Date=Start_Date 

                df=Finance().finance_sales_summary(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        df.to_csv('finance/finance_reports/invoice_summary.csv', index=False) 
                        df=df.head(1000) 

                        return render_template('finance_sales_summary.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )      

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('finance_sales_summary.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('finance_sales_summary.html', Start_Date=Start_Date, End_Date=End_Date)     

            else:  

                Start_Date = str(date.today()-timedelta(7))    
                End_Date = str(date.today())   

                df=Finance().finance_sales_summary(Start_Date, End_Date) 

                if df.shape[0]>0: 
                    try:

                        df.to_csv('finance/finance_reports/invoice_summary.csv', index=False) 
                        df=df.head(1000) 

                        return render_template('finance_sales_summary.html', 
                                            Start_Date=Start_Date, End_Date=End_Date,
                                            d1=df.values, d1col=df.columns.values,   
                                            )      

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('finance_sales_summary.html', Start_Date=Start_Date, End_Date=End_Date)     

                return render_template('finance_sales_summary.html', Start_Date=Start_Date, End_Date=End_Date)     


    return redirect('/') 



@financeapp.route('/finance_sales_summary_csv', methods = ['POST', 'GET'])  
def finance_sales_summary_csv():   
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['finance']==1: 
            return send_file('finance/finance_reports/invoice_summary.csv',   
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/')




@financeapp.route('/risk_wallet_balance', methods = ['POST', 'GET']) 
def risk_wallet_balance():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['finance']==1: 
            
            return render_template('risk_wallet_balance.html')         

    return redirect('/')


@financeapp.route('/weight_dispute', methods = ['POST', 'GET']) 
def weight_dispute():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['finance']==1: 
            
            return render_template('weight_dispute_finance.html')         

    return redirect('/')

