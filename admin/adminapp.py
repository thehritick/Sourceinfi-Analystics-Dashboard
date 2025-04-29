
from operator import index
from admin.admin import Admin
import pandas as pd
from users import User
from flask import Flask, Blueprint, render_template,request,send_file, redirect, url_for, Response,session
from datetime import date, timedelta, datetime
import time
import numpy as np 
import pytz 
from dateutil.relativedelta import relativedelta 
import json

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication



adminapp = Blueprint('adminapp', __name__, template_folder='templates', static_folder='static', url_prefix='/adminapp') 


@adminapp.route('/growth_report', methods = ['POST', 'GET']) 
def growth_report():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['admin']==1: 
        
            global PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date 

            if request.method=='POST':     

                PM_Start_Date =request.form.get('PM_Start_Date') 
                PM_End_Date =request.form.get('PM_End_Date')    

                CM_Start_Date =request.form.get('CM_Start_Date') 
                CM_End_Date =request.form.get('CM_End_Date')    
                
                if PM_Start_Date < str(date.today()- timedelta(60)):
                    PM_Start_Date=str(date.today()- timedelta(60))
                else:
                    PM_Start_Date=PM_Start_Date 

                if CM_Start_Date < str(date.today()- timedelta(60)):
                    CM_Start_Date=str(date.today()- timedelta(60))
                else:
                    CM_Start_Date=CM_Start_Date  

                df=Admin().growth_report(PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date)  

                if df.shape[0]>0:
                    try:

                        df.to_csv('admin/admin_reports/growth_report.csv', index=False)  
                        zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]}) 
                        zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'
                        
                        return render_template('ad_growth_report.html', 
                                                PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                                CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date, 
                                                d1=df.values, d1col=df.columns.values,  
                                                zz=zz.GOLM[0] 
                                                )     

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('ad_growth_report.html', 
                                                PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                                CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date, 
                                               )     

                return render_template('ad_growth_report.html', 
                                        PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                        CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,   
                                       )       

            else:  
                
                PM_Start_Date= str((date.today()-relativedelta(months=1))-timedelta(7)) 
                PM_End_Date= str(date.today()-relativedelta(months=1))

                CM_Start_Date = str(date.today()-timedelta(7))    
                CM_End_Date = str(date.today())    
 
                df=Admin().growth_report(PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date)  

                if df.shape[0]>0:
                    try:

                        df.to_csv('admin/admin_reports/growth_report.csv', index=False)  
                        zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]}) 
                        zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'
                        
                        return render_template('ad_growth_report.html', 
                                                PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                                CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date, 
                                                d1=df.values, d1col=df.columns.values,  
                                                zz=zz.GOLM[0] 
                                                )     

                    except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError): 
                        return render_template('ad_growth_report.html', 
                                                PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                                CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date, 
                                               )     

                return render_template('ad_growth_report.html', 
                                        PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
                                        CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,   
                                       )       

    return redirect('/') 



@adminapp.route('/growth_report_csv', methods = ['POST', 'GET'])    
def growth_report_csv():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])  
        if permission['admin']==1: 
            return send_file('admin/admin_reports/growth_report.csv',  
                            mimetype='text/csv',
                            as_attachment=True
                            )  

    return redirect('/') 



######################### email ##########################  

@adminapp.route('/send_email', methods = ['POST', 'GET'])  
def send_email():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            def send_email(sender_email, recipient_emails, cc_emails, subject, body, attachment_file_path):
                try:

                    PM_Start=pd.to_datetime(PM_Start_Date).strftime('%d-%b-%Y') 
                    PM_End=pd.to_datetime(PM_End_Date).strftime('%d-%b-%Y') 
                    CM_Start=pd.to_datetime(CM_Start_Date).strftime('%d-%b-%Y') 
                    CM_End=pd.to_datetime(CM_End_Date).strftime('%d-%b-%Y')  

                    # Create the MIME object
                    message = MIMEMultipart()
                    message["From"] = sender_email
                    message["To"] = ", ".join(recipient_emails) 
                    message["Cc"] = ", ".join(cc_emails) if cc_emails else ""
                    message["Subject"] = subject
                    message.attach(MIMEText(body, "plain"))

                    # Add HTML content to the body of the email        
                    df = pd.read_csv(attachment_file_path)  

                    zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]})  
                    zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'

                    html_content = f"Hi,<br>Please find below the Overall Comparison of Shipments between <b>{PM_Start} to {PM_End} vs. {CM_Start} to {CM_End}</b><br><br>{zz.to_html(index=False)}<br><br>"
                    text_content = f"Please find the Attached Growth Analysis Report of Sellers comparing Shipments between <b>{PM_Start} to {PM_End} vs. {CM_Start} to {CM_End}</b><br>" 

                    html_body = f"<html><body>{html_content}{text_content}<br>{df.to_html(index=False)}</body></html><br><br>Thanks & Regards,<br>Data Team<br>"
                    message.attach(MIMEText(html_body, "html")) 

                    # Attach the file
                    with open(attachment_file_path, "rb") as attachment:
                        attachment_part = MIMEApplication(attachment.read())
                        attachment_file_name = "Growth Analysis Report.csv"  # Change to the desired name 
                        attachment_part.add_header("Content-Disposition", f"attachment; filename={attachment_file_name}") 
                        message.attach(attachment_part)

                    # SMTP configuration for Gmail
                    smtp_server = "smtp.gmail.com"
                    smtp_port = 587
                    smtp_username = "data@sourceinfi.com"
                    smtp_password = "Shifto@1234"

                    # Combine recipient_emails and cc_emails into a single list
                    all_recipients = recipient_emails + cc_emails if cc_emails else recipient_emails

                    # Create an SMTP session
                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        server.starttls()  # Enable TLS
                        server.login(smtp_username, smtp_password)  # Login to your Gmail account
                        server.sendmail(sender_email, all_recipients, message.as_string())  # Send the email

                    print("Email with attachment sent successfully to multiple recipients")

                except Exception as e:
                    print(f"Error: {e}")
                    
            # Email configuration
            sender_email = "data@sourceinfi.com" 
            recipient_emails = [
                                # "rohit.chauhan@nimbuspost.com",
                                "yash@whitebrands.com",
                                "p@sourceinfi.com",
                                "anubhav@sourceinfi.com" 
                                ] 
            cc_emails = [
                        "rohit.chauhan@nimbuspost.com"
                        ] 
            subject = "Growth Analysis Report"
            body = ''

            attachment_file_path = "admin/admin_reports/growth_report.csv" 

            send_email(sender_email, recipient_emails, cc_emails, subject, body, attachment_file_path)  

            return 'done' 

    return redirect('/') 



############################ dashboard ############################ 



@adminapp.route('/', methods = ['POST', 'GET']) 
@adminapp.route('/dashboard', methods = ['POST', 'GET'])  
def dashboard():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            # if request.method=='POST':     

            #     Start_Date =request.form.get('Start_Date') 
            #     End_Date =request.form.get('End_Date')    
                
            #     return render_template('dashboard.html', Start_Date=Start_Date, End_Date=End_Date)   

            # else:  

            #     Start_Date = str(date.today()-timedelta(7))    
            #     End_Date = str(date.today())   

            return render_template('ad_dashboard.html', 
                                #    Start_Date=Start_Date, End_Date=End_Date
                                   )       

    return redirect('/') 




@adminapp.route('/top_sellers', methods = ['POST', 'GET'])  
def top_sellers():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().top_sellers(Start_Date, End_Date)  

            data={'Name' : df['Seller_Name'].tolist(),
                  'Value' : df['Ships'].tolist()}  
            
            return json.dumps(data) 

    return redirect('/') 


@adminapp.route('/top_vendors', methods = ['POST', 'GET'])  
def top_vendors():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().top_vendors(Start_Date, End_Date)  

            data={'Name' : df['Vendor_Name'].tolist(), 
                  'Value' : df['Ships'].tolist()}  
            
            return json.dumps(data) 

    return redirect('/') 


@adminapp.route('/top_couriers', methods = ['POST', 'GET'])  
def top_couriers():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().top_couriers(Start_Date, End_Date)  

            data={'Name' : df['Courier_Category'].tolist(), 
                  'Value' : df['Ships'].tolist()}  
            
            return json.dumps(data) 

    return redirect('/') 



@adminapp.route('/top_cities', methods = ['POST', 'GET'])  
def top_cities():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().top_cities(Start_Date, End_Date)   

            data={'Name' : df['Shipping_City'].tolist(), 
                  'Value' : df['Ships'].tolist()}  
            
            return json.dumps(data) 

    return redirect('/') 



@adminapp.route('/order_vs_ships', methods = ['POST', 'GET'])   
def order_vs_ships():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))     
            End_Date = str(date.today())   

            df=Admin().order_vs_ships(Start_Date, End_Date)   

            data={'Name' : df['Month'].tolist(), 
                  'Orders' : df['Orders'].tolist(), 
                  'Ships' : df['Ships'].tolist()}  
            
            return json.dumps(data) 

    return redirect('/') 



@adminapp.route('/top_products', methods = ['POST', 'GET'])  
def top_products():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().top_products(Start_Date, End_Date)   

            data={'Name' : df['Product_Name'].tolist(), 
                  'Value' : df['Ships'].tolist()}   
            
            return json.dumps(data) 

    return redirect('/') 


@adminapp.route('/cod_pre', methods = ['POST', 'GET'])  
def cod_pre():  
    if 'email' in session:
        permission=User().get_user_permissions(session['email']) 
        if permission['admin']==1: 

            Start_Date = str(date.today()-timedelta(7))    
            End_Date = str(date.today())   

            df=Admin().cod_pre(Start_Date, End_Date)   
            
            df= df.rename(columns={'Payment_Mode':'name','Ships':'y'}) 
            data=df.to_dict('records')   
           
            return json.dumps(data) 

    return redirect('/') 


