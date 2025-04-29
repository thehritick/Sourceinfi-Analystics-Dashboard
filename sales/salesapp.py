from operator import index
from sales.sales import Sales
import pandas as pd
from users import User
from flask import (
    Flask,
    Blueprint,
    render_template,
    request,
    send_file,
    redirect,
    url_for,
    Response,
    session,
)
from datetime import date, timedelta, datetime
import time
import numpy as np
import pytz
from dateutil.relativedelta import relativedelta
import json

# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart
# from email.mime.application import MIMEApplication


salesapp = Blueprint(
    "salesapp",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/salesapp",
)


@salesapp.route("/pendency_tat", methods=["POST", "GET"])
def pendency_tat():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Seller_ID = request.form.get("Seller_ID")
                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                if Seller_ID == "":
                    df = Sales().pendency_tat(Start_Date, End_Date)
                else:
                    df = Sales().pendency_tat_seller(Seller_ID, Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        df.to_csv(
                            "sales/sales_reports/pendency_tat_raw_data.csv", index=False
                        )

                        try:
                            pick_vs_approval = (
                                df.pivot_table(
                                    index="Diff_Pickup_vs_Approval",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                                .rename(columns={"AWB": "Shipments"})
                            )
                            pick_vs_approval["Ship_%"] = (
                                round(
                                    (
                                        pick_vs_approval["Shipments"]
                                        / pick_vs_approval["Shipments"].sum()
                                    )
                                    * 200,
                                    2,
                                ).astype(str)
                                + "%"
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            pick_vs_approval = pd.DataFrame([])

                        try:
                            pick_vs_order = (
                                df.pivot_table(
                                    index="Diff_Pickup_vs_Order",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                                .rename(columns={"AWB": "Shipments"})
                            )
                            pick_vs_order["Ship_%"] = (
                                round(
                                    (
                                        pick_vs_order["Shipments"]
                                        / pick_vs_order["Shipments"].sum()
                                    )
                                    * 200,
                                    2,
                                ).astype(str)
                                + "%"
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            pick_vs_order = pd.DataFrame([])

                        try:
                            vendor_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Vendor_Name",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            vendor_datewise = pd.DataFrame([])

                        try:
                            seller_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Seller_Name",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            seller_datewise = pd.DataFrame([])

                        try:
                            product_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index=["Variant_SKU", "Product_Name"],
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            product_datewise = pd.DataFrame([])

                        try:
                            courier_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Courier_Category",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            courier_datewise = pd.DataFrame([])

                        return render_template(
                            "pendency_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pick_vs_approval.values,
                            d1col=pick_vs_approval.columns.values,
                            d2=pick_vs_order.values,
                            d2col=pick_vs_order.columns.values,
                            d3=vendor_datewise.values,
                            d3col=vendor_datewise.columns.values,
                            d4=seller_datewise.values,
                            d4col=seller_datewise.columns.values,
                            d5=product_datewise.values,
                            d5col=product_datewise.columns.values,
                            d6=courier_datewise.values,
                            d6col=courier_datewise.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "pendency_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "pendency_tat.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Seller_ID = ""
                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())

                if Seller_ID == "":
                    df = Sales().pendency_tat(Start_Date, End_Date)
                else:
                    df = Sales().pendency_tat_seller(Seller_ID, Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        df.to_csv(
                            "sales/sales_reports/pendency_tat_raw_data.csv", index=False
                        )

                        try:
                            pick_vs_approval = (
                                df.pivot_table(
                                    index="Diff_Pickup_vs_Approval",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                                .rename(columns={"AWB": "Shipments"})
                            )
                            pick_vs_approval["Ship_%"] = (
                                round(
                                    (
                                        pick_vs_approval["Shipments"]
                                        / pick_vs_approval["Shipments"].sum()
                                    )
                                    * 200,
                                    2,
                                ).astype(str)
                                + "%"
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            pick_vs_approval = pd.DataFrame([])

                        try:
                            pick_vs_order = (
                                df.pivot_table(
                                    index="Diff_Pickup_vs_Order",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                                .rename(columns={"AWB": "Shipments"})
                            )
                            pick_vs_order["Ship_%"] = (
                                round(
                                    (
                                        pick_vs_order["Shipments"]
                                        / pick_vs_order["Shipments"].sum()
                                    )
                                    * 200,
                                    2,
                                ).astype(str)
                                + "%"
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            pick_vs_order = pd.DataFrame([])

                        try:
                            vendor_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Vendor_Name",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            vendor_datewise = pd.DataFrame([])

                        try:
                            seller_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Seller_Name",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            seller_datewise = pd.DataFrame([])

                        try:
                            product_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index=["Variant_SKU", "Product_Name"],
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            product_datewise = pd.DataFrame([])

                        try:
                            courier_datewise = (
                                df[df.Ship_Status == "pending pickup"]
                                .pivot_table(
                                    index="Courier_Category",
                                    columns="Order_Date",
                                    values="AWB",
                                    aggfunc="count",
                                    margins=True,
                                    margins_name="Grand_Total",
                                    fill_value="",
                                )
                                .reset_index()
                                .sort_values(by="Grand_Total", ascending=False)
                            )

                        except (
                            ValueError,
                            TypeError,
                            KeyError,
                            NameError,
                            IndexError,
                            UnicodeError,
                            AttributeError,
                            UnboundLocalError,
                        ):
                            courier_datewise = pd.DataFrame([])

                        return render_template(
                            "pendency_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pick_vs_approval.values,
                            d1col=pick_vs_approval.columns.values,
                            d2=pick_vs_order.values,
                            d2col=pick_vs_order.columns.values,
                            d3=vendor_datewise.values,
                            d3col=vendor_datewise.columns.values,
                            d4=seller_datewise.values,
                            d4col=seller_datewise.columns.values,
                            d5=product_datewise.values,
                            d5col=product_datewise.columns.values,
                            d6=courier_datewise.values,
                            d6col=courier_datewise.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "pendency_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "pendency_tat.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/pendency_tat_raw_data", methods=["POST", "GET"])
def pendency_tat_raw_data():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/pendency_tat_raw_data.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


# @salesapp.route('/growth_report', methods = ['POST', 'GET'])
# def growth_report():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             global PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date

#             if request.method=='POST':

#                 PM_Start_Date =request.form.get('PM_Start_Date')
#                 PM_End_Date =request.form.get('PM_End_Date')

#                 CM_Start_Date =request.form.get('CM_Start_Date')
#                 CM_End_Date =request.form.get('CM_End_Date')

#                 if PM_Start_Date < str(date.today()- timedelta(60)):
#                     PM_Start_Date=str(date.today()- timedelta(60))
#                 else:
#                     PM_Start_Date=PM_Start_Date

#                 if CM_Start_Date < str(date.today()- timedelta(60)):
#                     CM_Start_Date=str(date.today()- timedelta(60))
#                 else:
#                     CM_Start_Date=CM_Start_Date

#                 df=Sales().growth_report(PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date)

#                 if df.shape[0]>0:
#                     try:

#                         df.to_csv('sales/sales_reports/growth_report.csv', index=False)
#                         zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]})
#                         zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'

#                         return render_template('growth_report.html',
#                                                 PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                                 CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                                 d1=df.values, d1col=df.columns.values,
#                                                 zz=zz.GOLM[0]
#                                                 )

#                     except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError):
#                         return render_template('growth_report.html',
#                                                 PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                                 CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                                )

#                 return render_template('growth_report.html',
#                                         PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                         CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                        )

#             else:

#                 PM_Start_Date= str((date.today()-relativedelta(months=1))-timedelta(7))
#                 PM_End_Date= str(date.today()-relativedelta(months=1))

#                 CM_Start_Date = str(date.today()-timedelta(7))
#                 CM_End_Date = str(date.today())

#                 df=Sales().growth_report(PM_Start_Date, PM_End_Date, CM_Start_Date, CM_End_Date)

#                 if df.shape[0]>0:
#                     try:

#                         df.to_csv('sales/sales_reports/growth_report.csv', index=False)
#                         zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]})
#                         zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'

#                         return render_template('growth_report.html',
#                                                 PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                                 CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                                 d1=df.values, d1col=df.columns.values,
#                                                 zz=zz.GOLM[0]
#                                                 )

#                     except (ValueError,TypeError,KeyError,NameError,IndexError,UnicodeError,AttributeError,UnboundLocalError):
#                         return render_template('growth_report.html',
#                                                 PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                                 CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                                )

#                 return render_template('growth_report.html',
#                                         PM_Start_Date=PM_Start_Date, PM_End_Date=PM_End_Date,
#                                         CM_Start_Date=CM_Start_Date, CM_End_Date=CM_End_Date,
#                                        )

#     return redirect('/')


# @salesapp.route('/growth_report_csv', methods = ['POST', 'GET'])
# def growth_report_csv():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:
#             return send_file('sales/sales_reports/growth_report.csv',
#                             mimetype='text/csv',
#                             as_attachment=True
#                             )

#     return redirect('/')


# ######################### email ##########################

# @salesapp.route('/send_email', methods = ['POST', 'GET'])
# def send_email():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             def send_email(sender_email, recipient_emails, cc_emails, subject, body, attachment_file_path):
#                 try:

#                     PM_Start=pd.to_datetime(PM_Start_Date).strftime('%d-%b-%Y')
#                     PM_End=pd.to_datetime(PM_End_Date).strftime('%d-%b-%Y')
#                     CM_Start=pd.to_datetime(CM_Start_Date).strftime('%d-%b-%Y')
#                     CM_End=pd.to_datetime(CM_End_Date).strftime('%d-%b-%Y')

#                     # Create the MIME object
#                     message = MIMEMultipart()
#                     message["From"] = sender_email
#                     message["To"] = ", ".join(recipient_emails)
#                     message["Cc"] = ", ".join(cc_emails) if cc_emails else ""
#                     message["Subject"] = subject
#                     message.attach(MIMEText(body, "plain"))

#                     # Add HTML content to the body of the email
#                     df = pd.read_csv(attachment_file_path)

#                     zz=pd.DataFrame({'PM_Ships_Till_Date':[df.PM_Ships_Till_Date.sum()], 'CM_Ships_Till_Date':[df.CM_Ships_Till_Date.sum()]})
#                     zz['GOLM']=(((zz.CM_Ships_Till_Date)/zz.PM_Ships_Till_Date)*100).replace([np.nan,np.inf],0).astype(int).astype(str)+'%'

#                     html_content = f"Hi,<br>Please find below the Overall Comparison of Shipments between <b>{PM_Start} to {PM_End} vs. {CM_Start} to {CM_End}</b><br><br>{zz.to_html(index=False)}<br><br>"
#                     text_content = f"Please find the Attached Growth Analysis Report of Sellers comparing Shipments between <b>{PM_Start} to {PM_End} vs. {CM_Start} to {CM_End}</b><br>"

#                     html_body = f"<html><body>{html_content}{text_content}<br>{df.to_html(index=False)}</body></html><br><br>Thanks & Regards,<br>Data Team<br>"
#                     message.attach(MIMEText(html_body, "html"))

#                     # Attach the file
#                     with open(attachment_file_path, "rb") as attachment:
#                         attachment_part = MIMEApplication(attachment.read())
#                         attachment_file_name = "Growth Analysis Report.csv"  # Change to the desired name
#                         attachment_part.add_header("Content-Disposition", f"attachment; filename={attachment_file_name}")
#                         message.attach(attachment_part)

#                     # SMTP configuration for Gmail
#                     smtp_server = "smtp.gmail.com"
#                     smtp_port = 587
#                     smtp_username = "data@sourceinfi.com"
#                     smtp_password = "Shifto@1234"

#                     # Combine recipient_emails and cc_emails into a single list
#                     all_recipients = recipient_emails + cc_emails if cc_emails else recipient_emails

#                     # Create an SMTP session
#                     with smtplib.SMTP(smtp_server, smtp_port) as server:
#                         server.starttls()  # Enable TLS
#                         server.login(smtp_username, smtp_password)  # Login to your Gmail account
#                         server.sendmail(sender_email, all_recipients, message.as_string())  # Send the email

#                     print("Email with attachment sent successfully to multiple recipients")

#                 except Exception as e:
#                     print(f"Error: {e}")

#             # Email configuration
#             sender_email = "data@sourceinfi.com"
#             recipient_emails = [
#                                 # "rohit.chauhan@nimbuspost.com",
#                                 "yash@whitebrands.com",
#                                 "p@sourceinfi.com",
#                                 "anubhav@sourceinfi.com"
#                                 ]
#             cc_emails = [
#                         "rohit.chauhan@nimbuspost.com"
#                         ]
#             subject = "Growth Analysis Report"
#             body = ''

#             attachment_file_path = "sales/sales_reports/growth_report.csv"

#             send_email(sender_email, recipient_emails, cc_emails, subject, body, attachment_file_path)

#             return 'done'

#     return redirect('/')


@salesapp.route("/fad_report", methods=["POST", "GET"])
def fad_report():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Seller_ID = request.form.get("Seller_ID")
                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                if Seller_ID == "":
                    df = Sales().fad_report(Start_Date, End_Date)
                else:
                    df = Sales().fad_report_seller(Seller_ID, Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        total = df.pivot_table(
                            index=["Seller_ID", "Product_Name", "Courier_Category"],
                            values="AWB",
                            aggfunc="count",
                            fill_value=0,
                            margins=True,
                            margins_name="Grand_Total",
                        ).reset_index()
                        total.rename(columns={"AWB": "Grand_Total"}, inplace=True)

                        if "FAD" in df.NDR_Attempts.values:
                            FAD = (
                                df[df.NDR_Attempts == "FAD"]
                                .pivot_table(
                                    index=[
                                        "Seller_ID",
                                        "Product_Name",
                                        "Courier_Category",
                                    ],
                                    columns="Ship_Status",
                                    values="AWB",
                                    aggfunc="count",
                                    fill_value=0,
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                            )
                            FAD.rename(columns={"Grand_Total": "FAD"}, inplace=True)

                        else:
                            FAD = pd.DataFrame(
                                columns=[
                                    "Seller_ID",
                                    "Product_Name",
                                    "Courier_Category",
                                ]
                            )
                            FAD["Delivered"] = 0
                            FAD["FAD"] = 0

                        final = total.merge(
                            FAD,
                            on=["Seller_ID", "Product_Name", "Courier_Category"],
                            how="left",
                        ).fillna(0)

                        final["Delivered_%"] = (
                            (final.Delivered / final.Grand_Total) * 100
                        ).astype(int).astype(str) + "%"
                        final["FAD_%"] = ((final.FAD / final.Grand_Total) * 100).astype(
                            int
                        ).astype(str) + "%"

                        if "RTO" in final.columns:
                            final["RTO_%"] = (
                                (final.RTO / final.Grand_Total) * 100
                            ).astype(int).astype(str) + "%"
                        else:
                            final["RTO"] = 0
                            final["RTO_%"] = "0%"

                        final = (
                            final.set_index(
                                [
                                    "Seller_ID",
                                    "Product_Name",
                                    "Courier_Category",
                                    "Grand_Total",
                                    "FAD",
                                    "FAD_%",
                                ]
                            )
                            .sort_index(ascending=True, axis=1)
                            .reset_index()
                        )
                        final.drop(columns=["FAD", "FAD_%"], inplace=True)

                        final.to_csv("sales/sales_reports/fad_report.csv", index=False)

                        return render_template(
                            "fad_report.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=final.values,
                            d1col=final.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "fad_report.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "fad_report.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())

                df = Sales().fad_report(Start_Date, End_Date)
                print(df.shape)

                if df.shape[0] > 0:
                    try:

                        total = df.pivot_table(
                            index=["Seller_ID", "Product_Name", "Courier_Category"],
                            values="AWB",
                            aggfunc="count",
                            fill_value=0,
                            margins=True,
                            margins_name="Grand_Total",
                        ).reset_index()
                        total.rename(columns={"AWB": "Grand_Total"}, inplace=True)

                        if "FAD" in df.NDR_Attempts.values:
                            FAD = (
                                df[df.NDR_Attempts == "FAD"]
                                .pivot_table(
                                    index=[
                                        "Seller_ID",
                                        "Product_Name",
                                        "Courier_Category",
                                    ],
                                    columns="Ship_Status",
                                    values="AWB",
                                    aggfunc="count",
                                    fill_value=0,
                                    margins=True,
                                    margins_name="Grand_Total",
                                )
                                .reset_index()
                            )
                            FAD.rename(columns={"Grand_Total": "FAD"}, inplace=True)

                        else:
                            FAD = pd.DataFrame(
                                columns=[
                                    "Seller_ID",
                                    "Product_Name",
                                    "Courier_Category",
                                ]
                            )
                            FAD["Delivered"] = 0
                            FAD["FAD"] = 0

                        final = total.merge(
                            FAD,
                            on=["Seller_ID", "Product_Name", "Courier_Category"],
                            how="left",
                        ).fillna(0)

                        final["Delivered_%"] = (
                            (final.Delivered / final.Grand_Total) * 100
                        ).astype(int).astype(str) + "%"
                        final["FAD_%"] = ((final.FAD / final.Grand_Total) * 100).astype(
                            int
                        ).astype(str) + "%"

                        if "RTO" in final.columns:
                            final["RTO_%"] = (
                                (final.RTO / final.Grand_Total) * 100
                            ).astype(int).astype(str) + "%"
                        else:
                            final["RTO"] = 0
                            final["RTO_%"] = "0%"

                        final = (
                            final.set_index(
                                [
                                    "Seller_ID",
                                    "Product_Name",
                                    "Courier_Category",
                                    "Grand_Total",
                                    "FAD",
                                    "FAD_%",
                                ]
                            )
                            .sort_index(ascending=True, axis=1)
                            .reset_index()
                        )
                        final.drop(columns=["FAD", "FAD_%"], inplace=True)

                        final.to_csv("sales/sales_reports/fad_report.csv", index=False)

                        return render_template(
                            "fad_report.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=final.values,
                            d1col=final.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "fad_report.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "fad_report.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/fad_report_csv", methods=["POST", "GET"])
def fad_report_csv():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/fad_report.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/avg_margin_prod", methods=["POST", "GET"])
def avg_margin_prod():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                df = Sales().avg_margin_prod(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        product_margin = (
                            df.pivot_table(
                                index=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                                values=["AWB", "Product_Margin"],
                                aggfunc={"AWB": "count", "Product_Margin": "mean"},
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                            .sort_values(by="AWB", ascending=False)
                        )

                        product_margin.rename(
                            columns={
                                "AWB": "Ships",
                                "Product_Margin": "Avg_Product_Margin",
                            },
                            inplace=True,
                        )

                        Delivered = (
                            df[df.Ship_Status == "Delivered"]
                            .pivot_table(
                                index=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                                values="AWB",
                                aggfunc="count",
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )
                        Delivered.rename(columns={"AWB": "Delivered"}, inplace=True)

                        product_margin = product_margin.merge(
                            Delivered,
                            on=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                            how="left",
                        ).fillna(0)

                        product_margin["Delivered_%"] = (
                            round(
                                (product_margin["Delivered"] / product_margin["Ships"])
                                * 100,
                                2,
                            ).astype(str)
                            + "%"
                        )

                        product_margin.insert(
                            6,
                            "Avg_Product_Margin",
                            product_margin.pop("Avg_Product_Margin"),
                        )

                        product_margin["Avg_Product_Margin"] = product_margin[
                            "Avg_Product_Margin"
                        ].astype(int)
                        product_margin["Delivered"] = product_margin[
                            "Delivered"
                        ].astype(int)

                        product_margin.to_csv(
                            "sales/sales_reports/avg_margin_prod.csv", index=False
                        )

                        return render_template(
                            "avg_margin_prod.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=product_margin.values,
                            d1col=product_margin.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "avg_margin_prod.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "avg_margin_prod.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())

                df = Sales().avg_margin_prod(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        product_margin = (
                            df.pivot_table(
                                index=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                                values=["AWB", "Product_Margin"],
                                aggfunc={"AWB": "count", "Product_Margin": "mean"},
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                            .sort_values(by="AWB", ascending=False)
                        )

                        product_margin.rename(
                            columns={
                                "AWB": "Ships",
                                "Product_Margin": "Avg_Product_Margin",
                            },
                            inplace=True,
                        )

                        Delivered = (
                            df[df.Ship_Status == "Delivered"]
                            .pivot_table(
                                index=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                                values="AWB",
                                aggfunc="count",
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )
                        Delivered.rename(columns={"AWB": "Delivered"}, inplace=True)

                        product_margin = product_margin.merge(
                            Delivered,
                            on=["Variant_SKU", "Product_Name", "Pricing_Plan"],
                            how="left",
                        ).fillna(0)

                        product_margin["Delivered_%"] = (
                            round(
                                (product_margin["Delivered"] / product_margin["Ships"])
                                * 100,
                                2,
                            ).astype(str)
                            + "%"
                        )

                        product_margin.insert(
                            6,
                            "Avg_Product_Margin",
                            product_margin.pop("Avg_Product_Margin"),
                        )

                        product_margin["Avg_Product_Margin"] = product_margin[
                            "Avg_Product_Margin"
                        ].astype(int)
                        product_margin["Delivered"] = product_margin[
                            "Delivered"
                        ].astype(int)

                        product_margin.to_csv(
                            "sales/sales_reports/avg_margin_prod.csv", index=False
                        )

                        return render_template(
                            "avg_margin_prod.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=product_margin.values,
                            d1col=product_margin.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "avg_margin_prod.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "avg_margin_prod.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/avg_margin_prod_csv", methods=["POST", "GET"])
def avg_margin_prod_csv():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/avg_margin_prod.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/", methods=["POST", "GET"])
@salesapp.route("/seller_perf", methods=["POST", "GET"])
def seller_perf():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                df = Sales().seller_perf(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        seller_courier_status = (
                            df[
                                df.Ship_Status.isin(
                                    [
                                        "Delivered",
                                        "Exception",
                                        "In Transit",
                                        "Out For Delivery",
                                        "RTO",
                                    ]
                                )
                            ]
                            .pivot_table(
                                index=["Seller_Name", "Courier_Category"],
                                columns=["Ship_Status"],
                                values="AWB",
                                aggfunc="sum",
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )

                        seller_courier_status["Delivered_%"] = (
                            (
                                seller_courier_status["Delivered"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["Exception_%"] = (
                            (
                                seller_courier_status["Exception"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["In_Transit_%"] = (
                            (
                                seller_courier_status["In Transit"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["Out_For_Delivery_%"] = (
                            (
                                seller_courier_status["Out For Delivery"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["RTO_%"] = (
                            (
                                seller_courier_status["RTO"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"

                        seller_courier_status

                        seller_status = (
                            df[
                                df.Ship_Status.isin(
                                    [
                                        "Delivered",
                                        "Exception",
                                        "In Transit",
                                        "Out For Delivery",
                                        "RTO",
                                    ]
                                )
                            ]
                            .pivot_table(
                                index=["Seller_Name"],
                                columns=["Ship_Status"],
                                values="AWB",
                                aggfunc="sum",
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )

                        seller_status["Delivered_%"] = (
                            (seller_status["Delivered"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["Exception_%"] = (
                            (seller_status["Exception"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["In_Transit_%"] = (
                            (seller_status["In Transit"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["Out_For_Delivery_%"] = (
                            (
                                seller_status["Out For Delivery"]
                                / seller_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["RTO_%"] = (
                            (seller_status["RTO"] / seller_status["Grand_Total"]) * 100
                        ).astype(int).astype(str) + "%"

                        seller_status

                        seller_courier_merge = pd.concat(
                            [seller_status, seller_courier_status]
                        )
                        seller_courier_merge = seller_courier_merge[
                            seller_courier_merge.Seller_Name != "Grand_Total"
                        ]
                        seller_courier_merge

                        seller_courier_merge = seller_courier_merge.sort_index(
                            axis=1, ascending=True
                        )
                        seller_courier_merge = seller_courier_merge.set_index(
                            ["Seller_Name", "Courier_Category", "Grand_Total"]
                        ).reset_index()
                        seller_courier_merge = seller_courier_merge.sort_values(
                            by=["Seller_Name", "Courier_Category"],
                            ascending=[True, True],
                        )

                        seller_courier_merge["Courier_Category"].fillna(
                            "Grand_Total", inplace=True
                        )

                        seller_courier_merge.to_csv(
                            "sales/sales_reports/seller_courier_merge.csv", index=False
                        )

                        return render_template(
                            "seller_perf.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=seller_courier_merge.values,
                            d1col=seller_courier_merge.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "seller_perf.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "seller_perf.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())

                df = Sales().seller_perf(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        seller_courier_status = (
                            df[
                                df.Ship_Status.isin(
                                    [
                                        "Delivered",
                                        "Exception",
                                        "In Transit",
                                        "Out For Delivery",
                                        "RTO",
                                    ]
                                )
                            ]
                            .pivot_table(
                                index=["Seller_Name", "Courier_Category"],
                                columns=["Ship_Status"],
                                values="AWB",
                                aggfunc="sum",
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )

                        seller_courier_status["Delivered_%"] = (
                            (
                                seller_courier_status["Delivered"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["Exception_%"] = (
                            (
                                seller_courier_status["Exception"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["In_Transit_%"] = (
                            (
                                seller_courier_status["In Transit"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["Out_For_Delivery_%"] = (
                            (
                                seller_courier_status["Out For Delivery"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_courier_status["RTO_%"] = (
                            (
                                seller_courier_status["RTO"]
                                / seller_courier_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"

                        seller_courier_status

                        seller_status = (
                            df[
                                df.Ship_Status.isin(
                                    [
                                        "Delivered",
                                        "Exception",
                                        "In Transit",
                                        "Out For Delivery",
                                        "RTO",
                                    ]
                                )
                            ]
                            .pivot_table(
                                index=["Seller_Name"],
                                columns=["Ship_Status"],
                                values="AWB",
                                aggfunc="sum",
                                fill_value=0,
                                margins=True,
                                margins_name="Grand_Total",
                            )
                            .reset_index()
                        )

                        seller_status["Delivered_%"] = (
                            (seller_status["Delivered"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["Exception_%"] = (
                            (seller_status["Exception"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["In_Transit_%"] = (
                            (seller_status["In Transit"] / seller_status["Grand_Total"])
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["Out_For_Delivery_%"] = (
                            (
                                seller_status["Out For Delivery"]
                                / seller_status["Grand_Total"]
                            )
                            * 100
                        ).astype(int).astype(str) + "%"
                        seller_status["RTO_%"] = (
                            (seller_status["RTO"] / seller_status["Grand_Total"]) * 100
                        ).astype(int).astype(str) + "%"

                        seller_status

                        seller_courier_merge = pd.concat(
                            [seller_status, seller_courier_status]
                        )
                        seller_courier_merge = seller_courier_merge[
                            seller_courier_merge.Seller_Name != "Grand_Total"
                        ]
                        seller_courier_merge

                        seller_courier_merge = seller_courier_merge.sort_index(
                            axis=1, ascending=True
                        )
                        seller_courier_merge = seller_courier_merge.set_index(
                            ["Seller_Name", "Courier_Category", "Grand_Total"]
                        ).reset_index()
                        seller_courier_merge = seller_courier_merge.sort_values(
                            by=["Seller_Name", "Courier_Category"],
                            ascending=[True, True],
                        )

                        seller_courier_merge["Courier_Category"].fillna(
                            "Grand_Total", inplace=True
                        )
                        seller_courier_merge

                        seller_courier_merge.to_csv(
                            "sales/sales_reports/seller_courier_merge.csv", index=False
                        )

                        return render_template(
                            "seller_perf.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=seller_courier_merge.values,
                            d1col=seller_courier_merge.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "seller_perf.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "seller_perf.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/seller_courier_merge_csv", methods=["POST", "GET"])
def seller_courier_merge_csv():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/seller_courier_merge.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/referral", methods=["POST", "GET"])
def referral():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")
                try:
                    Referral_Bonus = int(request.form.get("Referral_Bonus"))
                except ValueError:
                    Referral_Bonus = 1

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                df = Sales().referral(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        df["Total_Referral_Amount"] = (
                            df.Delivered_Volume * Referral_Bonus
                        ).astype(int)

                        df.to_csv(
                            "sales/sales_reports/referral_report.csv", index=False
                        )

                        return render_template(
                            "referral.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Referral_Bonus=Referral_Bonus,
                            d1=df.values,
                            d1col=df.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "referral.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "referral.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())
                Referral_Bonus = 1

                df = Sales().referral(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        df["Total_Referral_Amount"] = (
                            df.Delivered_Volume * Referral_Bonus
                        ).astype(int)

                        df.to_csv(
                            "sales/sales_reports/referral_report.csv", index=False
                        )

                        return render_template(
                            "referral.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Referral_Bonus=Referral_Bonus,
                            d1=df.values,
                            d1col=df.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "referral.html", Start_Date=Start_Date, End_Date=End_Date
                        )

                return render_template(
                    "referral.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/referral_csv", methods=["POST", "GET"])
def referral_csv():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/referral_report.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/kam_performance", methods=["POST", "GET"])
def kam_performance():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:

            if request.method == "POST":

                Start_Date = request.form.get("Start_Date")
                End_Date = request.form.get("End_Date")

                if Start_Date < str(date.today() - timedelta(60)):
                    Start_Date = str(date.today() - timedelta(60))
                else:
                    Start_Date = Start_Date

                df = Sales().kam_performance(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        ss = df[df.Orders != 0]

                        CM = df[df.Onboard_Date >= date.today().replace(day=1)]
                        CM = (
                            CM.groupby("Account_Manager")
                            .agg({"Seller_ID": "count"})
                            .reset_index()
                        )
                        CM.rename(columns={"Seller_ID": "Seller_Count"}, inplace=True)

                        CM_A = ss[ss.Onboard_Date >= date.today().replace(day=1)]
                        CM_A = (
                            CM_A.groupby("Account_Manager")
                            .agg({"Seller_ID": "count", "Orders": "sum"})
                            .reset_index()
                        )
                        CM_A.rename(
                            columns={"Seller_ID": "Active_Sellers"}, inplace=True
                        )
                        CM = CM.merge(CM_A, on="Account_Manager", how="left").fillna(0)

                        PM = df[df.Onboard_Date < date.today().replace(day=1)]
                        PM = (
                            PM.groupby("Account_Manager")
                            .agg({"Seller_ID": "count"})
                            .reset_index()
                        )
                        PM.rename(columns={"Seller_ID": "Seller_Count"}, inplace=True)

                        PM_A = ss[ss.Onboard_Date < date.today().replace(day=1)]
                        PM_A = (
                            PM_A.groupby("Account_Manager")
                            .agg({"Seller_ID": "count", "Orders": "sum"})
                            .reset_index()
                        )
                        PM_A.rename(
                            columns={"Seller_ID": "Active_Sellers"}, inplace=True
                        )
                        PM = PM.merge(PM_A, on="Account_Manager", how="left").fillna(0)

                        CM.to_csv(
                            "sales/sales_reports/kam_performance_new_volume.csv",
                            index=False,
                        )
                        PM.to_csv(
                            "sales/sales_reports/kam_performance_existing_volume.csv",
                            index=False,
                        )

                        return render_template(
                            "kam_performance.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=CM.values,
                            d1col=CM.columns.values,
                            d2=PM.values,
                            d2col=PM.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "kam_performance.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "kam_performance.html", Start_Date=Start_Date, End_Date=End_Date
                )

            else:

                Start_Date = str(date.today() - timedelta(7))
                End_Date = str(date.today())

                df = Sales().kam_performance(Start_Date, End_Date)

                if df.shape[0] > 0:
                    try:

                        ss = df[df.Orders != 0]

                        CM = df[df.Onboard_Date >= date.today().replace(day=1)]
                        CM = (
                            CM.groupby("Account_Manager")
                            .agg({"Seller_ID": "count"})
                            .reset_index()
                        )
                        CM.rename(columns={"Seller_ID": "Seller_Count"}, inplace=True)

                        CM_A = ss[ss.Onboard_Date >= date.today().replace(day=1)]
                        CM_A = (
                            CM_A.groupby("Account_Manager")
                            .agg({"Seller_ID": "count", "Orders": "sum"})
                            .reset_index()
                        )
                        CM_A.rename(
                            columns={"Seller_ID": "Active_Sellers"}, inplace=True
                        )
                        CM = CM.merge(CM_A, on="Account_Manager", how="left").fillna(0)

                        PM = df[df.Onboard_Date < date.today().replace(day=1)]
                        PM = (
                            PM.groupby("Account_Manager")
                            .agg({"Seller_ID": "count"})
                            .reset_index()
                        )
                        PM.rename(columns={"Seller_ID": "Seller_Count"}, inplace=True)

                        PM_A = ss[ss.Onboard_Date < date.today().replace(day=1)]
                        PM_A = (
                            PM_A.groupby("Account_Manager")
                            .agg({"Seller_ID": "count", "Orders": "sum"})
                            .reset_index()
                        )
                        PM_A.rename(
                            columns={"Seller_ID": "Active_Sellers"}, inplace=True
                        )
                        PM = PM.merge(PM_A, on="Account_Manager", how="left").fillna(0)

                        CM.to_csv(
                            "sales/sales_reports/kam_performance_new_volume.csv",
                            index=False,
                        )
                        PM.to_csv(
                            "sales/sales_reports/kam_performance_existing_volume.csv",
                            index=False,
                        )

                        return render_template(
                            "kam_performance.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            d1=CM.values,
                            d1col=CM.columns.values,
                            d2=PM.values,
                            d2col=PM.columns.values,
                        )

                    except (
                        ValueError,
                        TypeError,
                        KeyError,
                        NameError,
                        IndexError,
                        UnicodeError,
                        AttributeError,
                        UnboundLocalError,
                    ):
                        return render_template(
                            "kam_performance.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                        )

                return render_template(
                    "kam_performance.html", Start_Date=Start_Date, End_Date=End_Date
                )

    return redirect("/")


@salesapp.route("/kam_performance_new_volume", methods=["POST", "GET"])
def kam_performance_new_volume():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/kam_performance_new_volume.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/kam_performance_existing_volume", methods=["POST", "GET"])
def kam_performance_existing_volume():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/kam_performance_existing_volume.csv",
                mimetype="text/csv",
                as_attachment=True,
            )

    return redirect("/")


@salesapp.route("/jit_tat", methods=["POST", "GET"])
def jit_tat():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission.get("sales", 0) == 1:
            sales_instance = Sales()

            if request.method == "POST":
                Seller_ID = request.form.get("Seller_ID", "")
                Start_Date = request.form.get("Start_Date", "")
                End_Date = request.form.get("End_Date", "")

                # Adjust Start_Date to be within a 120-day range
                if Start_Date < str(date.today() - timedelta(days=120)):
                    Start_Date = str(date.today() - timedelta(days=120))

                # Fetch the appropriate raw data from the database
                if Seller_ID:
                    df = sales_instance.jit_tat(Seller_ID, Start_Date, End_Date)
                else:
                    df = sales_instance.jit_tat(Start_Date, End_Date)

                # Debugging
                print("DataFrame Shape:", df.shape)
                print(df.head())
                print(df.columns)

                if not df.empty:
                    try:
                        # Create pivot tables based on different criteria
                        pivot_table1 = df.pivot_table(
                            index="App_to_PO_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        )
                        # .reset_index()  # Reset index to convert it back to a DataFrame

                        pivot_table2 = df.pivot_table(
                            index="PO_to_Ship_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        pivot_table3 = df.pivot_table(
                            index="App_to_ship_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        # Reset index to flatten the pivot tables for easier rendering
                        # pivot_table1.reset_index(inplace=True)
                        # pivot_table2.reset_index(inplace=True)
                        # pivot_table3.reset_index(inplace=True)
                        print(pivot_table2)
                        # # Save the pivot tables to CSV for debugging or further use
                        # pivot_table1.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table1.csv", index=False
                        # )
                        # pivot_table2.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table2.csv", index=False
                        # )
                        # pivot_table3.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table3.csv", index=False
                        # )

                        # Render the pivot tables as tables in the template
                        return render_template(
                            "jit_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pivot_table1.values.tolist(),
                            d1col=pivot_table1.columns.tolist(),
                            d2=pivot_table2.values.tolist(),
                            d2col=pivot_table2.columns.tolist(),
                            d3=pivot_table3.values.tolist(),
                            d3col=pivot_table3.columns.tolist(),
                        )
                    except Exception as e:
                        print(
                            f"Error generating pivot table or rendering template: {str(e)}"
                        )
                        return render_template(
                            "jit_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            error_message="Error generating report.",
                        )
                else:
                    return render_template(
                        "jit_tat.html",
                        Start_Date=Start_Date,
                        End_Date=End_Date,
                        error_message="No data available for the selected date range and criteria.",
                    )
            else:
                # Default behavior on GET request
                Seller_ID = ""
                Start_Date = str(date.today() - timedelta(days=120))
                End_Date = str(date.today())

                # Fetch the raw data for the default view
                df = sales_instance.jit_tat(Start_Date, End_Date)

                # Debugging
                print("DataFrame Shape:", df.shape)
                print(df.head())
                print(df.columns)

                if not df.empty:
                    try:
                        # Create pivot tables based on different criteria
                        pivot_table1 = df.pivot_table(
                            index="App_to_PO_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        pivot_table2 = df.pivot_table(
                            index="PO_to_Ship_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        pivot_table3 = df.pivot_table(
                            index="App_to_ship_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        # Reset index to flatten the pivot tables for easier rendering
                        # pivot_table1.reset_index(inplace=True)
                        # pivot_table2.reset_index(inplace=True)
                        # pivot_table3.reset_index(inplace=True)

                        # # Save the pivot tables to CSV for debugging or further use
                        # pivot_table1.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table1.csv", index=False
                        # )
                        # pivot_table2.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table2.csv", index=False
                        # )
                        # pivot_table3.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table3.csv", index=False
                        # )

                        # Render the pivot tables as tables in the template
                        return render_template(
                            "jit_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pivot_table1.values.tolist(),
                            d1col=pivot_table1.columns.tolist(),
                            d2=pivot_table2.values.tolist(),
                            d2col=pivot_table2.columns.tolist(),
                            d3=pivot_table3.values.tolist(),
                            d3col=pivot_table3.columns.tolist(),
                        )
                    except Exception as e:
                        print(
                            f"Error generating pivot table or rendering template: {str(e)}"
                        )
                        return render_template(
                            "jit_tat.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            error_message="Error generating report.",
                        )
                else:
                    return render_template(
                        "jit_tat.html",
                        Start_Date=Start_Date,
                        End_Date=End_Date,
                        error_message="No data available for the selected date range and criteria.",
                    )
    return redirect("/")


@salesapp.route("/jit_tat_raw_data", methods=["POST", "GET"])
def jit_tat_raw_data():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission["sales"] == 1:
            return send_file(
                "sales/sales_reports/jit_tat_raw_data.csv",
                mimetype="text/csv",
                as_attachment=True,
            )
    return redirect("/")


@salesapp.route("/user_remarks", methods=["POST", "GET"])
def user_remarks():
    if "email" in session:
        permission = User().get_user_permissions(session["email"])
        if permission.get("sales", 0) == 1:
            sales_instance = Sales()

            if request.method == "POST":
                Seller_ID = request.form.get("Seller_ID", "")
                Start_Date = request.form.get("Start_Date", "")
                End_Date = request.form.get("End_Date", "")

                # Adjust Start_Date to be within a 120-day range
                if Start_Date < str(date.today() - timedelta(days=120)):
                    Start_Date = str(date.today() - timedelta(days=120))

                # Fetch the appropriate raw data from the database
                if Seller_ID:
                    df = sales_instance.user_remarks(Seller_ID, Start_Date, End_Date)
                else:
                    df = sales_instance.user_remarks(Start_Date, End_Date)

                # Debugging
                print("DataFrame Shape:", df.shape)
                print(df.head())
                print(df.columns)

                if not df.empty:
                    try:
                        # Create pivot tables based on different criteria
                        pivot_table1 = df.pivot_table(
                            index="App_to_PO_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        )
                        # .reset_index()  # Reset index to convert it back to a DataFrame

                        # Reset index to flatten the pivot tables for easier rendering
                        # pivot_table1.reset_index(inplace=True)
                        # pivot_table2.reset_index(inplace=True)
                        # pivot_table3.reset_index(inplace=True)
                        print(pivot_table1)
                        # # Save the pivot tables to CSV for debugging or further use
                        # pivot_table1.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table1.csv", index=False
                        # )
                        # pivot_table2.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table2.csv", index=False
                        # )
                        # pivot_table3.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table3.csv", index=False
                        # )

                        # Render the pivot tables as tables in the template
                        return render_template(
                            "user_remarks.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pivot_table1.values.tolist(),
                            d1col=pivot_table1.columns.tolist(),
                        )
                    except Exception as e:
                        print(
                            f"Error generating pivot table or rendering template: {str(e)}"
                        )
                        return render_template(
                            "user_remarks.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            error_message="Error generating report.",
                        )
                else:
                    return render_template(
                        "user_remarks.html",
                        Start_Date=Start_Date,
                        End_Date=End_Date,
                        error_message="No data available for the selected date range and criteria.",
                    )
            else:
                # Default behavior on GET request
                Seller_ID = ""
                Start_Date = str(date.today() - timedelta(days=120))
                End_Date = str(date.today())

                # Fetch the raw data for the default view
                df = sales_instance.user_remarks(Start_Date, End_Date)

                # Debugging
                print("DataFrame Shape:", df.shape)
                print(df.head())
                print(df.columns)

                if not df.empty:
                    try:
                        # Create pivot tables based on different criteria
                        pivot_table1 = df.pivot_table(
                            index="App_to_PO_TAT",
                            values="Order_Item_ID",
                            aggfunc="count",  # Count of Purchase Orders                                },
                            fill_value=0,  # Fill missing values with 0
                            margins=True,  # Include a Grand Total row/column
                            margins_name="Grand_Total",  # Name the total as "Grand_Total"
                        ).reset_index()  # Reset index to convert it back to a DataFrame

                        # Reset index to flatten the pivot tables for easier rendering
                        # pivot_table1.reset_index(inplace=True)
                        # pivot_table2.reset_index(inplace=True)
                        # pivot_table3.reset_index(inplace=True)

                        # # Save the pivot tables to CSV for debugging or further use
                        # pivot_table1.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table1.csv", index=False
                        # )
                        # pivot_table2.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table2.csv", index=False
                        # )
                        # pivot_table3.to_csv(
                        #     "sales/sales_reports/jit_tat_pivot_table3.csv", index=False
                        # )

                        # Render the pivot tables as tables in the template
                        return render_template(
                            "user_remarks.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            Seller_ID=Seller_ID,
                            d1=pivot_table1.values.tolist(),
                            d1col=pivot_table1.columns.tolist(),
                        )
                    except Exception as e:
                        print(
                            f"Error generating pivot table or rendering template: {str(e)}"
                        )
                        return render_template(
                            "user_remarks.html",
                            Start_Date=Start_Date,
                            End_Date=End_Date,
                            error_message="Error generating report.",
                        )
                else:
                    return render_template(
                        "user_remarks.html",
                        Start_Date=Start_Date,
                        End_Date=End_Date,
                        error_message="No data available for the selected date range and criteria.",
                    )
    return redirect("/")


# ############################ dashboard ############################


# @salesapp.route('/dashboard', methods = ['POST', 'GET'])
# def dashboard():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             # if request.method=='POST':

#             #     Start_Date =request.form.get('Start_Date')
#             #     End_Date =request.form.get('End_Date')

#             #     return render_template('dashboard.html', Start_Date=Start_Date, End_Date=End_Date)

#             # else:

#             #     Start_Date = str(date.today()-timedelta(7))
#             #     End_Date = str(date.today())

#             return render_template('dashboard.html',
#                                 #    Start_Date=Start_Date, End_Date=End_Date
#                                    )

#     return redirect('/')


# @salesapp.route('/top_sellers', methods = ['POST', 'GET'])
# def top_sellers():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().top_sellers(Start_Date, End_Date)

#             data={'Name' : df['Seller_Name'].tolist(),
#                   'Value' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/top_vendors', methods = ['POST', 'GET'])
# def top_vendors():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().top_vendors(Start_Date, End_Date)

#             data={'Name' : df['Vendor_Name'].tolist(),
#                   'Value' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/top_couriers', methods = ['POST', 'GET'])
# def top_couriers():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().top_couriers(Start_Date, End_Date)

#             data={'Name' : df['Courier_Category'].tolist(),
#                   'Value' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/top_cities', methods = ['POST', 'GET'])
# def top_cities():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().top_cities(Start_Date, End_Date)

#             data={'Name' : df['Shipping_City'].tolist(),
#                   'Value' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/order_vs_ships', methods = ['POST', 'GET'])
# def order_vs_ships():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().order_vs_ships(Start_Date, End_Date)

#             data={'Name' : df['Month'].tolist(),
#                   'Orders' : df['Orders'].tolist(),
#                   'Ships' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/top_products', methods = ['POST', 'GET'])
# def top_products():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().top_products(Start_Date, End_Date)

#             data={'Name' : df['Product_Name'].tolist(),
#                   'Value' : df['Ships'].tolist()}

#             return json.dumps(data)

#     return redirect('/')


# @salesapp.route('/cod_pre', methods = ['POST', 'GET'])
# def cod_pre():
#     if 'email' in session:
#         permission=User().get_user_permissions(session['email'])
#         if permission['sales']==1:

#             Start_Date = str(date.today()-timedelta(7))
#             End_Date = str(date.today())

#             df=Sales().cod_pre(Start_Date, End_Date)

#             df= df.rename(columns={'Payment_Mode':'name','Ships':'y'})
#             data=df.to_dict('records')

#             return json.dumps(data)

#     return redirect('/')
