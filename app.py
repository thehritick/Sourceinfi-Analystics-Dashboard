import pandas as pd
from flask import Flask, render_template, url_for, request, session, redirect, flash
from datetime import date, timedelta, datetime 
from users import User 
from sales import salesapp
from operations import operationsapp
from finance import financeapp
from vendor import vendorapp 
from logistics import logisticsapp 
from admin import adminapp 


### SourceInfi Project ###   

app = Flask(__name__) 

app.register_blueprint(salesapp.salesapp) 
app.register_blueprint(operationsapp.operationsapp) 
app.register_blueprint(financeapp.financeapp) 
app.register_blueprint(vendorapp.vendorapp)  
app.register_blueprint(logisticsapp.logisticsapp)   
app.register_blueprint(adminapp.adminapp)   


@app.route('/')
@app.route('/home') 
def index():
    if 'email' in session:
        permission=User().get_user_permissions(session['email'])
        if session['type']=='admin':
            cursor=User().get_all_users()
            df =  pd.DataFrame(list(cursor)) 

            return render_template('admin.html',u=session['email'],d=df.values,p=permission)         
        return render_template('index.html',u=session['email'],p=permission,tp=session['type']) 
    return render_template('login.html')
    

@app.route('/admin/delete_employee/<email>', methods=['POST', 'GET']) 
def delete_employee(email): 
    if 'email' in session:
        if session['type']=='admin':
            cursor=User().find_One(email)
            if cursor:
                User().delete_One(email) 
                return redirect(url_for('index')) 

            return render_template('login.html')         

    return render_template('login.html')         


@app.route('/admin/editemployee/<email>', methods=['POST', 'GET'])
def edit_employee(email):
    if 'email' in session:
        if session['type']=='admin':
            cursor=User().find_One(email)
            df =  pd.DataFrame.from_records([cursor])
            if request.method == 'POST':
                cursor['name']=request.form['name']
                cursor['type']=request.form['type']
                permissions={
                            'sales':int(request.form['sales']),
                            'operations':int(request.form['operations']), 
                            'finance':int(request.form['finance']),
                            'vendor':int(request.form['vendor']), 
                            'logistics':int(request.form['logistics']), 
                            'admin':int(request.form['admin'])       
                            }  
                cursor['permissions']=permissions
                df =  pd.DataFrame.from_records([cursor]) 
                User().update_user(cursor)
                # return render_template('employee.html',d=df.values) 
                return redirect(url_for('index')) 

            else:
                return render_template('employee.html',d=df.values)
        return render_template('index.html',u=session['email'])        
    return render_template('login.html')        
    

@app.route('/login', methods=['POST'])
def login():
    login_user = User().find_One(request.form['email'])
    
    if login_user is not None:
        if User().check_password(login_user,request.form['pass']):
            session['email'] = request.form['email']
            session['type'] = login_user['type']
            session.permanent = True
            app.permanent_session_lifetime = timedelta(minutes=300)
            return redirect(url_for('index'))
        flash('Invalid password')
        return redirect(url_for('index'))

    flash('Invalid username')    
    return redirect(url_for('index'))



@app.route('/register', methods=['POST', 'GET'])
def register():
    if request.method == 'POST':
        existing_user = User().find_One(request.form['email'])
        existing_user=None
        if existing_user is None:
            new_user=User()
            new_user.user_details['email']=request.form['email']
            new_user.user_details['name']=request.form['name']
            new_user.user_details['password']=request.form['pass']
            
            new_user.register_user()
            
            session['email'] = new_user.user_details['email']
            session['type'] = 'employee'
            session.permanent = True
            app.permanent_session_lifetime = timedelta(minutes=30)
            return redirect(url_for('index'))
        
        flash ('That username already exists!')
        return redirect(url_for('register'))

    return render_template('register.html')


@app.route('/logout')
def logout():
    if 'email' in session:
        session.clear()
        return redirect(url_for('index'))
    return redirect(url_for('index'))



if __name__ == '__main__':
    app.secret_key = 'mysecret'
    app.run(debug=True, host='0.0.0.0')  



