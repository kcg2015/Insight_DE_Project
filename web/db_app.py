import os
from flask import Flask, make_response
from flask import render_template
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2

app = Flask(__name__)

user = 'data_engineer' #add your username here (same as previous postgreSQL)                      
host = 'localhost'
dbname = 'test_result_db'
db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
con = None
con = psycopg2.connect(database = dbname, user = user, password='kcguan')

@app.route('/')

@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/pipeline')
def display_pipeline():
    return render_template('pipeline.html')

@app.route('/rslt')  
def result_summary():
    cur = con.cursor()
    cur.execute("SELECT * FROM result3;")
    data = cur.fetchall()
    succ_count1 = 0
    total_count1 = 0
    for item in data:
        total_count1 += 1
        if item[12] == True:
            succ_count1 += 1
    succ_rate1 = succ_count1/total_count1
    print(succ_rate1)

    cur = con.cursor()
    cur.execute("SELECT * FROM result4;")
    data = cur.fetchall()
    succ_count2 = 0
    total_count2 = 0
    for item in data:
        total_count2 += 1
        if item[12] == True:
            succ_count2 += 1
    succ_rate2 = succ_count2/total_count2    
    print(succ_rate2)
            
    return render_template('result_summary.html', 
       model1 = { 'name': 'Vesrion 1.0.0' },
       model2 = { 'name': 'Vesrion 1.0.1' },
       model1_acc ={'name': str(succ_rate1)},
       model2_acc ={'name': str(succ_rate2)},)  

@app.route('/db1')
def result_db1():
    cur = con.cursor()
    cur.execute("SELECT * FROM result3;")
    data = cur.fetchall()
    return render_template('template_db_tl.html', data=data)

@app.route('/db2')
def result_db2():
    cur = con.cursor()
    cur.execute("SELECT * FROM result4;")
    data = cur.fetchall()
    return render_template('template_db_tl.html', data=data)

if __name__ == '__main__':
    app.run()