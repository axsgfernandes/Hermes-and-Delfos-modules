from flask import Flask, render_template, request, session, url_for, redirect, flash, send_file, send_from_directory
from flask import jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import re
import os
import csv, json
import pandas as pd
from os import PathLike
from hdfs import InsecureClient
from passlib.hash import sha256_crypt
from werkzeug.utils import secure_filename
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import os.path
import shutil
from json import dump, loads
from os import path

# testing locally
#engine = create_engine("mysql+pymysql://root:1234567@localhost/register")

# testing on portainer
engine = create_engine("mysql+pymysql://python:123456@mysql:3306/register")
#(mysql+pymysql://username:password@localhost/databasename
db = scoped_session(sessionmaker(bind=engine))
app = Flask(__name__)

# HDFS - portainer
client = InsecureClient("http://hdfs-nn:9870", user="hdfs")

# HDFS
#client = InsecureClient("http://localhost:9870")


# register form
@app.route("/register/", methods=["GET", "POST"])
def register():
    msg = ''
    if request.method == "POST" and 'email' in request.form and 'password' in request.form and 'confirm' in request.form:
        email = request.form['email']
        password = request.form["password"]
        confirm = request.form["confirm"]
        secure_password = sha256_crypt.encrypt(str(password))

        user = db.execute("SELECT * FROM users WHERE email = :email", {'email': email}).fetchone()

        if user:
            msg = 'Account already exists!'
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            msg = 'Invalid email address!'
        elif not email or not password:
            msg = 'Please fill out the form!'
        elif password != confirm:
            msg = 'Password does not match'
        else:
            db.execute("INSERT INTO users(email, password) VALUES(:email,:password)",
                       {"email": email, "password": secure_password})
            db.commit()
            msg = 'You have successfully registered!'
            return redirect(url_for('login'))
    elif request.method == 'POST':
        msg = 'Please fill out the form!'
    return render_template('register.html', msg=msg)


# login form
@app.route("/", methods=["GET", "POST"])
def login():
    msg = ''
    if request.method == 'POST' and 'email' in request.form and 'password' in request.form:
        email = request.form["email"]
        password = request.form["password"]

        user = db.execute('SELECT * FROM users WHERE email = :email', {'email': email}).fetchone()
        validatePassword = sha256_crypt.verify(password, user['password'])

        if user and validatePassword:
            session["loggedin"] = True
            session['id'] = user['id']
            session['email'] = user['email']

            return redirect(url_for('home'))
        else:
            msg = 'Incorrect username/password!'
    return render_template('login.html', msg=msg)


# logout
@app.route('/logout')
def logout():
    session.pop('loggedin', None)
    session.pop('id', None)
    session.pop('email', None)
    # Redirect to login page
    return redirect(url_for('login'))

# Home page: modules - HERMES, DELFOS, ULISSES and SIBILA
@app.route('/home/', methods=['GET', 'POST'])
def home():
    if 'loggedin' in session:
        if request.method == 'POST':
            selectedValue = request.form['data']
            return redirect(url_for('click', selectedValue=selectedValue))
        return render_template('home.html', id=session['id'])
    return render_template('login.html')

@app.route('/home/<selectedValue>')
def click(selectedValue):
    if 'loggedin' in session:
        return render_template(selectedValue + ".html", id=session['id'])

#######################################   DELFOS  ########################################################

# Img Delfos to page job 
@app.route('/select_job/', methods=['GET', 'POST'])
def job_page():
    if 'loggedin' in session:
        if request.method == 'POST':
            list_folders = dropdown_job()
            print(list_folders)
            return render_template('job.html', id=session['id'], list_folders=list_folders)
       
# Get job folders by user
def dropdown_job():
    user_id = 'user' + str(session['id'])
    job_path = '/delfos_platform/'+user_id+'/jobs/'
    job_folders = client.list(job_path)
    print(job_folders)
    return job_folders

# Save value of chosen job 
@app.route('/choose/', methods=['GET', 'POST'])
def chosen_job():
   if 'loggedin' in session:
        if request.method == 'POST':
            job_id = request.form.get('list_folders')
            print(request.form.get('list_folders'))
            if job_id is not None:
                return render_template('select_query.html', id=session['id'], job_id=job_id)
        return render_template("job.html", id=session['id'])

# Choose new or previous query
@app.route("/select_query/<job_id>", methods = ['POST', 'GET'])
def choose_query(job_id):
    if 'loggedin' in session:
        if request.method == 'POST':
            selection = request.form.get('data')
            if selection == 'new':
                return render_template('import_query.html', id=session['id'], job_id=job_id, selection=selection)  
            if selection == 'previous':
                if request.method == 'POST':
                    user_id = 'user' + str(session['id'])
                    job_path = '/delfos_platform/'+user_id+'/jobs/'+job_id+'/query_rules/'
                    job_folders = client.list(job_path)
                    list_folders = job_folders
                    print(list_folders)
                    return render_template('previous.html', id=session['id'], list_folders = list_folders, job_id=job_id)
        return render_template("job.html", id=session['id'])
    return render_template('login.html')

# Save value of chosen query
@app.route("/choose_query/<job_id>",  methods=['GET', 'POST'])
def chosen_query(job_id):
    if 'loggedin' in session:
        if request.method == 'POST':
            query_id = request.form.get('list_folders')
            print(request.form.get('list_folders'))
            if query_id is not None:
                content = read_results(job_id, query_id)
                return render_template('query_result.html', id=session['id'], query_id=query_id, column_names=content.columns.values, row_data=list(content.values.tolist()), zip=zip, table_id="table-id")
        return render_template("previous.html", id=session['id'])
    return render_template('login.html')

def read_results(job_id, query_id):
    user_id = 'user' + str(session['id'])
    filename = query_id.replace("rule", "result")
    result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/" + filename
    with client.read(result_path, encoding="utf-8") as reader:
        content=pd.read_csv(reader, sep=";",  encoding= 'unicode_escape')
        content = content.dropna(axis=1, how='all')
        return content

# Upload query rules
@app.route('/home/import_query/<job_id>', methods=['GET', 'POST'])
def upload_query(job_id):
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        print(user_id) 
        if request.method == 'POST':
            select_list = request.form.getlist('parameterName')
            select = ", ".join(select_list)
            files = request.files.getlist("importQuery")
            print(files)
            #Path to folder code Delfos
            basedir = os.path.abspath(os.path.dirname(__file__))
            print(basedir)
            #Verify if exists folder query_rules
            query_rules_path = '/delfos_platform/' + user_id + '/jobs/'+ job_id + '/'
            client.makedirs(query_rules_path)
            folders_hdfs = client.list(query_rules_path)
            print(folders_hdfs)
            #If not exists folder "query_rules"
            if len(folders_hdfs) == 0:
                hdfs_folder = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_rules/"
                for file in files:
                    print(file)
                    if file.filename != '':
                        filename = secure_filename(file.filename)
                        temp_path = os.path.join(basedir, '/tmp/', filename)
                        file.save(temp_path)
                        path_files = os.path.join(hdfs_folder, filename)
                        client.upload(path_files, temp_path)
                        os.remove(temp_path)
            #If exists folder "query_rules"
            else:
                for file in files:
                    client.makedirs('/delfos_platform/' + user_id + '/jobs/' + job_id + '/query_rules/')
                    if file.filename in client.list('/delfos_platform/' + user_id + '/jobs/' + job_id + '/query_rules/'):
                        flash("You must change the name of the file: " + file.filename)
                    else:
                        #Secure a filename before storing it directly on the filesystem
                        filename = secure_filename(file.filename)
                        print(filename)
                        #Path to temporary folder
                        temp_path = os.path.join(basedir, '/tmp/', filename)
                        print(temp_path)
                        #Save file in temp_path
                        file.save(temp_path)
                        hdfs_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_rules/"
                        path_files = os.path.join(hdfs_path, filename)
                        client.upload(path_files, temp_path)
                        os.remove(temp_path)
                        mechanism = query_mechanism(job_id, filename, select)
                        messages_result = session.get('messages_result')
                        if len(messages_result) == 0:
                            content = query_result(job_id)
                        return render_template('query_result.html', id=session['id'], job_id= job_id, column_names=content.columns.values, row_data=list(content.values.tolist()), zip=zip, table_id="table-id")
                    return render_template("import_query.html", id=session['id'], job_id=job_id)
    return render_template('import_query.html', id=session['id'], job_id=job_id)

#   Query Mechanism
def query_mechanism(job_id, filename, select):
    user_id = 'user' + str(session['id'])
  
    warehouse_location = 'hdfs://hdfs-nn:9000/delfos_platform'
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("read csv") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()
        \
    
    spark.sparkContext.setLogLevel('WARN')
    #spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    treated_file =  "/delfos_platform/" + user_id + "/jobs/" + job_id + "/treated_file.csv"
    print(treated_file)
    query_file = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_rules/" + filename
    print(query_file)

    ##### Read treated.file CSV and convert to df

    hdfs_path = "hdfs://hdfs-nn:9000/"+ treated_file
    csv_treated = spark.read.option("header",True).csv(hdfs_path, sep=';')

    ##### Read query_file and convert to df

    hdfs_path = "hdfs://hdfs-nn:9000/"+ query_file
    df_csv = spark.read.option('header','true').csv(hdfs_path, sep=';')

    ###### Uniformization of the column names

    rename_column = ['grouping', 'operator', 'parameter', 'parameter_type', 'condition', 'value']
    counter = {c: -1 for c in df_csv.columns}
    for c in df_csv.columns:
        new_c = c
        counter[c] += 1
        new_c += str(counter[c]) if counter[c] else ""
    df_csv = df_csv.toDF(*rename_column)

    ##### Verify if the condition for a parameter_type are correct

    operators_numbers = df_csv["condition"].isin('==','=','!=','<',">",'<=','>=','between','not between','is null','is not null','in','not in')
    operators_strings = df_csv["condition"].isin('==','=','!=','in','not in','LIKE','like','is null','is not null')
    operators_boolean = df_csv["condition"].isin('==','=')
    df_csv = df_csv.withColumn("message", when( ( (df_csv["parameter_type"].isin(['integer', 'double', 'float', 'decimal']) & (operators_numbers)) |  ((df_csv["parameter_type"] == 'string') & (operators_strings)) |  ((df_csv["parameter_type"] == 'boolean') & (operators_boolean)) ), "Right").otherwise("Wrong Condition"))

    #Mechanism
    
    message_list = [data[0] for data in df_csv.select('message').collect()]
    print(message_list)
    
    messages_result = []

    if 'Wrong Condition' in message_list:
        #print("Yes, 'Wrong Condition' found in message_list : ", message_list)
        delete_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_rules/" + filename
        client.delete(delete_path)
        messages_result.append('Error')
        flash("The input file doesn't match the predefined template")
    
    else:
    
        #For in and not in
        df_csv = df_csv.withColumn('value', F.when((df_csv['condition'] =='in') | (df_csv['condition'] == 'not in'), F.concat(lit("("),col("value"),lit(")"))).otherwise(F.col('value')))

        df_csv = df_csv.withColumn("first", when( ((df_csv["parameter_type"].isin(['string'])) & (df_csv['condition'].isin(['in','not in']))), split(df_csv['value'], ',').getItem(0)).otherwise(""))
        df_csv = df_csv.withColumn("second", when( ((df_csv["parameter_type"].isin(['string'])) & (df_csv['condition'].isin(['in','not in']))), split(df_csv['value'], ' ').getItem(1)).otherwise(""))

        df_csv = df_csv.withColumn('first', F.when(((df_csv['condition'].isin(['in','not in'])) & (df_csv["parameter_type"].isin(['string']))), F.concat(col("first"),lit("'"))).otherwise(F.col('first')))
        df_csv = df_csv.withColumn('second', F.when(((df_csv['condition'].isin(['in','not in'])) & (df_csv["parameter_type"].isin(['string']))), F.concat(lit("'"),col("second"))).otherwise(F.col('second')))

        df_csv = df_csv.withColumn('value', F.when(((df_csv['condition'].isin(['in','not in'])) & (df_csv["parameter_type"].isin(['string']))), F.concat(col("first"),lit(","),col("second"))).otherwise(F.col('value')))
        df_csv = df_csv.drop('first','second')

        ###### If type = String --> add ''     
        df_csv = df_csv.withColumn('value', F.when((df_csv['parameter_type'] =='string') | (df_csv['parameter_type'] == 'boolean'), F.concat(lit("'"),col("value"),lit("'"))).otherwise(F.col('value')))

        #FUll Condition
        df_csv = df_csv.withColumn("full_condition", concat(col("parameter"), lit(" "), col("condition"), lit(" "), col("value")))

        #add inder column
        from pyspark.sql.functions import monotonically_increasing_id 

        df_index = df_csv.select("*").withColumn("id", monotonically_increasing_id())
        df_csv = df_index.select("id","grouping","operator","parameter","full_condition","message") 

        #remove null rows
        df_csv = df_csv.where(df_csv.full_condition != '')

        #remove possible spaces
        df_csv = df_csv.select([trim(col(c)).alias(c) for c in df_csv.columns])

        #Get rules
        rules = ""
        increment = 0
        row_max = df_csv.agg({"id": "max"}).collect()[0]
        print(row_max["max(id)"])

        for row in df_csv.collect():
            if row['id'] == row_max["max(id)"]:
                rules = rules + row['full_condition'] + ")"*increment 
                print(rules)
            else:
                if row['grouping']=='yes':
                    rules = rules + "(" + row['full_condition'] + " " + row['operator'] + " "
                    increment = increment + 1
                    
                elif row['grouping']=='no':
                    rules = rules + row['full_condition'] + ")"*increment + " " + row['operator'] + " "
                    increment=0
            
        csv_treated = csv_treated.createOrReplaceTempView("csv_treated")
    
        #query = "SELECT " + "{}".format(parameter) + " from " + "file" + " WHERE " + "{}".format(full_condition)
        query = "SELECT  "+ select + " FROM " + "csv_treated" + " WHERE " + rules
        print(query)

        sql_statement = spark.sql(query)

        ######### Upload Result to hdfs
        results_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/" 
        client.makedirs(results_path)
        folders_result_hdfs = client.list(results_path)

        if len(folders_result_hdfs) == 0:
            result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/query_result_1.csv" 
                    
            pandasDF = sql_statement.toPandas()
            with client.write(result_path, overwrite=True, encoding = 'utf-8') as writer:
                pandasDF.to_csv(writer, index=False, sep=";", header=True)
        else:
            new_list = []
            for f in folders_result_hdfs:
                f = f.replace(".csv","")
                print(f)
                f = f.split('_')
                print(f)
                new_list.append(int(f[2]))
                print(new_list)                
            new_list = sorted(new_list)                      
            last_folder = new_list[-1]
            new_index = str(last_folder+1)
            result_index = "query_result_"+new_index
            result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+result_index+".csv"
            pandasDF = sql_statement.toPandas()
            with client.write(result_path, overwrite=True, encoding = 'utf-8') as writer:
                pandasDF.to_csv(writer, index=False, sep=";", header=True)

    session['messages_result'] = messages_result
    return spark.stop()

'''
    def result_path(folders_result_hdfs):
        new_list = []
        for f in folders_result_hdfs:
            f = f.replace(".csv","")
            print(f)
            f = f.split('_')
            print(f)
            new_list.append(int(f[2]))
            print(new_list)                
        new_list = sorted(new_list)
                                
        last_folder = new_list[-1]
        new_index = str(last_folder+1)
        result_index = "query_result_"+new_index
        result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+result_index+".csv"
        print(result_path)  

        return result_path
'''
def query_result(job_id):
    user_id = 'user' + str(session['id'])
    if 'loggedin' in session:
    #if request.method == 'POST':
        results_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/" 
        client.makedirs(results_path)
        folders_result_hdfs = client.list(results_path)

        if len(folders_result_hdfs) == 0:
            result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/query_result_1.csv" 
            with client.read(result_path, encoding="utf-8") as reader:
                content=pd.read_csv(reader, sep=";",  encoding= 'unicode_escape')
                content = content.dropna(axis=1, how='all')
                return content
        else:
            new_list = []
            for f in folders_result_hdfs:
                f = f.replace(".csv","")
                print(f)
                f = f.split('_')
                print(f)
                new_list.append(int(f[2]))
                print(new_list)                
            new_list = sorted(new_list)

            last_folder = new_list[-1]
            result_index = "query_result_"+ str(last_folder)                                  
            result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+result_index+".csv"
            print(result_path)  
            with client.read(result_path, encoding="utf-8") as reader:
                content=pd.read_csv(reader, sep=";" ,  encoding= 'unicode_escape')
                content = content.dropna(axis=1, how='all')
                return content

#   Page Query Builder
@app.route("/query_builder/<job_id>",  methods=['GET', 'POST'])
def query_builder(job_id):
    if 'loggedin' in session:
        return render_template('query_builder.html', id=session['id'], job_id=job_id)

#   Query Builder Mechanism
@app.route('/get_SQL/<job_id>', methods=['GET', 'POST'])
def get_SQL():
    if 'loggedin' in session:
        #rules = request.form.get('result')
        get_rules = request.json
        rules = get_rules['sql']
        #print(rules, 'ok')
        #session["rules"] = rules
        #return session["rules"]
        return redirect(url_for('/results_builder/<job_id>', rules=rules))

@app.route('/results_builder/<job_id>', methods=['GET', 'POST'])       
def results_builder(job_id, rules):
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        #rules = session.get("rules", None)
        #print(rules)
        mechanism = query_builder_mechanism(job_id, rules, user_id)
        content = query_result(job_id)
        return render_template('query_result.html',id=session['id'], job_id=job_id, column_names=content.columns.values, row_data=list(content.values.tolist()), zip=zip, table_id="table-id")
        #return render_template('test.html', rules = rules)

#   Query Builder Mechanism
def query_builder_mechanism(job_id, rules, user_id):

    warehouse_location = 'hdfs://hdfs-nn:9000/delfos_platform'
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("read csv") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    #spark.sparkContext.setLogLevel('WARN')
    #spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    treated_file= "/delfos_platform/" + user_id + "/jobs/" + job_id + "/treated_file.csv"

    ##### Read treated.file CSV and convert to df

    hdfs_path = "hdfs://hdfs-nn:9000/"+ treated_file
    csv_treated = spark.read.option("header",True).csv(hdfs_path, sep=';')

    ##### Query 
    csv_treated.createOrReplaceTempView("csv_treated")

    if rules is not None:
        query = "SELECT *" + " from " + "csv_treated" + " WHERE " + rules
        sql_statement = spark.sql(query)
    
        ######### Upload Result to hdfs
        results_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/" 
        client.makedirs(results_path)
        folders_result_hdfs = client.list(results_path)

        if len(folders_result_hdfs) == 0:
            result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/query_result_1.csv"     
            pandasDF = sql_statement.toPandas()
            with client.write(result_path, overwrite=True, encoding = 'utf-8') as writer:
                pandasDF.to_csv(writer, index=False, sep=";", header=True)
        else:
            results_path = result_paths(folders_result_hdfs, job_id)
            pandasDF = sql_statement.toPandas()
            with client.write(results_path, overwrite=True, encoding = 'utf-8') as writer:
                pandasDF.to_csv(writer, index=False, sep=";", header=True)

        
    return spark.stop()

def result_paths(folders_result_hdfs, job_id):
    user_id = 'user' + str(session['id'])
    new_list = []
    for f in folders_result_hdfs:
        f = f.replace(".csv","")
        print(f)
        f = f.split('_')
        print(f)
        new_list.append(int(f[2]))
        print(new_list)                
    new_list = sorted(new_list)
                            
    last_folder = new_list[-1]
    new_index = str(last_folder+1)
    result_index = "query_result_"+new_index
    result_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+result_index+".csv"
    print(result_path)  

    return result_path

# Export results 
@app.route('/import_query/<job_id>', methods=['POST', 'GET'])
@app.route('/results/<job_id>', methods=['POST', 'GET'])
def results(job_id):
    if 'loggedin' in session:
        if request.method == 'POST':
            selection = request.form['option']
            if selection == 'csv':
                #if exists query_result path --> Remove 
                if os.path.exists('query_result'):
                    os.remove('query_result')
                user_id = 'user' + str(session['id'])
                results_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"
                client.makedirs(results_path)
                folders_result_hdfs = client.list(results_path)
                last_element = folders_result_hdfs[-1]
                hdfs_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+ last_element
                temp_path =  os.path.abspath('query_result')
                client.download(hdfs_path, temp_path)
                if os.path.exists(temp_path):
                    os.remove('query_result')
                client.download(hdfs_path, temp_path, overwrite=True)
                return send_file('query_result', mimetype='text/csv', download_name='query_result.csv', as_attachment=True)
            #return render_template('query_result.html', id=session['id'], column_names=content.columns.values, row_data=list(content.values.tolist()), zip=zip, table_id="table-id")
            else:
                if selection == 'json':
                    #if exists query_result path --> Remove 
                    if os.path.exists('query_result'):
                        os.remove('query_result')
                    user_id = 'user' + str(session['id'])
                    results_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/" 
                    client.makedirs(results_path)
                    folders_result_hdfs = client.list(results_path)
                    new_list = []
                    for f in folders_result_hdfs:
                        f = f.replace(".csv","")
                        new_list.append(f)                  
                    new_list = sorted(new_list)
                    #print(new_list)                   
                    last_folder = new_list[-1]
                    #print(last_folder)
                    hdfs_path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+last_folder+".csv"
                    temp_path =  os.path.abspath('query_result')
                    client.download(hdfs_path, temp_path)
                    # Convert csv to json and store in HDFS
                    json_schema_str = to_json()
                    with client.write(results_path + last_folder +'.json', overwrite=True, encoding='latin-1') as json_writer:
                        dump(json_schema_str, json_writer, indent=2)

                    path = "/delfos_platform/" + user_id + "/jobs/" + job_id + "/query_results/"+last_folder+".json"   
                    if os.path.exists(temp_path):
                        os.remove('query_result')
                    client.download(path, temp_path, overwrite=True)
                    # remove file.json of HDFS
                    client.delete(path)
                    return send_file('query_result', mimetype='application/json', download_name='query_result.json', as_attachment=True)
            return render_template('query_result.html', id=session['id'], column_names=content.columns.values, row_data=list(content.values.tolist()), zip=zip, table_id="table-id")
    return render_template('login.html')

def to_json():
    #convert df to pandas df
    df = pd.read_csv('query_result', sep=";")

    #create the json schema with the structure defined by the pros research center
    df = df.groupby(["variant_id"])

    result = []
    for d in df:
        dataframe = d[1]
        result_dict = dict()

        for column in dataframe:
            if column in (["variant_name", "variant_id", "chromosome", "variant_type", "description", "polyphen_prediction", "sift_prediction"]):
                name_column = dataframe[column].drop_duplicates()
                for i in name_column.items():
                    result_dict[column] = i[1]

            #genes
            if column == "gene":
                genes_df = dataframe.groupby("gene")
                #verify if the group of dataframe is empty
                if genes_df.size().empty:
                    result_dict["genes"] = None
                else:
                    genes = []
                    for gene in genes_df:
                        df_g = gene[1]
                        df_genes = df_g[['gene']]
                        df_genes = df_genes.drop_duplicates()
                        #print(df_genes)

                        for row, data in df_genes.iterrows():
                            genes.append(data[0])

                        if not genes:
                            result_dict["genes"] = None
                        else:
                            result_dict["genes"] = genes
            #hgvs
            if column == "hgvs":
                hgvs_df = dataframe.groupby("hgvs")
                #verify if the group of dataframe is empty
                if hgvs_df.size().empty:
                    result_dict["hgvs"] = None
                else:
                    hgvs_list = []
                    for hgvs in hgvs_df:
                        df_hgvs = hgvs[1]
                        df_hgvs = df_hgvs[['hgvs']]
                        df_hgvs = df_hgvs.drop_duplicates()
                        #print(df_hgvs)

                        for row, data in df_hgvs.iterrows():
                            hgvs_list.append(data[0])

                        if not hgvs_list:
                            result_dict["hgvs"] = None
                        else:
                            result_dict["hgvs"] = hgvs_list

            #assemblies
            if column == "assembly":
                assembly_df = dataframe.groupby("assembly")
                #verify if the group of dataframe is empty
                if assembly_df.size().empty:
                    result_dict["assembly"] = None
                else:
                    assemblies = []
                    for assembly in assembly_df:
                        df_ass = assembly[1]
                        df_assembly = df_ass[['assembly', 'assembly_date', 'start', 'end', 'ref', 'alt', 'risk_allele']]
                        df_assembly = df_assembly.drop_duplicates()

                        for row, data in df_assembly.iterrows():
                            assembly_dict = dict()
                            assembly_dict["assembly"] = data[0]
                            assembly_dict["date"] = data[1]
                            assembly_dict["start"] = data[2]
                            assembly_dict["end"] = data[3]
                            assembly_dict["ref"] = data[4]
                            assembly_dict["alt"] = data[5]
                            assembly_dict["risk_allele"] = data[6]

                            #if all values of bibliography_dict are None, this will return True
                            check = all(x is None for x in assembly_dict.values())

                            if check:
                                assemblies = []
                            else:
                                assemblies.append(assembly_dict)

                        if not assemblies:
                            result_dict["assembly"] = None
                        else:
                            result_dict["assembly"] = assemblies
                            #databanks
            if column == "name":
                databank_df = dataframe.groupby("name")
                #verify if the group of dataframe is empty
                if databank_df.size().empty:
                    result_dict["databanks"] = None
                else:
                    databanks = []
                    for databank in databank_df:
                        df_db = databank[1]
                        df_databank = df_db[['name', 'url', 'version', 'databanks_variant_id', 'clinvar_accession']]
                        df_databank = df_databank.drop_duplicates()

                        for row, data in df_databank.iterrows():
                            databank_dict = dict()
                            databank_dict["name"] = data[0]
                            databank_dict["url"] = data[1]
                            databank_dict["version"] = data[2]
                            databank_dict["variant_id"] = data[3]
                            databank_dict["clinvar_accession"] = data[4]

                            #if all values of bibliography_dict are None, this will return True
                            check = all(x is None for x in databank_dict.values())

                            if check:
                                databanks = []
                            else:
                                databanks.append(databank_dict)

                        if not databanks:
                            result_dict["databanks"] = None
                        else:
                            result_dict["databanks"] = databanks
            #phenotypes
            if column == "phenotype":
                phenotype_df = dataframe.groupby("phenotype")
                #verify if the group of dataframe is empty
                if phenotype_df.size().empty:
                    result_dict["phenotypes"] = None
                else:
                    phenotypes = []
                    for phenotype in phenotype_df:
                        phenotype_dict = {}
                        phenotype_dict["phenotype"] = phenotype[0]

                        clinical_actionability = phenotype[1]["clinical_actionability"].drop_duplicates()
                        for i in clinical_actionability.items():
                            phenotype_dict["clinical_actionability"] = i[1]

                        classification = phenotype[1]["classification"].drop_duplicates()
                        for i in classification.items():
                            phenotype_dict["classification"] = i[1]

                        interpretations = []
                        bibliographies = []

                        #interpretations
                        df_int = phenotype[1]
                        df_interpretation = df_int[['clinical_significance', 'method', 'assertion_criteria', 'level_certainty', 'date', 'author', 'origin']]
                        df_interpretation = df_interpretation.drop_duplicates()

                        for row, data in df_interpretation.iterrows():
                            interpretation_dict = dict()
                            interpretation_dict["clinical_significance"] = data[0]
                            interpretation_dict["method"] = data[1]
                            interpretation_dict["assertion_criteria"] = data[2]
                            interpretation_dict["level_certainty"] = data[3]
                            interpretation_dict["date"] = data[4]
                            interpretation_dict["author"] = data[5]
                            interpretation_dict["origin"] = data[6]

                            #if all values of bibliography_dict are None, this will return True
                            check = all(x is None for x in interpretation_dict.values())

                            if check:
                                interpretations = []
                            else:
                                interpretations.append(interpretation_dict)

                        if not interpretations:
                            phenotype_dict["interpretation"] = None
                        else:
                            phenotype_dict["interpretation"] = interpretations

                            #bibliographies
                        df_bibl = phenotype[1]
                        df_bibliography = df_bibl[['title', 'year', 'authors', 'pmid', 'is_gwas']]
                        df_bibliography = df_bibliography.drop_duplicates()

                        for row, data in df_bibliography.iterrows():
                            bibliography_dict = dict()
                            bibliography_dict["title"] = data[0]
                            bibliography_dict["year"] = data[1]
                            bibliography_dict["authors"] = data[2]
                            bibliography_dict["pmid"] = data[3]
                            bibliography_dict["is_gwas"] = data[4]

                            #if all values of bibliography_dict are None, this will return True
                            check = all(x is None for x in bibliography_dict.values())

                            if check:
                                bibliographies = []
                            else:
                                bibliographies.append(bibliography_dict)

                        if not bibliographies:
                            phenotype_dict["bibliography"] = None
                        else:
                            phenotype_dict["bibliography"] = bibliographies

                        phenotypes.append(phenotype_dict)
                    result_dict["phenotypes"] = phenotypes
        result.append(result_dict)

    return result


if __name__ == "__main__":
    app.secret_key = "1234567delfosplatform"
    #app.run(host='localhost', debug=True)
    app.run(host='0.0.0.0', debug=True)






