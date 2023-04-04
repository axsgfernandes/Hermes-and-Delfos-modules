from flask import Flask, render_template, request, session, url_for, redirect, flash, send_file, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import re
import os
import shutil
import zipfile
import datetime
import pytz
import xml.etree.ElementTree as ET
from Bio import Entrez
import csv
from json import dump
import pandas as pd
from hdfs import InsecureClient
from passlib.hash import sha256_crypt
from werkzeug.utils import secure_filename

from pyspark.sql import SparkSession
from pyspark.sql import Row
from functools import reduce
from itertools import zip_longest
import pyspark.sql.functions as F

# testing locally
#engine = create_engine("mysql+pymysql://root:1234567@localhost/register")


# testing on portainer
engine = create_engine("mysql+pymysql://python:123456@mysql:3306/register")
# (mysql+pymysql://username:password@localhost/databasename

db = scoped_session(sessionmaker(bind=engine))
app = Flask(__name__)

# HDFS - portainer
client = InsecureClient("http://hdfs-nn:9870", user="hdfs")


# HDFS - local
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

# Upload raw data, mapping rules and transformation rules - database tab
@app.route('/home/local/', methods=['POST'])
def upload_local_files():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        if request.method == 'POST':
            uploadData = request.files['uploadData']
            mappingRules = request.files['mappingRules']
            transformationRules = request.files['transformationRules']
            databaseName = request.form["databaseName"]

            basedir = os.path.abspath(os.path.dirname(__file__))
            # create a csv file in a temporary location with only the database name (value entered by the user)
            tmp = os.path.join(basedir, '/tmp/databaseName.csv')
            with open(tmp, 'w', newline='') as csvfile:
                fieldname = ['databaseName']
                writer = csv.DictWriter(csvfile, fieldnames=fieldname)
                writer.writeheader()
                writer.writerow({'databaseName': str(databaseName)})
            raw_data_path = '/delfos_platform/' + user_id + '/raw_data/'
            client.makedirs(raw_data_path)
            folders_hdfs = client.list(raw_data_path)

            if len(folders_hdfs) == 0 and "quality_rules" not in folders_hdfs:
                quality_rules_path = '/delfos_platform/' + user_id + '/raw_data/quality_rules'
                client.makedirs(quality_rules_path)
                hdfs_path = "/delfos_platform/" + user_id + "/raw_data/raw_data_1/"
                upload(uploadData, mappingRules, transformationRules, basedir, tmp, hdfs_path)
            elif (len(folders_hdfs) != 0) and ("raw_data_1" in folders_hdfs) and ("quality_rules" not in folders_hdfs):
                quality_rules_path = '/delfos_platform/' + user_id + '/raw_data/quality_rules'
                client.makedirs(quality_rules_path)
                folders_hdfs = client.list(raw_data_path)
                hdfs_path = sum_folder_raw_data(folders_hdfs, user_id)
                upload(uploadData, mappingRules, transformationRules, basedir, tmp, hdfs_path)
            else:
                hdfs_path = sum_folder_raw_data(folders_hdfs, user_id)
                upload(uploadData, mappingRules, transformationRules, basedir, tmp, hdfs_path)
        return render_template('local.html', msg='ERROR')
    return render_template('login.html')


def sum_folder_raw_data(folders_hdfs, user_id):
    folders_hdfs.remove('quality_rules')
    new_list = []

    if len(folders_hdfs) != 0:
        for f in folders_hdfs:
            f = f.replace("raw_data_", "")
            new_list.append(int(f))
        new_list = sorted(new_list)

        last_folder = new_list[-1]
        new_index = str(last_folder + 1)
        raw_data_index = "raw_data_" + new_index
        hdfs_path = "/delfos_platform/" + user_id + "/raw_data/" + raw_data_index + "/"
    else:
        hdfs_path = "/delfos_platform/" + user_id + "/raw_data/raw_data_1/"
    return hdfs_path


def upload(uploadData, mappingRules, trasformationRules, basedir, tmp, hdfs_path):
    files = []
    files.append(uploadData)
    files.append(mappingRules)
    files.append(trasformationRules)

    path_databasename = hdfs_path + 'databaseName.csv'
    client.upload(path_databasename, tmp)
    os.remove(tmp)

    for file in files:
        print(file)
        if file.filename != '':
            filename = secure_filename(file.filename)
            temp_path = os.path.join(basedir, '/tmp/', filename)
            file.save(temp_path)
            path_files = os.path.join(hdfs_path, filename)
            client.upload(path_files, temp_path)
            os.remove(temp_path)
    return render_template('local.html')


# Upload local files - quality rules tab
@app.route('/home/local/quality_rule', methods=["GET", "POST"])
def quality_rule_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        if request.method == 'POST':
            files = request.files.getlist("qualityRules")
            for file in files:
                if file.filename in client.list('/delfos_platform/'+user_id+'/raw_data/quality_rules/'):
                    flash("You must change the name of the file: " + file.filename)
                else:
                    filename = secure_filename(file.filename)
                    basedir = os.path.abspath(os.path.dirname(__file__))
                    temp_path = os.path.join(basedir, '/tmp/', filename)
                    file.save(temp_path)
                    hdfs_path = "/delfos_platform/" + user_id + "/raw_data/quality_rules/"
                    path_files = os.path.join(hdfs_path, filename)
                    client.upload(path_files, temp_path)
                    os.remove(temp_path)
        return render_template("quality_rule.html", id=session['id'])
    return render_template('login.html')


# search tab
@app.route('/home/local/search', methods=["GET", "POST"])
def search_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        names_and_paths = table_data(user_id)
        names = names_and_paths[0]
        paths = names_and_paths[1]

        quality_rules = table_quality_rules(user_id)
        names_list = quality_rules[0]
        paths_list = quality_rules[1]

        if request.method == 'POST':
            hdfs_path = "hdfs://hdfs-nn:9000"
            parameter = request.form.get('parameterName')
            value = request.form["value"]
            checkboxData = request.form.getlist("checkboxData")
            checkboxQR = request.form.getlist("checkboxQR")

            i = 0

            #create a csv file with all information about the search - databases selected, quality rules select, parameter and value
            create_info_search(checkboxQR, checkboxData, parameter, value)

            for cd in checkboxData:
                i = i + 1
                if cd.endswith(".xml"):
                    read_xml(user_id, cd, i)
                    transformation_rules_path = "/".join(list(cd.split('/')[0:-1]))+'/transformation_rules.csv'
                    mapped_file_path = "/".join(list(cd.split('/')[0:-3]))+"/staging_area/mapped_files/"
                    #function for transforming data
                    transformation_result = transformed_data(user_id,hdfs_path,transformation_rules_path,mapped_file_path,i)
                    messages_transformation = transformation_result[0]
                else:
                    read_csv(user_id, hdfs_path, cd, i)
                    transformation_rules_path = "/".join(list(cd.split('/')[0:-1]))+'/transformation_rules.csv'
                    mapped_file_path = "/".join(list(cd.split('/')[0:-3]))+"/staging_area/mapped_files/"
                    #function for transforming data
                    transformation_result = transformed_data(user_id,hdfs_path,transformation_rules_path,mapped_file_path,i)
                    messages_transformation = transformation_result[0]

            create_file_result = create_treated_file(user_id, hdfs_path, parameter, value, checkboxQR)
            messages_create_file = create_file_result[0]

            messages_result = messages_transformation + messages_create_file

            client.delete("/delfos_platform/"+user_id+"/staging_area/", recursive=True)

            if len(messages_result) == 0:
                return redirect(url_for('results_tab'))
        return render_template("search.html", id=session['id'], data=zip(names, paths), data_QR=zip(names_list, paths_list))
    return render_template('login.html')

###Functions used in the search tab
#####fills the "Data" table
def table_data(user_id):
    user_path = '/delfos_platform/' + user_id + '/'
    create_path_user = client.makedirs(user_path)

    raw_data_path = '/delfos_platform/' + user_id + '/raw_data/'
    create_path_raw = client.makedirs(raw_data_path)
    walk_path = client.walk(raw_data_path)

    paths = []
    db_names = []
    raw_data_names = []
    total_paths = []
    data = []

    for w in walk_path:
        paths.append(w[0])

    if len(paths) != 0:
        if '/delfos_platform/'+user_id+'/raw_data' in paths:
            paths.remove('/delfos_platform/'+user_id+'/raw_data')
        if '/delfos_platform/'+user_id+'/raw_data/quality_rules' in paths:
            paths.remove('/delfos_platform/'+user_id+'/raw_data/quality_rules')

    if len(paths) != 0:
        for p in paths:
            list_folders_raw_data = client.list(p)
            for l in list_folders_raw_data:
                if (l != 'mapping_rules.csv') and (l != 'transformation_rules.csv') and (l != 'databaseName.csv'):
                    index = list_folders_raw_data.index(l)
                    raw_data = list_folders_raw_data[index]
                    raw_data_names.append(raw_data)
                    total_paths.append(p + '/' + raw_data)
                elif l == 'databaseName.csv':
                    with client.read(p + '/' + l, encoding='latin-1') as reader:
                        content = pd.read_csv(reader)
                        db_names.append(content.databaseName[0])

        for index, rdn in enumerate(raw_data_names):
            name_for_table = str(db_names[index])
            data.append(name_for_table)

    return data, total_paths

#####fills the "Quality rules" table
def table_quality_rules(user_id):
    user_path = '/delfos_platform/' + user_id + '/'
    create_path_user = client.makedirs(user_path)

    quality_rules_path = '/delfos_platform/' + user_id + '/raw_data/quality_rules/'
    create_path_quality = client.makedirs(quality_rules_path)

    list_files = client.list(quality_rules_path)
    list_paths = []

    if len(list_files) != 0:
        for lf in list_files:
            path = quality_rules_path + lf
            list_paths.append(path)
    return list_files, list_paths

#####create a csv with all information about the search - parameter, value, data and quality rules
def create_info_search(checkboxQR, data, parameter, value):
    file_name = []
    quality_rules_names = []
    databases_name = []

    if isinstance(data, list):
        type = "local"
        for d in data:
            database = os.path.dirname(d)+"/databaseName.csv"
            file_name.append(os.path.basename(d))
            with client.read(database, encoding='latin-1') as reader:
                content = pd.read_csv(reader)
                databases_name.append(content.databaseName[0])
    else:
        type ="external"
        data_split = data.split("_")
        databases_name.append(data_split[0])
        file_name.append(data_split[1])


    if isinstance(checkboxQR, list):
        if len(checkboxQR) > 1:
            for quality_rule in checkboxQR:
                quality_rules_names.append(os.path.basename(quality_rule))
        elif len(checkboxQR) == 1:
            quality_rules_names = checkboxQR[0]
            quality_rules_names = os.path.basename(quality_rules_names)
        else:
            quality_rules_names = ""
    else:
        quality_rules_names = checkboxQR

    if isinstance(databases_name, list) & len(databases_name)==1:
        databases_name = databases_name[0]
    if isinstance(file_name, list) & len(file_name)==1:
        file_name = file_name[0]

    with open('search_info.csv', 'w', newline='') as csvfile:
        fieldnames = ['sources', 'type', 'filenames/phenotype', 'parameter', 'value', 'quality_rules']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'sources': str(databases_name),'type': str(type), 'filenames/phenotype': str(file_name), 'parameter': str(parameter), 'value': str(value), 'quality_rules': str(quality_rules_names)})

    return None

###reads a raw data file that is in .csv format and applies the mapping rules
def read_csv(user_id, hdfs_path, path_raw_data, i):
    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("read csv") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    #create mapping rules path
    path_mapping_rules = "/".join(list(path_raw_data.split('/')[0:-1]))+'/mapping_rules.csv'

    #read raw data and correspondent mapping rules
    raw_data = spark.read.options(header='True', delimiter=';').csv(hdfs_path+path_raw_data)
    mapping_rules = spark.read.options(header='True', delimiter=';').csv(hdfs_path+path_mapping_rules)

    #convert mapping rules csv columns into 2 lists
    newColumns = [data[0] for data in mapping_rules.select('attribute').collect()]
    extractColumns = [data[0] for data in mapping_rules.select('name of column').collect()]

    new_raw_data = raw_data

    #checks whether the columns that are in the array "extractColumns" exist in the dataframe. If not, it creates a new column with the empty value
    for col in extractColumns:
        if col not in raw_data.columns:
            new_raw_data = new_raw_data.withColumn(col, F.lit(''))

    new_raw_data = new_raw_data.select(*extractColumns)

    df = new_raw_data.toDF(*newColumns)

    #write csv and save in hdfs
    df.repartition(1).write.format('csv').option('header',True).mode('overwrite').option('sep',';').save(hdfs_path+"/delfos_platform/"+user_id+"/staging_area/mapped_files/mapped_file_"+str(i)+".csv")

    return spark.stop()

###the following 3 functions -"get_xml_node_list", "get_xml_node_property" and "read_xml" - will be used to read an .xml file and apply mapping rules
#returns a list of nodes
def get_xml_node_list(class_name, node_list, mapping_rules):
    # Get the XPath to the each node
    rules = [rule for rule in mapping_rules if rule['elementName'] == class_name and rule['attrName'] == ""]

    if len(rules) > 0:
        # There can be more than one rule to provide different options if one of them fails
        for rule in rules:
            xpath = rule["XPath"]
            rootName = rule["rootName"]
            condition = rule["ConditionValue"]

            # Find the root node from the list of visited nodes
            for node in node_list:
                if node.tag == rootName:
                    if condition != "":
                        # The path to the list of nodes includes a value from other node
                        condition_value = get_xml_node_property(condition, "", node_list, mapping_rules)
                        # Replace XPath value
                        xpath = xpath.replace("<value>", condition_value)

                    result = node.findall(xpath)
                    if len(result) > 0:
                        return result

    return None

#returns the properties of each attribute in the given list
def get_xml_node_property(class_name, class_attribute, node_list, mapping_rules):
    # Get the rule where the target attribute is defined
    rules = [rule for rule in mapping_rules if rule['elementName'] == class_name and rule['attrName'] == class_attribute]

    if len(rules) > 0:
        # There can be more than one rule to provide different options if one of them fails
        for rule in rules:
            root_attr = rule["rootAttr"]
            xpath_node = rule["XPath"]
            root_name = rule["rootName"]
            root_node = None
            fixed_value = rule["FixedValue"]

            # First search for a fixed value provide by the user
            if fixed_value != "":
                result = fixed_value
                return result

            # Find the root node from the list of visited nodes
            for node in node_list:
                if node.tag == root_name:
                    root_node = node
                    break

            if root_attr == "":
                if xpath_node == "":
                    # The value is the text of the current node
                    result = root_node.text
                    if result is not None:
                        return result
                else:
                    # The value is the text of a subnode. The XPath to the subnode must be specified
                    node = root_node.find(xpath_node)
                    if node is not None:
                        return node.text
            else:
                if xpath_node == "":
                    # The value is stored in a property of the current object node
                    result = root_node.attrib[root_attr]
                    if result is not None:
                        return result

                else:
                    # The value is in a property of a subnode
                    node = root_node.find(xpath_node)
                    if node is not None:
                        return node.attrib[root_attr]

    return None

#this function, for now, respects only the schema/conceptual model developed by the PROS Research Center
def read_xml(user_id, path_raw_data, i):

    #create mapping rules path
    path_mapping_rules = "/".join(list(path_raw_data.split('/')[0:-1]))+'/mapping_rules.csv'

    #Read raw data
    with client.read(path_raw_data) as xml_file:
        tree = ET.parse(xml_file)
        xml_root = tree.getroot()


    # Read mapping rules
    mapping_rules = []
    with client.read(path_mapping_rules, encoding="latin-1") as rules:
        rule_reader = csv.DictReader(rules, delimiter=';')
        for rule in rule_reader:
            mapping_rules.append(dict(rule))

    #columns for dataframe and csv
    columns_list = ["variant_name", "variant_id", "assemblies", "chromosome", "phenotypes", "gene", "variant_type", "description", "polyphen_prediction", "sift_prediction", "hgvs", "databanks"]

    #first pointer
    node_list = [xml_root]

    #get variant list
    variant_list = get_xml_node_list("variation", node_list, mapping_rules)

    #extract the values of the attributes that identify each of the variants according the mapping rules
    '''
    the attributes according to the conceptual model of the PROS research center of the university of valencia are:
       variant_name,variant_id,chromosome,gene,variant_type,description,polyphen_prediction,sift_prediction,hgvs,assembly,assembly_date,
       start,end,ref,alt,risk_allele,phenotype,clinical_actionability,classification,clinical_significance,method,assertion_criteria,
       level_certainty,date,author,origin,title,year,authors,pmid,is_gwas,name,url,version,databanks_variant_id and clinvar_accession
    '''
    all_data = []
    for variant in variant_list:
        data = []

        #variant name
        variant_name = data.append(get_xml_node_property("variation", "variant_name", [variant], mapping_rules))

        #variant id
        variant_id = data.append(get_xml_node_property("variation", "variant_id", [variant], mapping_rules))

        #assemblies
        #assemblies are list because we have more than one assembly in a variant - so it is needed to move the pointer and go deeper in the structure of xml
        assembly_list = get_xml_node_list("assembly", [variant], mapping_rules)
        assemblies = []

        if assembly_list is not None:
            for assembly_element in assembly_list:
                assembly_dict = dict()
                assembly_dict["assembly"] = get_xml_node_property("assembly", "assembly", [assembly_element], mapping_rules)
                assembly_dict["assembly_date"] = get_xml_node_property("assembly", "date", [assembly_element], mapping_rules)
                assembly_dict["start"] = get_xml_node_property("assembly", "start", [assembly_element], mapping_rules)
                assembly_dict["end"] = get_xml_node_property("assembly", "end", [assembly_element], mapping_rules)
                assembly_dict["ref"] = get_xml_node_property("assembly", "ref", [assembly_element], mapping_rules)
                assembly_dict["alt"] = get_xml_node_property("assembly", "alt", [assembly_element], mapping_rules)
                assembly_dict["risk_allele"] = get_xml_node_property("assembly", "risk_allele", [assembly_element], mapping_rules)

                assemblies.append(assembly_dict)
            data.append(assemblies)
        else:
            assembly_dict = dict()
            assembly_dict["assembly"] = None
            assembly_dict["assembly_date"] = None
            assembly_dict["start"] = None
            assembly_dict["end"] = None
            assembly_dict["ref"] = None
            assembly_dict["alt"] = None
            assembly_dict["risk_allele"] = None

            assemblies.append(assembly_dict)
            data.append(assemblies)

        #chromossome
        chromosome = data.append(get_xml_node_property("variation", "chromosome", [variant], mapping_rules))

        #phenotypes
        phenotype_list = get_xml_node_list("phenotype", [variant], mapping_rules)
        phenotypes = []

        if phenotype_list is not None:

            for phenotype_element in phenotype_list:
                phenotypes_dict = dict()

                #phenotype
                phenotypes_dict["phenotype"] = get_xml_node_property("phenotype", "phenotype", [phenotype_element], mapping_rules)

                #clinical_actionability
                phenotypes_dict["clinical_actionability"] = get_xml_node_property("phenotype", "clinical_actionability", [phenotype_element], mapping_rules)

                #classification
                phenotypes_dict["classification"] = get_xml_node_property("phenotype", "classification", [phenotype_element], mapping_rules)

                rules = mapping_rules

                interpretation = []
                bibliography = []
                for rule in rules:
                    if rule['elementName'] == 'interpretation' and rule['attrName'] == "":
                        xpath = rule["XPath"]
                        condition = rule["ConditionValue"]

                        condition_value = get_xml_node_property(condition, "", [phenotype_element], rules)
                        xpath = xpath.replace("<value>", condition_value)

                        result_node = variant.findall(xpath)

                        for node in result_node:
                            interpretations_dict = dict()
                            interpretations_dict["clinical_significance"] = get_xml_node_property("interpretation", "clinical_significance", [node], rules)
                            interpretations_dict["method"] = get_xml_node_property("interpretation", "method", [node], rules)
                            interpretations_dict["assertion_criteria"] = get_xml_node_property("interpretation", "assertion_criteria", [node], rules)
                            interpretations_dict["level_certainty"] = get_xml_node_property("interpretation", "level_certainty", [node], rules)
                            interpretations_dict["date"] = get_xml_node_property("interpretation", "date", [node], rules)
                            interpretations_dict["author"] = get_xml_node_property("interpretation", "author", [node], rules)
                            interpretations_dict["origin"] = get_xml_node_property("interpretation", "origin", [node], rules)

                            interpretation.append(interpretations_dict)

                    elif rule['elementName'] == 'bibliography' and rule['attrName'] == "":
                        xpath = rule["XPath"]
                        condition = rule["ConditionValue"]

                        condition_value = get_xml_node_property(condition, "", [phenotype_element], rules)
                        xpath = xpath.replace("<value>", condition_value)

                        result_node = variant.findall(xpath)

                        for node in result_node:
                            bibliographies_dict = dict()
                            bibliographies_dict["title"] = get_xml_node_property("bibliography", "title", [node], rules)
                            bibliographies_dict["year"] = get_xml_node_property("bibliography", "year", [node], rules)
                            bibliographies_dict["authors"] = get_xml_node_property("bibliography", "authors", [node], rules)
                            bibliographies_dict["pmid"] = get_xml_node_property("bibliography", "pmid", [node], rules)
                            bibliographies_dict["is_gwas"] = get_xml_node_property("bibliography", "is_gwas", [node], rules)

                            bibliography.append(bibliographies_dict)

                if interpretation:
                    phenotypes_dict["interpretation"] = interpretation
                else:
                    interpretation = [{'clinical_significance': None,
                                       'method': None,
                                       'assertion_criteria': None,
                                       'level_certainty': None,
                                       'date': None,
                                       'author': None,
                                       'origin': None}]
                    phenotypes_dict["interpretation"] = interpretation

                if bibliography:
                    phenotypes_dict["bibliography"] = bibliography
                else:
                    bibliography = [{'title': None,
                                     'year': None,
                                     'authors': None,
                                     'pmid': None,
                                     'is_gwas': None}]
                    phenotypes_dict["bibliography"] = bibliography

                phenotypes.append(phenotypes_dict)
            data.append(phenotypes)
        else:
            phenotypes_dict = dict()
            phenotypes_dict["phenotype"] = None
            phenotypes_dict["clinical_actionability"] = None
            phenotypes_dict["classification"] = None
            phenotypes_dict["interpretation"] = [{'clinical_significance': None,
                                                  'method': None,
                                                  'assertion_criteria': None,
                                                  'level_certainty': None,
                                                  'date': None,
                                                  'author': None,
                                                  'origin': None}]
            phenotypes_dict["bibliography"] = [{'title': None,
                                                'year': None,
                                                'authors': None,
                                                'pmid': None,
                                                'is_gwas': None}]
            phenotypes.append(phenotypes_dict)
            data.append(phenotypes)

        #genes
        genes_list = get_xml_node_list("gene", [variant], mapping_rules)
        genes = []

        if genes_list is not None:
            for gene_element in genes_list:
                genes.append(get_xml_node_property("gene", "gene", [gene_element], mapping_rules))
            data.append(genes)
        else:
            data.append(None)

        #variant type
        variant_type = data.append(get_xml_node_property("variation", "variant_type", [variant], mapping_rules))

        #description
        description = data.append(get_xml_node_property("variation", "description", [variant], mapping_rules))

        #polyphen prediction
        polyphen_prediction = data.append(get_xml_node_property("variation", "polyphen_prediction", [variant], mapping_rules))

        #sift_prediction
        sift_prediction = data.append(get_xml_node_property("variation", "sift_prediction", [variant], mapping_rules))

        #hgvs
        hgvs_list = get_xml_node_list("hgvs", [variant], mapping_rules)
        hgvs = []

        if hgvs_list is not None:
            for hgvs_element in hgvs_list:
                hgvs.append(get_xml_node_property("hgvs", "hgvs", [hgvs_element], mapping_rules))
            data.append(hgvs)
        else:
            data.append(None)

        #databanks
        databank_list = get_xml_node_list("databank", [variant], mapping_rules)
        databanks = []

        if databank_list is not None:
            for databank_element in databank_list:
                databank_dict = dict()
                databank_dict["name"] = get_xml_node_property("databank", "name", [databank_element], mapping_rules)
                databank_dict["url"] = get_xml_node_property("databank", "url", [databank_element], mapping_rules)
                databank_dict["version"] = get_xml_node_property("databank", "version", [databank_element], mapping_rules)
                databank_dict["databanks_variant_id"] = get_xml_node_property("databank", "variant_id", [databank_element], mapping_rules)
                databank_dict["clinvar_accession"] = get_xml_node_property("databank", "clinvar_accession", [databank_element], mapping_rules)

                databanks.append(databank_dict)
            data.append(databanks)
        else:
            databank_dict = dict()
            databank_dict["name"] = None
            databank_dict["url"] = None
            databank_dict["version"] = None
            databank_dict["databanks_variant_id"] = None
            databank_dict["clinvar_accession"] = None

            databanks.append(databank_dict)
            data.append(databanks)

        all_data.append(data)

    #create a dataframe
    df = pd.DataFrame(all_data, columns=columns_list)

    #explode some columns so that we have the data in tabular form to make it easier to manipulate it later
    ##explode assemblies
    df = df.explode("assemblies")
    df = pd.concat([df.drop(['assemblies'], axis=1), df['assemblies'].apply(pd.Series)], axis=1)

    ##explode phenotypes
    df = df.explode("phenotypes")
    df = pd.concat([df.drop(['phenotypes'], axis=1), df['phenotypes'].apply(pd.Series)], axis=1)

    ##explode interpretation
    df = df.explode("interpretation")
    df = pd.concat([df.drop(['interpretation'], axis=1), df['interpretation'].apply(pd.Series)], axis=1)

    ##explode bibliography
    df = df.explode("bibliography")
    df = pd.concat([df.drop(['bibliography'], axis=1), df['bibliography'].apply(pd.Series)], axis=1)

    ##explode databanks
    df = df.explode("databanks")
    df = pd.concat([df.drop(['databanks'], axis=1), df['databanks'].apply(pd.Series)], axis=1)

    ##explode genes
    df = df.explode("gene")

    ##explode hgvs
    df = df.explode("hgvs")

    #save in hdfs
    with client.write("/delfos_platform/"+user_id+"/staging_area/mapped_files/mapped_file_"+str(i)+".csv", encoding='latin-1', overwrite=True) as writer:
        df.to_csv(writer, index = False, sep=";", header=True)

    return None

###applies the respective transformation rules to the respective "mapped" files
def transformed_data(user_id,hdfs_path,transformation_rules_path,mapped_file_path,i):
    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("apply transformation rules") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    #read mapped file
    df = spark.read.option("header",True).option("delimiter",";").format("csv").load(hdfs_path+mapped_file_path+"mapped_file_"+str(i)+".csv")

    #read transformation rules
    transformation_rules = spark.read.option("header",True).option("delimiter",";").format("csv").load(hdfs_path+transformation_rules_path)

    #convert transformation rules csv columns into 4 lists
    list_columns = [data[0] for data in transformation_rules.select('attribute').collect()]
    list_functions = [data[0] for data in transformation_rules.select('function').collect()]
    list_expressions = [data[0] for data in transformation_rules.select('regular expression').collect()]
    list_replacements = [data[0] for data in transformation_rules.select('replacement').collect()]

    messages_result = []
    #apply transformation rules to the mapped file and create a new dataframe
    for (column, function, expression, replacement) in zip_longest(list_columns, list_functions, list_expressions, list_replacements):
        if function == 'replace':
            if column not in df.columns:
                flash("The attribute " + column + " does not exist (check the transformation rules)")
                messages_result.append("Error")
            else:
                df = df.withColumn(column, \
                                   F.when(F.col(column).rlike(expression), F.regexp_replace(column, expression, replacement)).otherwise(F.col(column)))
        elif function == 'extract':
            if column not in df.columns:
                flash("The attribute " + column + " does not exist (check the transformation rules)")
                messages_result.append("Error")
            else:
                df = df.withColumn(column, \
                                   F.when(F.col(column).rlike(expression), F.regexp_extract(column, expression, 1)).otherwise(F.col(column)))

        elif function == 'create empty attribute':
            if column not in df.columns:
                df = df.withColumn(column, F.lit(None))
            else:
                flash("The attribute " + column + " already exists (check the transformation rules)")
                messages_result.append("Error")
        elif function == 'use values from another attribute':
            if column not in df.columns:
                flash("The attribute " + column + " does not exist (check the transformation rules)")
                messages_result.append("Error")
            else:
                if replacement not in df.columns:
                    flash("The attribute " + replacement + " does not exist (check the transformation rules)")
                    messages_result.append("Error")
                else:
                    df = df.withColumn(column, F.when(F.col(column).isNull(), F.col(replacement)).otherwise(F.col(column)))

        elif function == 'fill na':
            if column not in df.columns:
                flash("The attribute " + column + " does not exist (check the transformation rules)")
                messages_result.append("Error")
            else:
                df = df.fillna(value=replacement,subset=[column])

    #save the new dataframe in the staging area
    df.repartition(1).write.format('csv').option('header',True).mode('overwrite').option('sep',';').save(hdfs_path+"/delfos_platform/"+user_id+"/staging_area/transformed_files/transformed_file_"+str(i)+".csv")

    return messages_result, spark.stop()

###the function "create_treated_file" uses the following 4 functions: "unite_dfs", "to_json", "write_json_and_csv" and "apply_quality_rules
'''
this function collects all the transformed files, 
applies the filter you submitted to them, builds a json schema 
and puts it in a variable, and finally converts it to a json file and a csv file
'''
def create_treated_file(user_id, hdfs_path, parameter, value, quality_rules_list):
    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("merge transformed files and filter and then covert to json") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    ##MERGE TRANSFORMED FILES
    dataframes_list = []

    #attributes defined by the PROS research center
    columns_schema = ["variant_name","variant_id","chromosome","gene","variant_type","description","polyphen_prediction","sift_prediction","hgvs","assembly","assembly_date",
                      "start","end","ref","alt","risk_allele","phenotype","clinical_actionability","classification","clinical_significance","method","assertion_criteria","level_certainty",
                      "date","author","origin","title","year","authors","pmid","is_gwas","name","url","version","databanks_variant_id","clinvar_accession"]

    #lists all files that are in the staging area in the transformed files folder
    list_files_hdfs = client.list('/delfos_platform/'+user_id+'/staging_area/transformed_files/')

    #for each file in the transformed files folder
    for file in list_files_hdfs:
        #the file will be converted to a dataframe and it will be checked that this dataframe contains all the attributes that were defined in the schema. If not, it will be created.
        df = spark.read.options(header='True', delimiter=';').csv("hdfs://hdfs-nn:9000/delfos_platform/"+user_id+"/staging_area/transformed_files/"+file)
        columns_df = df.columns
        for column in columns_schema:
            if column not in columns_df:
                #create empty column
                df = df.withColumn(column, F.lit(None))
        #create a list of dataframes
        dataframes_list.append(df)

    #uses the function "unite_dfs" to join the various dataframes
    united_df = reduce(unite_dfs, dataframes_list)

    ##FILTER THE RESULT OF THE MERGER OF THE TRANSFORMED FILES ACCORDING TO THE PARAMETER AND THE VALUE THE USER WANTS
    parameter = parameter.lower()

    messages_result = []

    if parameter in united_df.columns:
        filtered_df = united_df.filter(F.col(parameter) == value)

        if filtered_df.count() == 0:
            flash("No results to show with parameter "+parameter+" with value "+value)
            messages_result.append("Error")
        else:
            #create a date and time variable
            tz_Portugal = pytz.timezone('Portugal')
            date_time = datetime.datetime.now(tz_Portugal).strftime("%d_%m_%Y %H_%M_%S")
            #convert result to json schema and save in hdfs
            json_schema_str = to_json(filtered_df)
            write_json_and_csv(user_id, hdfs_path, filtered_df, filtered_df.toPandas(), quality_rules_list,json_schema_str, date_time)
    else:
        flash("The parameter " + parameter + " does not exist")
        messages_result.append("Error")

    return messages_result, spark.stop()

#unifies several dataframes by name and different columns, adds them together with null values
def unite_dfs(df1, df2):
    return df1.unionByName(df2, allowMissingColumns=True)

#convert dataframe to json schema given by pros research center
def to_json(df):
    #convert df to pandas df
    df = df.toPandas()

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

#save in hdfs in csv and json format
##needs a pandas dataframe and the result of to_json function
def write_json_and_csv(user_id, hdfs_path, df, pandas_dataframe, quality_rules_list,json_schema_str, date_time):
    jobs_path = '/delfos_platform/'+user_id+'/jobs/'
    client.makedirs(jobs_path)
    folders_hdfs = client.list(jobs_path)

    #date_time = str(datetime.datetime.now())

    if len(folders_hdfs) == 0:
        job_path = '/delfos_platform/'+user_id+'/jobs/job_1/'
        #client.makedirs(job_path)
        #write json
        with client.write(job_path+'treated_file.json', overwrite=True, encoding='latin-1') as json_writer:
            dump(json_schema_str, json_writer, indent=2)

        with client.write(job_path+'result/'+date_time+'/treated_file.json', overwrite=True, encoding='latin-1') as json_writer:
            dump(json_schema_str, json_writer, indent=2)

        apply_quality_rules(hdfs_path, df, quality_rules_list, job_path, date_time)

        #write csv
        with client.write(job_path+'treated_file.csv', overwrite=True, encoding='latin-1') as csv_writer:
            pandas_dataframe.to_csv(csv_writer, index = False, sep=";", header=True)

        #upload info about the search
        client.upload(job_path+'result/'+date_time+'/search_info.csv', 'search_info.csv')
        os.remove('search_info.csv')
    else:
        new_list = []
        for f in folders_hdfs:
            f = f.replace("job_","")
            new_list.append(int(f))
        new_list = sorted(new_list)

        last_folder = new_list[-1]
        new_index = str(last_folder+1)
        job_index = "job_"+new_index
        job_path = "/delfos_platform/"+user_id+"/jobs/"+job_index+"/"

        #write json - treated file - in a job folder
        with client.write(job_path+'treated_file.json', overwrite=True, encoding='latin-1') as json_writer:
            dump(json_schema_str, json_writer, indent=2)

        #write json - treated file - in a job folder
        with client.write(job_path+'result/'+date_time+'/treated_file.json', overwrite=True, encoding='latin-1') as json_writer:
            dump(json_schema_str, json_writer, indent=2)

        apply_quality_rules(hdfs_path, df, quality_rules_list, job_path, date_time)

        #write csv
        with client.write(job_path+'treated_file.csv', overwrite=True, encoding='latin-1') as csv_writer:
            pandas_dataframe.to_csv(csv_writer, index = False, sep=";", header=True)

        #upload info about the search
        client.upload(job_path+'result/'+date_time+'/search_info.csv', 'search_info.csv')
        os.remove('search_info.csv')
    return None

#applies the quality files that the user has selected
def apply_quality_rules(hdfs_path, df, quality_rules_list, job_path, date_time):
    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("apply quality rules") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    #result_quality_array = []
    for quality_rules in quality_rules_list:
        #filename
        filename = os.path.splitext(os.path.basename(quality_rules))[0]

        #read quality rules
        quality_rules = spark.read.option("header",True).option("delimiter",";").format("csv").load(hdfs_path+quality_rules)

        #convert quality rules csv columns into 6 lists
        list_columns = [data[0] for data in quality_rules.select('attribute').collect()]
        list_types = [data[0] for data in quality_rules.select('type').collect()]
        list_notColumn = [data[0] for data in quality_rules.select('not').collect()]
        list_conditions = [data[0] for data in quality_rules.select('condition').collect()]
        list_values = [data[0] for data in quality_rules.select('value').collect()]
        list_messages = [data[0] for data in quality_rules.select('message').collect()]

        #apply quality rules to the treated dataframe
        for (column, type, notColumn, condition, value, message) in zip_longest(list_columns, list_types, list_notColumn, list_conditions, list_values, list_messages):
            if type in ["integer", "float", "double", "byte", "short", "long", "decimal"]:
                df_change_type = df.withColumn(column, F.col(column).cast(type))
                if notColumn == "yes":
                    if condition == "empty":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column).isNull()), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == ">=":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) >= int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "<=":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) <= int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "<":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) < int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == ">":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) > int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "=":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) == int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "range":
                        value = value.split(",")
                        lower = int(value[0].replace("(", "").strip())
                        upper = int(value[1].replace(")", "").strip())
                        df = df_change_type.withColumn(column, \
                                                       F.when(~df_change_type[column].between(lower, upper), message) \
                                                       .otherwise(F.col(column)))
                else:
                    if condition == "empty":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column).isNull()), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == ">=":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) >= int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "<=":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) <= int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "<":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) < int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == ">":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) > int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "=":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) == int(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "range":
                        value = value.split(",")
                        lower = int(value[0].replace("(", "").strip())
                        upper = int(value[1].replace(")", "").strip())
                        df = df_change_type.withColumn(column, \
                                                       F.when(df_change_type[column].between(lower, upper), message) \
                                                       .otherwise(F.col(column)))
            elif type == "string":
                df_change_type = df.withColumn(column, F.col(column).cast(type))
                if notColumn == "yes":
                    if condition == "empty":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column).isNull()), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "contains":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column).contains(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "equals":
                        df = df_change_type.withColumn(column, \
                                                       F.when(~(F.col(column) == value), message) \
                                                       .otherwise(F.col(column)))

                else:
                    if condition == "empty":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column).isNull()), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "contains":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column).contains(value)), message) \
                                                       .otherwise(F.col(column)))
                    elif condition == "equals":
                        df = df_change_type.withColumn(column, \
                                                       F.when((F.col(column) == value), message) \
                                                       .otherwise(F.col(column)))
            else:
                #print("The type"+type+"does not exist!")
                flash("The type"+type+"does not exist! (check the quality rules)")

        #convert to json structure and write json
        result_quality = to_json(df)
        #result_quality_array.append(result_quality)

        #date_time = str(datetime.datetime.now())

        #write_quality_result(user_id, result_quality, filename)
        with client.write(job_path+'result/'+date_time+'/'+filename+'.json', overwrite=True, encoding='latin-1') as json_writer:
            dump(result_quality, json_writer, indent=2)

    return spark.stop()

# Download a specific result in a specific job - results_tab
@app.route('/home/local/results', methods=['GET', 'POST'])
def results_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        #names_and_paths = table_jobs(user_id)
        #names = names_and_paths[0]
        #paths = names_and_paths[1]
        jobs_details = jobs_info(user_id)
        jobs_names = jobs_details[0]
        total_paths = jobs_details[1]
        dates = jobs_details[2]
        times = jobs_details[3]

        search_details = search_info(total_paths)
        types = search_details[0]
        parameters = search_details[1]
        values = search_details[2]
        rules = search_details[3]

        if request.method == 'POST':
            selectedValue = request.form['download']
            tmp = os.path.abspath('result')

            if os.path.exists(tmp):
                shutil.rmtree(tmp)
                os.remove("result.zip")

            #download to the app basedir
            client.download(selectedValue, tmp, overwrite=True)

            #create zip
            #use send_file function
            #remove result from hdfs and from app
            shutil.make_archive("result","zip",tmp)

            return send_file("result.zip", mimetype="zip", download_name="result.zip", as_attachment=True)
        return render_template("search_result.html", id=session['id'], jobs=zip(jobs_names, dates, times, types, parameters, values, rules, total_paths))
    return render_template('login.html')

#returns all information about each of the existing jobs
def jobs_info(user_id):
    user_path = '/delfos_platform/' + user_id + '/'
    create_path_user = client.makedirs(user_path)

    jobs_list = '/delfos_platform/' + user_id + '/jobs/'
    create_path_job = client.makedirs(jobs_list)

    jobs = client.list(jobs_list)

    jobs_names = []
    total_paths = []
    dates = []
    times = []

    if len(jobs_list) != 0:
        for job in jobs:
            result_path = client.list(jobs_list+job+"/result")
            for result in result_path:
                jobs_names.append(job)
                total_paths.append(jobs_list+job+"/result/"+result)

        for path in total_paths:
            name_folder = os.path.basename(path).split(" ")
            date = name_folder[0].replace("_", "/")
            time = name_folder[1].replace("_", ":")
            dates.append(date)
            times.append(time)

    return jobs_names, total_paths, dates, times

#returns all information about the searches that were done - parameter, value and quality rules
def search_info(total_paths):
    hdfs_path = "hdfs://hdfs-nn:9000"

    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("read info about the search") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    types = []
    parameters = []
    values = []
    rules = []

    if len(total_paths) != 0:
        for path in total_paths:
            df = spark.read.option("header",True).option("delimiter",",").format("csv").load(hdfs_path+path+'/search_info.csv')
            type = df.select('type').collect()[0].type
            types.append(type)
            parameter = df.select('parameter').collect()[0].parameter
            parameters.append(parameter)
            value = df.select('value').collect()[0].value
            values.append(value)
            quality_rules = df.select('quality_rules').collect()[0].quality_rules
            rules.append(quality_rules)

    return types, parameters, values, rules, spark.stop()

#####EXTERNAL
# Searches the phenotype written by the user,
# extracts the attributes according to the json schema,
# makes the necessary transformations and applies the filter chosen by the user - Database and search tab (external)
# search tab
@app.route('/home/external/', methods=["GET", "POST"])
def external_search_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])

        if request.method == 'POST':
            hdfs_path = "hdfs://hdfs-nn:9000"
            mapped_file_path = "/delfos_platform/"+user_id+"/staging_area/mapped_files/mapped_file_1.csv"

            selectSource = request.form.get('databaseName')
            phenotype = request.form['phenotype']
            parameter = request.form.get('parameterName')
            value = request.form["value"]

            #databaseName ----> example: ClinVar_Migraine with aura
            data = selectSource+"_"+phenotype
            checkboxQR = []

            #applies the phenotype written by the user
            raw_data = get_clinvar_data_by_phenotype(phenotype)

            #create a mapped file - extract the attributes according to the json schema
            read_clinvar_data(user_id, raw_data)

            #reads the mapped file that was created and applies the necessary transformations to it, and saves the result to HDFS
            transform_data_from_clinvar(user_id, hdfs_path, mapped_file_path)

            #create a csv with all information about the search - parameter, value, data and quality rules
            create_info_search(checkboxQR, data, parameter, value)

            #create treated file
            create_file_result = create_treated_file(user_id, hdfs_path, parameter, value, checkboxQR)
            messages_create_file = create_file_result[0]

            client.delete("/delfos_platform/"+user_id+"/staging_area/", recursive=True)

            if len(messages_create_file) == 0:
                return redirect(url_for('external_results_tab'))

        return render_template("external.html", id=session['id'])
    return render_template('login.html')

###the previous function uses the following functions:
####for the extraction: get_clinvar_data_by_phenotype(phenotype),read_clinvar_data(user_id, raw_data),get_assemblies(variant),assembly_data(accession),
####                    get_assembly_idList(accession),get_assembly_data(id),get_chromosome(variant),get_genes(variant),get_hgvs(variant),get_databank(variant),
####                    get_databank_version(db_summary),get_ncbi_database_summary(database),get_phenotypes(variant),get_phenotype(variant, id, clinicalAssertion),
###                     get_assertion_method(clinicalAssertion)
####for the transformation: transform_data_from_clinvar(user_id, hdfs_path, mapped_file_path)
#                    |
#                    |
#                    V
#### ---------- EXTRACTION ---------
#This function gets the information about the variants associated to a specified phenotype from the ClinVar database
def get_clinvar_data_by_phenotype(phenotype):
    Entrez.email = "delfos.platform.upv@gmail.com"
    Entrez.api_key = "6e27666506b3fe1d92f074f1d5c5a289e608"

    #get a list of identifiers associated with a phenotype
    phenotype = "\" %s \" [DIS]" %phenotype
    handle = Entrez.esearch(db="clinvar", term=phenotype)
    phenotype_ids = Entrez.read(handle)['IdList']

    xml = Entrez.efetch(db="clinvar", id=phenotype_ids, rettype='vcv', is_varationid="true", from_esearch="true", retmode='xml')

    return xml

#create a mapped file - extract the attributes according to the json schema
def read_clinvar_data(user_id, raw_data):
    #parse xml and get root
    tree = ET.parse(raw_data)
    root = tree.getroot()

    #columns for dataframe and csv
    columns_list = ["variant_name", "variant_id", "assemblies", "chromosome", "phenotypes", "gene", "variant_type", "description", "polyphen_prediction", "sift_prediction", "hgvs", "databanks"]

    #extract the attribute according to the json schema
    all_data = []
    for variant in root:
        data = []
        #variant_name - will be generated
        variant_name = None
        data.append(variant_name)

        #variant_id - will be generated
        variant_id = None
        data.append(variant_id)

        #assemblies
        assemblies = get_assemblies(variant)
        data.append(assemblies)

        #chromosome
        chromosome = get_chromosome(variant)
        data.append(chromosome)

        #phenotypes
        phenotypes = get_phenotypes(variant)
        data.append(phenotypes)

        #genes
        genes = get_genes(variant)
        data.append(genes)

        #variant_type
        variant_type = variant.attrib["VariationType"]
        data.append(variant_type)

        #description
        description = None
        data.append(description)

        #polyphen_prediction
        polyphen_prediction = None
        data.append(polyphen_prediction)

        #sift_prediction
        sift_prediction = None
        data.append(sift_prediction)

        #hgvs
        hgvs_list = get_hgvs(variant)
        data.append(hgvs_list)

        #databanks
        databanks = get_databank(variant)
        data.append(databanks)

        all_data.append(data)

    #create a dataframe
    df = pd.DataFrame(all_data, columns=columns_list)

    #explode some columns so that we have the data in tabular form to make it easier to manipulate it later
    ##explode assemblies
    df = df.explode("assemblies")
    df = pd.concat([df.drop(['assemblies'], axis=1), df['assemblies'].apply(pd.Series)], axis=1)

    ##explode phenotypes
    df = df.explode("phenotypes")
    df = pd.concat([df.drop(['phenotypes'], axis=1), df['phenotypes'].apply(pd.Series)], axis=1)

    ##explode interpretation
    df = df.explode("interpretation")
    df = pd.concat([df.drop(['interpretation'], axis=1), df['interpretation'].apply(pd.Series)], axis=1)

    ##explode bibliography
    df = df.explode("bibliography")
    df = pd.concat([df.drop(['bibliography'], axis=1), df['bibliography'].apply(pd.Series)], axis=1)

    ##explode databanks
    df = df.explode("databanks")
    df = pd.concat([df.drop(['databanks'], axis=1), df['databanks'].apply(pd.Series)], axis=1)

    ##explode genes
    df = df.explode("gene")

    ##explode hgvs
    df = df.explode("hgvs")

    #save in hdfs
    with client.write("/delfos_platform/"+user_id+"/staging_area/mapped_files/mapped_file_1.csv", encoding='latin-1', overwrite=True) as writer:
        df.to_csv(writer, index = False, sep=";", header=True)

    return None

########assemblies
def get_assemblies(variant):
    assemblies = []
    #assemblies
    location = variant.findall("./InterpretedRecord/SimpleAllele/Location")
    assembly = None
    assembly_date = None
    start = None
    ref = None
    alt = None
    risk_allele = None

    #try to extract from SequenceLocation
    if location is not None:
        for l in location:
            for sequenceLocation in l.findall('./SequenceLocation'):
                if 'Assembly' in sequenceLocation.attrib:
                    assembly = sequenceLocation.attrib["Assembly"]
                else:
                    assembly = None

                if assembly is not None:
                    assembly_date = assembly_data(assembly)[1]
                else:
                    assembly_date = None

                if 'start' in sequenceLocation.attrib:
                    start = sequenceLocation.attrib["start"]
                elif 'display_start' in sequenceLocation.attrib:
                    start = sequenceLocation.attrib["display_start"]
                else:
                    start = None

                if 'stop' in sequenceLocation.attrib:
                    end = sequenceLocation.attrib["stop"]
                elif 'display_stop' in sequenceLocation.attrib:
                    end = sequenceLocation.attrib["display_stop"]
                else:
                    end = None

                if 'referenceAlleleVCF' in sequenceLocation.attrib:
                    ref = sequenceLocation.attrib["referenceAlleleVCF"]
                else:
                    ref = None

                if 'alternateAlleleVCF' in sequenceLocation.attrib:
                    alt = sequenceLocation.attrib["alternateAlleleVCF"]
                    risk_allele = sequenceLocation.attrib["alternateAlleleVCF"]
                else:
                    alt = None
                    risk_allele = None

                assembly_dict = dict()
                assembly_dict["assembly"] = assembly
                assembly_dict["assembly_date"] = assembly_date
                assembly_dict["start"] = start
                assembly_dict["end"] = end
                assembly_dict["ref"] = ref
                assembly_dict["alt"] = alt
                assembly_dict["risk_allele"] = risk_allele

                assemblies.append(assembly_dict)

    #try to extract from HGVS expressions (only NC_)
    if len(assemblies) == 0:
        hgvs_list = variant.findall("./InterpretedRecord/SimpleAllele/HGVSlist")

        if hgvs_list is not None:
            for hgvs in hgvs_list:
                expression = hgvs.find('.//NucleotideExpression/Expression').text
                start = expression
                end = expression

                # Extract only NC_ expressions
                if expression is not None and expression.startswith('NC_'):
                    assembly = hgvs.find('.//NucleotideExpression').attrib['Assembly']
                    if assembly is not None:
                        assembly_date = assembly_data(assembly)[1]
                    else:
                        assembly_date = None

                assembly_dict = dict()
                assembly_dict["assembly"] = assembly
                assembly_dict["assembly_date"] = assembly_date
                assembly_dict["start"] = start
                assembly_dict["end"] = end
                assembly_dict["ref"] = None
                assembly_dict["alt"] = None
                assembly_dict["risk_allele"] = None

                assemblies.append(assembly_dict)

    if len(assemblies) != 0:
        for ass in assemblies:
            if ass['ref'] is None and ass['alt'] is None:
                canonical = variant.find("./InterpretedRecord/SimpleAllele/CanonicalSPDI")
                if canonical is not None:
                    ass['ref'] = canonical.text
                    ass['alt'] = canonical.text
                    ass['risk_allele'] = canonical.text

    return assemblies

#Extracts the required data about an assembly
def assembly_data(accession):
    edata = get_assembly_idList(accession)
    id = edata["IdList"][0]

    edata = get_assembly_data(id)
    assembly = edata['DocumentSummarySet']['DocumentSummary'][0]['AssemblyName']
    last_update = edata['DocumentSummarySet']['DocumentSummary'][0]['AsmUpdateDate']

    return assembly, last_update

#Connects to Assembly and extracts a list of identifiers corresponding to the accession term (output format: list)
def get_assembly_idList(accession):
    Entrez.email = "delfos.platform.upv@gmail.com"
    Entrez.api_key = "6e27666506b3fe1d92f074f1d5c5a289e608"

    handle = Entrez.esearch(db="assembly", term=accession)
    record = Entrez.read(handle)

    return record

#Connects to Assembly and extracts the raw data corresponding to the identifier of the assembly (output format: list)
def get_assembly_data(id):
    Entrez.email = "delfos.platform.upv@gmail.com"
    Entrez.api_key = "6e27666506b3fe1d92f074f1d5c5a289e608"

    handle = Entrez.esummary(db="assembly", id=id)
    record = Entrez.read(handle)

    return record

########chromosome
def get_chromosome(variant):
    cyt_location = variant.find('./InterpretedRecord/SimpleAllele/Location/CytogeneticLocation')
    sequenceLocation = variant.find('./InterpretedRecord/SimpleAllele/Location/SequenceLocation[1]')
    chromosome = None
    accession = None

    #try to extract from sequence location
    if sequenceLocation is not None:
        chromosome = sequenceLocation.attrib["Chr"]

    #try to extract from cytogenetic location
    if chromosome is None:
        if cyt_location is not None:
            chromosome = cyt_location.text

    # Try to extract from variant name
    if chromosome is None:
        variant_name = variant.attrib["VariationName"]
        if variant_name.startswith('NC_'):
            chromosome = variant_name

    #try to extract from HGVS expressions
    if chromosome is None:
        hgvs_list = variant.findall("./InterpretedRecord/SimpleAllele/HGVSlist")

        if hgvs_list is not None:
            for hgvs in hgvs_list:
                accession = hgvs.find('.//NucleotideExpression/Expression').text

                if accession is not None and accession.startswith('NC_'):
                    chromosome = accession

    # Try to extract from the genes
    if chromosome is None:
        genes_list = variant.findall("./InterpretedRecord/SimpleAllele/GeneList")

        if genes_list is not None:
            for gene in genes_list:
                location = gene.find(".//Location")
                if location is not None:
                    sequenceLocation = location.find('./SequenceLocation[1]')
                    if sequenceLocation is not None:
                        chromosome = sequenceLocation.attrib["Chr"]

    return chromosome

########genes
def get_genes(variant):
    genes = []
    genes_list = variant.findall("./InterpretedRecord/SimpleAllele/GeneList")

    if genes_list is not None:
        for gene in genes_list:
            all_genes = gene.findall("./Gene")
            if all_genes is not None:
                for g in all_genes:
                    genes.append(g.attrib["Symbol"])
    return genes

########hgvs
def get_hgvs(variant):
    hgvs_list = []
    hgvs_node = variant.findall("./InterpretedRecord/SimpleAllele/HGVSlist")

    if len(hgvs_node) == 0:
        hgvs_list = None
    else:
        for hgsv in hgvs_node:
            expression_list = hgsv.findall('.//Expression')

            if len(expression_list) == 0:
                hgvs_list = None
            else:
                for expression in expression_list:
                    hgvs_list.append(expression.text)
    return hgvs_list

########databanks
def get_databank(variant):
    databanks = []
    clinvar_id = variant.attrib["VariationID"]
    db_summary = get_ncbi_database_summary("clinvar")
    clinvar_version = get_databank_version(db_summary)

    databank_dict = dict()
    databank_dict["name"] = "ClinVar"
    databank_dict["url"] = "https://www.ncbi.nlm.nih.gov/clinvar/"
    databank_dict["version"] = clinvar_version
    databank_dict["databanks_variant_id"] = clinvar_id
    databank_dict["clinvar_accession"] = variant.attrib["Accession"]

    databanks.append(databank_dict)

    return databanks

def get_databank_version(db_summary):
    return db_summary["DbInfo"]["DbBuild"]

def get_ncbi_database_summary(database):
    Entrez.email = "delfos.platform.upv@gmail.com"
    Entrez.api_key = "6e27666506b3fe1d92f074f1d5c5a289e608"

    handle = Entrez.einfo(db="clinvar")
    db_summary = Entrez.read(handle)

    return db_summary

########phenotypes
def get_phenotypes(variant):
    clinicalAssertionList = variant.findall("./InterpretedRecord/ClinicalAssertionList/ClinicalAssertion")
    phenotypes = []

    if clinicalAssertionList is not None:
        for clinicalAssertion in clinicalAssertionList:
            if "ID" in clinicalAssertion.attrib:
                id = clinicalAssertion.attrib["ID"]
            else:
                id = None

            level_certainty = clinicalAssertion.find("./ReviewStatus").text
            clinical_significance = clinicalAssertion.find("./Interpretation/Description").text

            interpretation_date_location = clinicalAssertion.find("./Interpretation")
            if "DateLastEvaluated" in interpretation_date_location.attrib:
                interpretation_date = interpretation_date_location.attrib["DateLastEvaluated"]
            else:
                interpretation_date = None

            author_location = clinicalAssertion.find("./ClinVarAccession")
            if "SubmitterName" in author_location.attrib:
                author = author_location.attrib["SubmitterName"]
            else:
                author = None

            method = clinicalAssertion.find("./ObservedInList/ObservedIn/Method/MethodType").text
            origin = clinicalAssertion.find("./ObservedInList/ObservedIn/Sample/Origin").text

            assertion_criteria = get_assertion_method(clinicalAssertion)

            phenotype = get_phenotype(variant, id, clinicalAssertion)

            for name in phenotype:
                found = False
                if len(phenotypes) > 0:
                    for i in range(0,len(phenotypes)):
                        if phenotypes[i]["phenotype"].lower() == name.lower():
                            found = True

                            interpretation_dict = dict()
                            interpretation_dict["clinical_significance"] = clinical_significance
                            interpretation_dict["method"] = method
                            interpretation_dict["assertion_criteria"] = assertion_criteria
                            interpretation_dict["level_certainty"] = level_certainty
                            interpretation_dict["date"] = interpretation_date
                            interpretation_dict["author"] = author
                            interpretation_dict["origin"] = origin

                            phenotypes[i]["interpretation"].append(interpretation_dict)

                            bibliography_dict = dict()
                            bibliography_dict["title"] = None
                            bibliography_dict["year"] = None
                            bibliography_dict["authors"] = None
                            bibliography_dict["pmid"] = None
                            bibliography_dict["is_gwas"] = None

                            phenotypes[i]["bibliography"].append(bibliography_dict)

                    if not found:
                        phenotypes_dict = dict()
                        phenotypes_dict["phenotype"] = name
                        phenotypes_dict["clinical_actionability"] = None
                        phenotypes_dict["classification"] = None

                        interpretation_dict = dict()
                        interpretation_dict["clinical_significance"] = clinical_significance
                        interpretation_dict["method"] = method
                        interpretation_dict["assertion_criteria"] = assertion_criteria
                        interpretation_dict["level_certainty"] = level_certainty
                        interpretation_dict["date"] = interpretation_date
                        interpretation_dict["author"] = author
                        interpretation_dict["origin"] = origin

                        phenotypes_dict["interpretation"] = [interpretation_dict]

                        bibliography_dict = dict()
                        bibliography_dict["title"] = None
                        bibliography_dict["year"] = None
                        bibliography_dict["authors"] = None
                        bibliography_dict["pmid"] = None
                        bibliography_dict["is_gwas"] = None

                        phenotypes_dict["bibliography"] = [bibliography_dict]

                        phenotypes.append(phenotypes_dict)
                else:
                    phenotypes_dict = dict()
                    phenotypes_dict["phenotype"] = name
                    phenotypes_dict["clinical_actionability"] = None
                    phenotypes_dict["classification"] = None

                    interpretation_dict = dict()
                    interpretation_dict["clinical_significance"] = clinical_significance
                    interpretation_dict["method"] = method
                    interpretation_dict["assertion_criteria"] = assertion_criteria
                    interpretation_dict["level_certainty"] = level_certainty
                    interpretation_dict["date"] = interpretation_date
                    interpretation_dict["author"] = author
                    interpretation_dict["origin"] = origin

                    phenotypes_dict["interpretation"] = [interpretation_dict]

                    bibliography_dict = dict()
                    bibliography_dict["title"] = None
                    bibliography_dict["year"] = None
                    bibliography_dict["authors"] = None
                    bibliography_dict["pmid"] = None
                    bibliography_dict["is_gwas"] = None

                    phenotypes_dict["bibliography"] = [bibliography_dict]

                    phenotypes.append(phenotypes_dict)

    if len(phenotypes) == 0:
        phenotypes_dict = dict()
        phenotypes_dict["phenotype"] = None
        phenotypes_dict["clinical_actionability"] = None
        phenotypes_dict["classification"] = None
        phenotypes_dict["interpretation"] = [{'clinical_significance': None,
                                              'method': None,
                                              'assertion_criteria': None,
                                              'level_certainty': None,
                                              'date': None,
                                              'author': None,
                                              'origin': None}]
        phenotypes_dict["bibliography"] = [{'title': None,
                                            'year': None,
                                            'authors': None,
                                            'pmid': None,
                                            'is_gwas': None}]
        phenotypes.append(phenotypes_dict)

    return phenotypes

def get_phenotype(variant, id, clinicalAssertion):
    traitMappingList = variant.findall("./InterpretedRecord/TraitMappingList")
    name = []

    if traitMappingList is not None:
        for traitMapping in traitMappingList:
            trait_mapping = traitMapping.findall("./TraitMapping")
            for trait in trait_mapping:
                if "ClinicalAssertionID" in trait.attrib:
                    assertion_id = trait.attrib["ClinicalAssertionID"]
                else:
                    assertion_id = None
                if assertion_id is not None and assertion_id == id:
                    element = trait.find("./MedGen")
                    if "Name" in element.attrib:
                        name.append(element.attrib["Name"])
                    else:
                        name.append(None)
    else:
        for trait in clinicalAssertion.findall("./TraitSet"):
            print(trait)
            if trait.find(".//Name") is not None:
                phenotype = trait.find(".//Name/ElementValue").text
                name.append(phenotype)
            else:
                name.append(None)

    return name

def get_assertion_method(clinicalAssertion):
    attributeSet_list = clinicalAssertion.findall("./AttributeSet")
    assertion_criteria = None

    if attributeSet_list is not None:
        for attributeSet in attributeSet_list:
            attributes = attributeSet.findall("./Attribute")
            for attribute in attributes:
                if "Type" in attribute.attrib:
                    type = attribute.attrib["Type"]
                else:
                    type = None
                if type is not None and type == "AssertionMethod":
                    assertion_criteria = attribute.text
                else:
                    assertion_criteria = None
    return assertion_criteria

#### ---------- TRANSFORMATION ---------
#performs the necessary transformations to the data coming from the clinvar database (transformations are applied "by hand" by the domain expert)
def transform_data_from_clinvar(user_id, hdfs_path, mapped_file_path):
    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'
    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("transform data") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    messages_result = []

    #read mapped file
    df = spark.read.option("header",True).option("delimiter",";").format("csv").load(hdfs_path+mapped_file_path)

    #TRANSFORMATIONS
    #variant_name
    df = df.withColumn("variant_name", F.when((F.col("start").isNotNull()) & (F.col("end").isNotNull()) & (F.col("ref").isNotNull()) & (F.col("alt").isNotNull()),
                                              F.concat(F.lit("Chr"),F.col("chromosome"),F.lit(":g."),F.col("start"),F.lit("-"),F.col("end"),F.lit(":"),F.col("ref"),
                                                       F.lit(">"),F.col("alt"),F.lit("("),F.col("assembly"),F.lit(")"))) \
                       .otherwise(F.concat(F.lit("Chr"),F.col("chromosome"),F.lit(":ImpreciseVariant"))))

    #variant_id
    df = df.withColumn("variant_id", F.when(F.col("variant_id").isNull(), F.col("clinvar_accession")).otherwise(F.col("variant_id")))

    #phenotype
    df = df.withColumn('phenotype', F.regexp_replace('phenotype', ',', ''))

    #clinical_actionability
    df = df.withColumn("clinical_actionability",
                       F.when((F.lower(F.col('clinical_significance')) == "pathogenic") | (F.lower(F.col('clinical_significance')) == "pathologic") |
                              (F.lower(F.col('clinical_significance')) == "likely pathogenic") | (F.lower(F.col('clinical_significance')) == "risk factor") |
                              (F.lower(F.col('clinical_significance')) == "affects") | (F.lower(F.col('clinical_significance')) == "association"),"Disorder causing or risk factor") \
                       .when((F.lower(F.col('clinical_significance')) == "benign") | (F.lower(F.col('clinical_significance')) == "likely benign") |
                             (F.lower(F.col('clinical_significance')) == "association not found") | (F.lower(F.col('clinical_significance')) == "protective"),"Not disorder causing or protective effect") \
                       .when((F.lower(F.col('clinical_significance')) == "drug response") | (F.lower(F.col('clinical_significance')) == "confers sensitivity"),"Affects drugs or treatment response") \
                       .when((F.lower(F.col('clinical_significance')) == "vus") | (F.lower(F.col('clinical_significance')) == "uncertain significance") |
                             (F.lower(F.col('clinical_significance')) == "conflicting data from submitters"),"Uncertain role") \
                       .otherwise("Not provided"))

    #start
    ##example: NC_000001.10:G.11076931C>T   ----> 11076931
    df = df.withColumn("start", \
                       F.when(F.col("start").rlike("g\.([0-9]+)"), F.regexp_extract("start", "g\.([0-9]+)", 1)).otherwise(F.col("start")))

    #end
    ##example: NC_000001.10:G.11076931C>T   ----> 11076931
    df = df.withColumn("end", \
                       F.when(F.col("end").rlike("g\.([0-9]+)"), F.regexp_extract("end", "g\.([0-9]+)", 1)).otherwise(F.col("end")))

    #ref
    ##example: NC_000019.10:15192004:A:G  ----> A
    df = df.withColumn("ref", \
                       F.when(F.col("ref").rlike(":([a-zA-Z]):"), F.regexp_extract("ref", ":([a-zA-Z]):", 1)).otherwise(F.col("ref")))

    #alt
    ##example: NC_000019.10:15192004:A:G  ----> G
    df = df.withColumn("alt", \
                       F.when(F.col("alt").rlike("([a-zA-Z])$"), F.regexp_extract("alt", "([a-zA-Z])$", 1)).otherwise(F.col("alt")))

    #risk_allele
    ##example: NC_000019.10:15192004:A:G  ----> G
    df = df.withColumn("risk_allele", \
                       F.when(F.col("risk_allele").rlike("([a-zA-Z])$"), F.regexp_extract("alt", "([a-zA-Z])$", 1)).otherwise(F.col("risk_allele")))

    #chromosome
    ##example: 19p13.12 ----> 19
    df = df.withColumn("chromosome", \
                       F.when(F.col("chromosome").rlike("(^[^pq]*)"), F.regexp_extract("chromosome", "(^[^pq]*)", 1)) \
                       .otherwise(F.col("chromosome")))

    ##example: NC_000023.10:g.32407761G>A (Feb.2009: h19, GRCh37) ----> 23     (from VariationName)
    ##       : NC_000019.10:g.1041829A>T ----> 19     (from Expression)
    df = df.withColumn("chromosome", \
                       F.when(F.col("chromosome").rlike("NC_0+([0-9]+)."), F.regexp_extract("chromosome", "NC_0+([0-9]+).", 1)) \
                       .otherwise(F.col("chromosome")))

    #save the new dataframe in the staging area
    df.repartition(1).write.format('csv').option('header',True).mode('overwrite').option('sep',';').save(hdfs_path+"/delfos_platform/"+user_id+"/staging_area/transformed_files/transformed_file_1.csv")

    return messages_result, spark.stop()

# Upload local files - quality rules tab
@app.route('/home/external/quality_rule', methods=["GET", "POST"])
def quality_rule_external_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])
        if request.method == 'POST':
            files = request.files.getlist("qualityRules")
            for file in files:
                if file.filename in client.list('/delfos_platform/'+user_id+'/raw_data/quality_rules/'):
                    flash("You must change the name of the file: " + file.filename)
                else:
                    filename = secure_filename(file.filename)
                    basedir = os.path.abspath(os.path.dirname(__file__))
                    temp_path = os.path.join(basedir, '/tmp/', filename)
                    file.save(temp_path)
                    hdfs_path = "/delfos_platform/" + user_id + "/raw_data/quality_rules/"
                    path_files = os.path.join(hdfs_path, filename)
                    client.upload(path_files, temp_path)
                    os.remove(temp_path)
        return render_template("quality_rule_external.html", id=session['id'])
    return render_template('login.html')

# Show all jobs and it is possible to apply them quality rules - Results tab
@app.route('/home/external/results', methods=["GET", "POST"])
def external_results_tab():
    if 'loggedin' in session:
        user_id = 'user' + str(session['id'])

        jobs_details = jobs_info(user_id)
        jobs_names = jobs_details[0]
        total_paths = jobs_details[1]
        dates = jobs_details[2]
        times = jobs_details[3]

        search_details = search_info(total_paths)
        types = search_details[0]
        parameters = search_details[1]
        values = search_details[2]

        quality_rules = table_quality_rules(user_id)
        names_list = quality_rules[0]
        paths_list = quality_rules[1]


        if request.method == 'POST':
            hdfs_path = "hdfs://hdfs-nn:9000"
            #parameter = request.form["parameterName"]
            #value = request.form["value"]
            selectJob = request.form['download']
            checkboxQR = request.form.getlist("checkboxQR")

            tmp = os.path.abspath('result')
            job_path = os.path.dirname(os.path.dirname(selectJob))+"/"
            treated_file_csv_path = job_path+"treated_file.csv"
            treated_file_json_path = job_path+"treated_file.json"


            if len(checkboxQR) == 0:
                if os.path.exists(tmp):
                    shutil.rmtree(tmp)
                    os.remove("result.zip")

                #download to the app basedir
                client.download(selectJob, tmp, overwrite=True)
                #create zip
                #use send_file function
                #remove result from hdfs and from app
                shutil.make_archive("result","zip",tmp)

                return send_file("result.zip", mimetype="zip", download_name="result.zip", as_attachment=True)
            else:
                #create a date and time variable
                tz_Portugal = pytz.timezone('Portugal')
                date_time = datetime.datetime.now(tz_Portugal).strftime("%d_%m_%Y %H_%M_%S")

                download_path = get_results_external(selectJob, checkboxQR, job_path, treated_file_csv_path, treated_file_json_path, date_time)
                download_path = download_path[0]

                if os.path.exists(tmp):
                    shutil.rmtree(tmp)
                    os.remove("result.zip")

                #download to the app basedir
                client.download(download_path, tmp, overwrite=True)
                #create zip
                #use send_file function
                #remove result from hdfs and from app
                shutil.make_archive("result","zip",tmp)

                return send_file("result.zip", mimetype="zip", download_name="result.zip", as_attachment=True)
        return render_template("external_results_tab.html", id=session['id'], jobs=zip(jobs_names, dates, times, types, parameters, values, total_paths), data_QR=zip(names_list, paths_list))
    return render_template('login.html')

'''
if the user selects quality rules, applies those quality rules to the processed file he or she has selected and then downloads the result.
if the user don't select any quality rules, you only download the processed file
'''
def get_results_external(selectJob, checkboxQR, job_path, treated_file_csv_path, treated_file_json_path, date_time):
    hdfs_path = "hdfs://hdfs-nn:9000"

    #spark session
    warehouse_location ='hdfs://hdfs-nn:9000/delfos_platform'

    spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("apply quality rules in external databases") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .getOrCreate()

    #read treated file in csv format
    treated_file = spark.read.options(header='True', delimiter=';').csv(hdfs_path+treated_file_csv_path)

    #create a new file with quality rules that user chose - all info about the job and the quality rules
    df_info = spark.read.option("header",True).option("delimiter",",").format("csv").load(hdfs_path+selectJob+'/search_info.csv')

    sources = df_info.select('sources').collect()[0].sources
    type = df_info.select('type').collect()[0].type
    filenames = df_info.select('filenames/phenotype').collect()[0][0]
    parameter = df_info.select('parameter').collect()[0].parameter
    value = df_info.select('value').collect()[0].value
    quality_rules_names = []

    if isinstance(checkboxQR, list):
        if len(checkboxQR) > 1:
            for quality_rule in checkboxQR:
                quality_rules_names.append(os.path.basename(quality_rule))
        elif len(checkboxQR) == 1:
            quality_rules_names = checkboxQR[0]
            quality_rules_names = os.path.basename(quality_rules_names)
        else:
            quality_rules_names = ""
    else:
        quality_rules_names = checkboxQR

    with open('search_info.csv', 'w', newline='') as csvfile:
        fieldnames = ['sources', 'type', 'filenames/phenotype', 'parameter', 'value', 'quality_rules']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({'sources': str(sources),'type': str(type), 'filenames/phenotype': str(filenames), 'parameter': str(parameter), 'value': str(value), 'quality_rules': str(quality_rules_names)})

    #upload info about the search
    client.upload(job_path+'result/'+date_time+'/search_info.csv', 'search_info.csv')
    os.remove('search_info.csv')

    #apply quality rules and upload in the right place in hdfs
    apply_quality_rules(hdfs_path, treated_file, checkboxQR, job_path, date_time)

    client.download(treated_file_json_path, "treated_file.json")
    client.upload(job_path+'result/'+date_time+'/treated_file.json', 'treated_file.json')
    os.remove('treated_file.json')

    download_path = job_path+'result/'+date_time

    return download_path, spark.stop()

@app.route("/delete",methods=["POST","GET"])
def delete():
    if request.method == 'POST':
        getid = request.form['string']
        client.delete(getid, recursive=True)
    return None

@app.route("/delete_raw_data",methods=["POST","GET"])
def delete_raw_data():
    if request.method == 'POST':
        getid = request.form['raw_data']
        path = os.path.dirname(getid)
        client.delete(path, recursive=True)
    return None

if __name__ == "__main__":
    app.secret_key = "1234567delfosplatform"
    # portainer
    app.run(host='0.0.0.0', debug=True)
    # local
    #app.run(host='localhost', debug=True)
