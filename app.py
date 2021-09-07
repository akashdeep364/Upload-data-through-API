from flask import Flask, render_template, request, jsonify
import mysql.connector as connection
import pymongo
import csv
from cassandra.cluster import Cluster



app = Flask(__name__)
app.debug = True

@app.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('index.html')

@app.route('/db_opeartions', methods=['POST'])  # This will be called from UI
def math_operation():
    if (request.method=='POST'):
        database = request.form['db']
        operation = request.form['operation']
        if database == 'mysql':
            mydb = connection.connect(host="127.0.0.1", user="root", passwd="mysql", use_pure=True, database='ineuron_task')
            cursor = mydb.cursor()  # create a cursor to execute queries

            if(operation=='create'):
                try:
                    cursor.execute("CREATE TABLE if not exists Student (student_id INT(10) , first_name VARCHAR(255), registeration_date DATE, address VARCHAR(255))")
                    cursor.execute('desc Student')
                    result = cursor.fetchall()

                except Exception as e:
                    mydb.close()
                    print(str(e))
                return render_template('results.html',result=result)

            if (operation == 'insert'):
                try:
                    cursor.execute("insert into Student VALUES (1, 'Akash','2021-02-12','cuttack')")
                    cursor.execute("insert into Student VALUES (2, 'Pankaj','2021-02-13','Mumbai')")
                    cursor.execute("insert into Student VALUES (3, 'Spandan','2021-02-14','Shirdi')")
                    cursor.execute("insert into Student VALUES (4, 'SAyali','2021-02-15','Coimbature')")
                    cursor.execute("insert into Student VALUES (5, 'Pooja','2021-02-16','ooty')")
                    mydb.commit()
                    cursor.execute('select * from student')
                    result1 = cursor.fetchall()
                    cursor.execute('desc student')
                    result = cursor.fetchall()

                except Exception as e:
                    mydb.close()
                    print(str(e))
                return render_template('results1.html',result=result,result1 = result1)


            if (operation == 'update'):
                try:
                    cursor.execute('update Student set first_name="Pratik" where student_id = 2')
                    cursor.execute('select * from student')
                    result1 = cursor.fetchall()
                    cursor.execute('desc student')
                    result = cursor.fetchall()

                except Exception as e:
                    mydb.close()
                    print(str(e))
                return render_template('results1.html',result=result,result1 = result1)

            if (operation == 'delete'):
                try:
                    cursor.execute('delete from Student where student_id = 3')
                    cursor.execute('select * from student')
                    result1 = cursor.fetchall()
                    cursor.execute('desc student')
                    result = cursor.fetchall()

                except Exception as e:
                    mydb.close()
                    print(str(e))
                return render_template('results1.html', result=result, result1=result1)

            if (operation == 'bulk_insert'):
                try:
                    with open('D://Upload data through API/carbon_nanotubes.csv', 'r') as csvfile:
                        csvreader = csv.reader(csvfile, delimiter=';')

                        # Read from line 2, skip column names
                        header = next(csvreader)

                        # Create table
                        cursor.execute('create table IF NOT EXISTS carbon (c1 int(5),c2 int(5),c3 varchar(10),c4 varchar(10),c5 varchar(10),c6 varchar(10),c7 varchar(10),c8 varchar(10))')

                        for row in csvreader:
                            row[0], row[1] = int(row[0]), int(row[1])
                            row = str(row).replace('[', '').replace(']','')  # to make it in the correct form for insert
                            cursor.execute(f'INSERT INTO carbon values ({row})')
                            mydb.commit()
                        print("Values inserted sucessfully!!")
                        cursor.execute('select * from carbon')
                        result1 = cursor.fetchall()
                        cursor.execute('desc carbon')
                        result = cursor.fetchall()

                        cursor.close()
                        mydb.close()
                except Exception as e:
                    print(str(e))
                return render_template('results1.html', result=result, result1=result1)

            if (operation == 'download'):
                try:
                    cursor.execute('select * from carbon')

                    rows = cursor.fetchall()

                    f = open('download1_mysql_data.csv', 'w')
                    myFile = csv.writer(f)
                    myFile.writerows(rows)
                    f.close()

                except Exception as e:
                    print(str(e))
                return render_template('results.html',result1 = 'File downloaded successfully!!!')


        if database == 'mongodb':
            DEFAULT_CONNECTION_URL = 'mongodb://localhost:27017/'
            pymongo.MongoClient(DEFAULT_CONNECTION_URL)

            if(operation=='create'):
                DB_NAME = 'ineuron1'
                client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                database = client[DB_NAME]
                collection_name = 'test'
                collection = database[collection_name]
                return render_template('results.html',result1 = f'database {DB_NAME} and collection {collection_name} have been created ')

            if(operation == 'insert'):
                DB_NAME = 'ineuron1'
                client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                database = client[DB_NAME]
                collection_name = 'test2'
                collection = database[collection_name]
                try:
                    record = [{"_id": "1",
                               "companyName": "iNeuron",
                               "Faculty": "abcd efg"},
                               {"_id": "2",
                                "companyName": "iNeuron",
                                "Faculty": "pqrs tuv"},
                              {"_id": "3",
                               "companyName": "iNeuron",
                               "Faculty": "Sudhanshu Kumar"},
                              {"_id": "4",
                               "companyName": "iNeuron",
                               "Faculty": "Krish Naik"},
                             ]

                    faculties_record = collection.insert_many(record)
                    result = ''
                    for i in collection.find({'_id': {'$gt': '0'}}):
                        result += str(i)

                except Exception as e:
                    print(str(e))
                return render_template('results.html',result1 = result)

            if(operation == 'delete'):
                DB_NAME = 'ineuron1'
                client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                database = client[DB_NAME]
                collection_name = 'test2'
                collection = database[collection_name]

                query = {'_id': '4'}
                collection.delete_one(query)

                result = ''
                for i in collection.find({'_id': {'$gt': '0'}}):
                    result += str(i)

                return render_template('results.html',result1 = result)

            if(operation == 'update'):
                DB_NAME = 'ineuron1'
                client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                database = client[DB_NAME]
                collection_name = 'test2'
                collection = database[collection_name]

                current = {'Faculty': 'abcd efg'}
                new = {'$set': {'Faculty': 'Krish Naik'}}

                collection.update_one(current, new)

                result = ''
                for i in collection.find({'_id': {'$gt': '0'}}):
                    result += str(i)

                return render_template('results.html',result1 = result)

            if (operation == 'bulk_insert'):
                DB_NAME = 'ineuron1'
                client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                database = client[DB_NAME]
                collection = database['test1']

                with open('D://Upload data through API/carbon_nanotubes.csv') as file:
                    collection.insert_many(
                        ({"no": line.strip()} for line in file if line),ordered=False)

                result = ''
                for i in collection.find():
                    result += str(i)

                return render_template('results.html',result1 = result)

            if (operation == 'download'):
                try:
                    DB_NAME = 'ineuron1'
                    client = pymongo.MongoClient(DEFAULT_CONNECTION_URL)
                    database = client[DB_NAME]
                    collection = database['test1']

                    cursor = collection.find({}, {'_id': 1, 'no': 1})

                    with open('download2_mongodbdata.csv', 'w') as outfile:
                        fields = ['id', 'data']
                        write = csv.DictWriter(outfile, fieldnames=fields)
                        write.writeheader()
                        for answers_record in cursor:  # Here we are using 'cursor' as an iterator
                            answers_record_id = answers_record['_id']
                            answers_record_no = answers_record['no']
                            flattened_record = {
                                    'id': answers_record_id,
                                    'data': answers_record_no,
                                }
                            write.writerow(flattened_record)

                except Exception as e:
                    print(str(e))
                return render_template('results.html',result1 = 'File downloaded successfully!!!')


        if database == 'cassandra':
            cluster1 = Cluster(['127.0.0.1'])
            session1 = cluster1.connect()
            row = session1.execute("select release_version from system.local").one()

            if operation == 'create':
                row = session1.execute("CREATE KEYSPACE IF NOT EXISTS ineuron2 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };").one()
                session1.execute(" CREATE TABLE IF NOT EXISTS ineuron2.customer(id int PRIMARY KEY,first_name text,last_name text,phone varint);").one()

                return render_template('results.html', result1= 'Keyspace ineuron2 and table customer created successfully!!!')

            if operation == 'insert':
                session1.execute("insert into ineuron2.customer(id,first_name,last_name,phone) values (1,'Akash','Deep',123456789);").one()
                session1.execute("insert into ineuron2.customer(id,first_name,last_name,phone) values (2,'Spandan','Rakshe',1234567896);").one()
                session1.execute("insert into ineuron2.customer(id,first_name,last_name,phone) values (3,'Pankaj','Rakshe',4444567898);").one()
                session1.execute("insert into ineuron2.customer(id,first_name,last_name,phone) values (4,'Sayali','Kadam',1678678971);").one()
                session1.execute("insert into ineuron2.customer(id,first_name,last_name,phone) values (5,'Rashmi','Sachdev',1124567891);").one()

                rows = session1.execute('select * from ineuron2.customer;')
                return render_template('results1.html',result1 = rows)

            if operation == 'update':
                session1.execute("update ineuron2.customer set last_name = 'aaaaaaaaaa' where id = 1")

                rows = session1.execute('select * from ineuron2.customer;')
                return render_template('results1.html',result1 = rows)

            if operation == 'delete':
                session1.execute('delete from ineuron2.customer where id = 4;')
                rows = session1.execute('select * from ineuron2.customer;')
                return render_template('results1.html',result1 = rows)

            if operation == 'bulk_insert':
                with open('D://Upload data through API/carbon_nanotubes.csv', 'r') as csvfile:
                    csvreader = csv.reader(csvfile, delimiter=';')

                    # Read from line 2, skip column names
                    header = next(csvreader)

                    # Create table
                    session1.execute('create table IF NOT EXISTS ineuron2.carbon (id int primary key,c1 int,c2 int,c3 text,c4 text,c5 text,c6 text,c7 text,c8 text);')
                    count = 1
                    for row in csvreader:
                        row[0], row[1] = int(row[0]), int(row[1])
                        row = str(row).replace('[', '').replace(']', '')  # to make it in the correct form for insert
                        row = str(count) + ',' + row
                        session1.execute(f'INSERT INTO ineuron2.carbon(id,c1,c2,c3,c4,c5,c6,c7,c8) values ({row});')
                        count += 1

                rows = session1.execute('select * from ineuron2.carbon;')
                return render_template('results1.html',result1 = rows)

            if (operation == 'download'):
                try:
                    rows = session1.execute('select * from ineuron2.carbon;')

                    f = open('download3_cassandra_data.csv', 'w')
                    myFile = csv.writer(f)
                    myFile.writerows(rows)
                    f.close()

                except Exception as e:
                    print(str(e))
                return render_template('results.html',result1 = 'File downloaded successfully!!!')

if __name__ == '__main__':
    app.run()
