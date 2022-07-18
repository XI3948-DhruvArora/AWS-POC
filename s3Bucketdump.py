import datetime
import mysql.connector
from mysql.connector import Error
import csv
class readFile:

  def readCsv(event):
    bucket=event['Records'][0]['s3']['bucket']['name']
    csv_file=event['Records'][0]['s3']['object']['key']
    csv_file_obj=s3_cilent.get_object(Bucket=bucket,Key=csv_file)
    lines=csv_file_obj['Body'].read().decode('utf-8').splitlines()
    bucketData=[]
    dateTime= datetime.datetime.now()
    for row in csv.DictReader(lines):
        bucketData.append(row.values())
    return bucketData

class transformer:
    def addTimesttamp(bucketData):
        flag=True
        transformedData=[]
        for row in bucketData:
            if flag:
                row["currentTimestamp"]="currentTimeStamp"
                flag=False
            else:
                row["currentTimestamp"]=dateTime
                transformedData.append(row.value())
        return transformedData

class sqlQuery:
    connection=""
    cursor=""
    def createConnection(local_host,db,usr,pswd,self):
        try:
            self.connection = mysql.connector.connect(host=local_host,database=db,user=usr,password=pswd)
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                return self.connection
        except Error as e:
            print("Error while connecting to MySQL", e)
    
    def createCursor():
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute("select database();")
            record = self.cursor.fetchone()
            print("You're connected to database: ", record)
            
        except Error as e:
            print("Error while creating cursor", e)

    def selectAll(db):
        self.cursor.execute("SELECT * FROM "+db)
        result = self.cursor.fetchall()
        print(result)

    def executeQuery(query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(result)

    def disconnectCursor():
        try:
            self.cursorcursor.close()
        except Error as e:
            print("Error while closing cursor", e)

    def disconnectConnection(connection):     
       if self.connection.is_connected():
            self.cursorconnection.close()
            print("MySQL connection is closed")

    def insertData(results):
        mysql_insert = "insert into details(Jv_Date,Jv_AMount,Server_Name,Resp_Code,Switch_Id,Switch_Card_Type,First_6_Digit_Of_Card,Cust_Terminal_Code,currentTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        self.cursor = connection.cursor()
        self.cursor.executemany(mysql_insert, results)
        connection.commit()
def main():
    read=readFile()
    transform=transformer()
    query=sqlQuery()
    query.createConnection("postgres.cedt1mj8y7vr.ap-south-1.rds.amazonaws.com","postgres","admin","postgres")
    query.createCursor()
    bucketData=read.readCsv()
    results=transform.addTimesttamp(bucketData)
    query.insertData(results)

