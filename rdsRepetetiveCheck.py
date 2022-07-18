import json
import boto3
import mysql.connector
from mysql.connector import Error
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

    def selectLast15(db):
        self.cursor.execute("SELECT COUNT(NULLIF(Resp_Code,0)) FROM "+db +"where currentTimestamp > sysdate - (15/1440)")
        result = self.cursor.fetchall()
        print(result)
        return result

    

    def disconnectCursor():
        try:
            self.cursorcursor.close()
        except Error as e:
            print("Error while closing cursor", e)

    def disconnectConnection(connection):     
       if self.connection.is_connected():
            self.cursorconnection.close()
            print("MySQL connection is closed")


class messenger:
    def sendEmail(failedTransactions):
        notification = "The number of failed transactions are" +str(failedTransactions)
        client = boto3.client('sns')
        response = client.publish (
            TargetArn = "<ARN of the SNS topic>",
            Message = json.dumps({'default': notification}),
            MessageStructure = 'json'
        )
def main():
    query=sqlQuery()
    message=messenger()
    query.createConnection("postgres.cedt1mj8y7vr.ap-south-1.rds.amazonaws.com","postgres","admin","postgres")
    query.createCursor()
    result=query.selectLast15()
    message.sendEmail(result)


