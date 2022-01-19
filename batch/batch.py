import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error, connect
import boto3
from datetime import datetime, timezone
load_dotenv()
full_date_now = datetime.now()
date_now = full_date_now.strftime("%Y-%m-%d %H:%M:%S")
try:
   connection_dict = {
       'user': os.getenv('DB_USER'),
       'password': os.getenv('DB_PASSWORD'),
       'host': os.getenv('DB_HOST'),
       'database': os.getenv('DB_NAME'),
       'raise_on_warnings': True,
       'use_pure': False,
       'autocommit': True,
       'pool_size': 5
   }
   connection = mysql.connector.connect(**connection_dict)

   if connection.is_connected():
       cursor = connection.cursor()
       cursor.execute("select database();")
       record = cursor.fetchone()
       print("You are connected to the database:", record)
       s3 = boto3.resource(
       's3',
       region_name=os.getenv('BUCKET_REGION'),
        )
       content="Database connection succeeded on " +date_now
       s3.Object(os.getenv('BUCKET_NAME'), os.getenv('LOG_FILE')).put(Body=content)
       print("Database connection is succeeded at ",date_now)
except Error as e:
    print("Error in connecting to the database server", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Database connection closed")
