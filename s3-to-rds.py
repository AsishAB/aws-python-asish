import os
import boto3
from dotenv import load_dotenv
import csv
load_dotenv()
import mysql.connector as mysql 
from mysql.connector import Error 
import pandas as pd
# dotenv_path = Path('path/to/.env')
# load_dotenv(dotenv_path=dotenv_path)

aws_access_key_id=os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
aws_region_name=os.getenv('AWS_REGION_NAME')
aws_s3_bucket_source = os.getenv("AWS_S3_SOURCE_BUCKET")

mysql_host=os.getenv('MYSQL_HOST')
mysql_database=os.getenv('MYSQL_DATABASE')
mysql_user=os.getenv('MYSQL_USER')
mysql_password=os.getenv('MYSQL_PASSWORD')

file_path = os.getenv("LOCAL_FILE_PATH")




# s3 = boto3.client(
#     's3',
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key,
#     region_name=aws_region_name,
    
# )

def read_file_from_s3(bucket_name, file_name):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    data = obj['Body'].read()
    return data





# with open(file_path, mode ='r',encoding='utf-8') as file:
#   # reading the CSV file
#   csvFile = csv.reader(file, delimiter=',')
#   # displaying the contents of the CSV file
#   for lines in csvFile:
#         print(lines[1])
#         break

empdata = pd.read_csv(file_path, index_col=False, delimiter = ',') 
empdata.head()

total_no_of_rows = len(empdata.index)
sql_injection_protectors = ''
sql_values=  ''
sql_values_tuple = []

for i,row in empdata.iterrows(): 
    sql_values = (row['game_id'], row['minute'], row['player'], row['team1score'], row['team2score'])
    sql_values_tuple.append(sql_values)

insert_data_into_table_command =f"INSERT INTO tbl_1(game_id,no_of_minute,player_name,team_1_score,team_2_score) VALUES (%s,%s,%s,%s,%s)"


try: 
    conn = mysql.connect(
                        host=mysql_host, 
                        database=mysql_database, 
                        user=mysql_user, 
                        password=mysql_password
    ) 
    if (conn.is_connected()): 
        print("Connection Successful !!")
        cursor = conn.cursor() 
        cursor.executemany(insert_data_into_table_command, sql_values_tuple)
        conn.commit()
    
except Error as e: 
    print("Error !Exception Raised \n")
    print(e)