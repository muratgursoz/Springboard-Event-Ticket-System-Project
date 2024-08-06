import os
import mysql.connector
import pathlib
import csv
import pandas as pd



# function to connect to local sql server
def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user='root',
        password='root',
        host = 'localhost',
        database = 'tixy')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection

myconn = get_db_connection()

mycursor = myconn.cursor()
#Establish the Ticket Sales Table Structure in MYSQL in accordance with the DDL
mycursor.execute("CREATE TABLE IF NOT EXISTS event_sales (ticket_id INT PRIMARY KEY, trans_date DATE, event_id INT, event_name VARCHAR(50), event_date DATE, event_type VARCHAR(10), event_city VARCHAR(20), customer_id INT, price DECIMAL, num_tickets INT);")
mycursor.close()

# Specify filepath containing ticket sales csv file



def load_third_party(connection, file_path_csv):
    csv_path = pathlib.Path.cwd() / file_path_csv
    cursor = connection.cursor()
    dict_list = list()
    with csv_path.open(mode = "r") as csv_reader:
        csv_reader = csv.reader(csv_reader)
        for rows in csv_reader:
            dict_list.append({'ticket_id':rows[0],
                          'trans_date':rows[1],
                          'event_id':rows[2],
                          'event_name':rows[3],
                          'event_date':rows[4],
                          'event_type':rows[5],
                          'event_city':rows[6],
                          'customer_id':rows[7],
                          'price':rows[8],
                          'num_tickets':rows[9]})

    for i in dict_list:
        que = "INSERT INTO event_sales(ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = i['ticket_id'], i['trans_date'], i['event_id'], i['event_name'], i['event_date'], i['event_type'], i['event_city'], i['customer_id'], i['price'], i['num_tickets']
        cursor.execute(que, val)
    connection.commit()
    cursor.close()
    return

load_third_party(myconn, 'third_party_sales_1.csv')

def query_popular_tickets(connection):
    # Get the most popular tickets in the past month
    poptix_df = pd.read_sql_query("SELECT event_name, SUM(num_tickets) AS total_sales FROM tixy.event_sales GROUP BY event_name ORDER BY SUM(num_tickets) DESC LIMIT 3;", connection)
    print("The top 3 most popular events by August 2020 ticket sales:")
    print(poptix_df)
    return
query_popular_tickets(myconn)
myconn.close()
