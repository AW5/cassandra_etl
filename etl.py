#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 23:38:40 2019

@author: anthonywa
"""

# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
import sql_queries as sq


#function to find and process data in the folder
def process_data_in_folder(filepath):
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
  
    
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = [] 
    
    # for every filepath in the file path list 
    for f in file_path_list:

        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

        # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line) 
            
    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                         'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def dump_data(session):
    
    #for query 1
    file = 'event_datafile_new.csv'
    session.execute(sq.table_1)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
            query = "INSERT INTO query1(firstname,lastname,song,artist,length,itemInSession, sessionId)"
            query = query + "VALUES(%s,%s,%s,%s,%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            session.execute(query, (line[1],line[4],line[9],line[0],line[5],line[3],line[8]))
            
    #for query 2

    session.execute(sq.table_2)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
            query = "INSERT INTO query2(firstname,lastname,song,artist,length,itemInSession, sessionId,userid)"
            query = query + "VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            session.execute(query, (line[1],line[4],line[9],line[0],line[5],line[3],line[8],line[10]))
    
    
    
    session.execute(sq.table_3)
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
        ## TO-DO: Assign the INSERT statements into the `query` variable
            query = "INSERT INTO query3(firstname,lastname,song)"
            query = query + "VALUES(%s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
            session.execute(query, (line[1],line[4],line[9]))
                    
def test(session):
    print('query 1 result')
    #query1
    try:
        rows = session.execute(sq.query_1)
    except Exception as e:
        print(e)
    
    for row in rows:
        print (row.song, row.artist,row.length)
        
    print('\nquery 2 result')
    
    ## query2

    try:
        rows = session.execute(sq.query_2)
    except Exception as e:
        print(e)

    for row in rows:
        print (row.artist,row.firstname, row.lastname, row.song)

    print('\nquery 3 result')
    
    ## query3

    try:
        rows = session.execute(sq.query_3)
    except Exception as e:
        print(e)

    for row in rows:
        print (row.firstname, row.lastname)
                

    
def main():
      
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'
    
    ##connect first
    # This should make a connection to a Cassandra instance your local machine 
    # (127.0.0.1)
    from cassandra.cluster import Cluster
    cluster = Cluster()
    
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    
    # TO-DO: Create a Keyspace 
    try:
        session.execute('''
        CREATE KEYSPACE IF NOT EXISTS ac_hw 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }'''
                       )
    except Exception as e:
        print(e)
    
    # TO-DO: Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('ac_hw')
    except Exception as e:
        print(e)
    
    
    process_data_in_folder(filepath)
    
    dump_data(session)
    test(session)
    
    session.shutdown()
    cluster.shutdown()
    
if __name__ == "__main__":
    main()
    