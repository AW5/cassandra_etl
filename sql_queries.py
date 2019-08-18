#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 23:37:45 2019

@author: anthonywa
"""

## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query_1="SELECT artist, song, length FROM query1 WHERE  itemInSession='4' AND sessionId='338'"

#create table one
table_1='''CREATE TABLE IF NOT EXISTS query1(firstname text,lastname text,song text, 
artist text,length text,itemInSession text,sessionId text, PRIMARY KEY(itemInSession, sessionId))'''

## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

query_2="SELECT artist, song, firstname, lastname FROM query2 WHERE userid='10' AND  sessionId='182'"  

#create table two
table_2='''CREATE TABLE IF NOT EXISTS query2(firstname text,lastname text,
song text, artist text,length text,itemInSession text,sessionId text, userid text, PRIMARY KEY(userid, sessionId,iteminsession))'''

## query3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query_3="SELECT firstname, lastname FROM query3 WHERE song='All Hands Against His Own'"
#create table three
table_3='CREATE TABLE IF NOT EXISTS query3(firstname text,lastname text,song text, PRIMARY KEY(song))'

