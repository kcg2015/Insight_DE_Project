#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility functions related to PostgreSQL database

@author: kyleguan
"""
import psycopg2
import sys
from psycopg2 import sql

def create_database(db_name, tb_name):
    """
    Create a PostgeSQL table

    param: db_name: database name
           tb_name: database table name
    return:  none
    """
    
    con = None
    connect_str= "host='localhost'"+" dbname='"+db_name+"' user='data_engineer' password='kcguan'"
    try:
        con = psycopg2.connect(connect_str)   
        cur = con.cursor()
        
        cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}(\
                            id INTEGER PRIMARY KEY, \
                            image_id  VARCHAR(20), \
                            img_w INTEGER,\
                            img_h INTEGER,\
                            model_name  VARCHAR(20), \
                            model_ver  VARCHAR(20),\
                            ts   VARCHAR(30),\
                            det INT[],\
                            label INT[],\
                            iou REAL,\
                            conf REAL,\
                            color VARCHAR(20), \
                            succ BOOLEAN)").format(sql.Identifier(tb_name)))
 
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
     
        print('Error %s' % e)    
        sys.exit(1)
     
    finally:   
        if con:
            con.close()
            
            
def insert_database(db_name, tb_name, row):
    """
    Insert a row into a PostgeSQL table

    param: db_name: database name
           tb_name: database table name
           row:   a row of a table
    return:  none
    """
    con = None
    connect_str= "host='localhost'"+" dbname='"+db_name+"' user='data_engineer' password='kcguan'"
    try:
        con = psycopg2.connect(connect_str)   
        cur = con.cursor()
        
        cur.execute(sql.SQL("insert into {} \
                            values (%s, %s, %s, \
                            %s, %s, %s,\
                            %s, %s, %s,\
                            %s, %s, %s, %s) on conflict(id) do nothing;")
        .format(sql.Identifier(tb_name)), row)  
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
     
        print('Error %s' % e)    
        sys.exit(1)
     
    finally:   
        if con:
            con.close()    
            
def print_database(db_name, tb_name):
    
    """
    Print all the rows of a table

    param: db_name: database name
           tb_name: database table name
           
    return:  none
    """
    con = None
    connect_str= "host='localhost'"+" dbname='"+db_name+"' user='data_engineer' password='kcguan'"
    try:
        con = psycopg2.connect(connect_str)   
        cur = con.cursor()
        cur.execute(sql.SQL("select * from {}").format(sql.Identifier(tb_name)))
        while True:
                row = cur.fetchone()
                if row == None:
                    break
         
                print(row)
        con.commit()
    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        print('Error %s' % e)    
        sys.exit(1)
     
    finally:   
        if con:
            con.close()    