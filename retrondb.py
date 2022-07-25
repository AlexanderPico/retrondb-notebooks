#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The retrondb module of utility functions for interacting with the retronDB
database hosted on MongoDB Atlas.

Usage: import retrondb

Created on Mon Jul 25 09:58:00 2022
@author: alexpico
"""

from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import os
import getpass

#Constants
ansiRed = "\033[91m {}\033[00m"
ansiGreen = "\033[92m {}\033[00m"

#Functions
def get_retronDB(db_name='retronDB'):
    """
    Utility function to connect to the retron databases hosted by MongoDB Atlas.
    """
    
    # Load config from a .env file or prompt for entries
    load_dotenv(verbose=True)
    try:
        RETRONDB_USR = os.environ['RETRONDB_USR']
    except KeyError:
        RETRONDB_USR = input("Enter username:")
    try:
        RETRONDB_PWD = os.environ['RETRONDB_PWD']
    except KeyError:
        RETRONDB_PWD = getpass.getpass(prompt="Enter password:") 

    # Construct MongoClient URI    
    RETRONDB_URI = str("mongodb+srv://" +
    RETRONDB_USR + ":" +
    RETRONDB_PWD +
    "@cluster0.uuutrha.mongodb.net/?retryWrites=true&w=majority")

    # Connect to our MongoDB cluster
    client = MongoClient(RETRONDB_URI)
    # print(client.database_names())

    # Get the retronDB
    db = client[db_name]
    try:
        db.list_collection_names()
        print("\n" + ansiGreen.format("Success")+": Connected to "+
              db_name + "\n" )
    except:
        print("\n" + ansiRed.format("Error")+": Failed to connect to " +
              db_name + ". Check your username and password (or .env file).\n")
    else:
        # Return the retrons collection
        return(db['retrons'])
    
def get_all_retrons():
    """
    Returns a pandas DataFrame of all fields (columns) for all retrons (rows).
    """

    
    
def get_retron(id=None, format="df"):
    """
    Returns a single retron as either JSON or dictionary or DataFrame (default).
    """

    
def get_retrons_by(key="_id", values=None):
    """
    Returns one or more retrons by a particular key and value. The default
    key is "_id".
    """
    