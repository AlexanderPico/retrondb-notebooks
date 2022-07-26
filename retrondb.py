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
from tabulate import tabulate
import pandas as pd
import os
import getpass
import json

#CONSTANTS
ansiRed = "\033[91m {}\033[00m"
ansiGreen = "\033[92m {}\033[00m"
ansiBlue = "\033[94m {}\033[00m"

#FUNCTIONS
def get_retronDB(db_name='retronDB'):
    """
    Utility function to connect to the retron databases hosted by MongoDB Atlas.
    
    Parameters
    ----------
    db_name: Name of the database to connect to, e.g., 'sandbox'. Default is
    'retronDB'.
    
    Returns
    -------
    A database collection object intended to be used as the "rdb_handle" 
    parameter in other functions.
    
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
    
def get_all_retrons(rdb_handle=None):
    """
    Returns a pandas DataFrame of all fields (columns) for all retrons (rows).

    Parameters
    ----------
    rdb_handle: Output of get_retronDB()
    
    Returns
    -------
    DataFrame
    
    """
    res = rdb_handle.find()
    return(pd.DataFrame(list(res)))
    
    
def get_retron(rdb_handle=None, id=None, format="df"):
    """
    Returns a single retron as either a JSON string, Python dictionary or 
    Pandas DataFrame (default).

    Parameters
    ----------
    rdb_handle : Output of get_retronDB()
    id : Unique retron record _id
    format : Either "json", "dict" or "df" (default)
    
    Returns
    -------
    JSON (str), dictionary, or DataFrame
    
    """
    res = rdb_handle.find({"_id":str(id)})
    format = format.lower()
    if format == "json":
        return(json.dumps(list(res)[0]))
    elif format == "dict":
        return(json.loads(json.dumps(list(res)[0])))
    else: #DataFrame
        return(pd.DataFrame(list(res)))
    
def get_retrons_by(rdb_handle=None, key="_id", values=None):
    """
    Returns one or more retrons by a particular key and value. The default
    key is "_id".
    
    Parameters
    ----------
    rdb_handle : Output of get_retronDB()
    key : Property key or name, e.g., "genus"
    value : Propery value, e.g., "Escherichia"
    
    Returns
    -------
    DataFrame
    
    """
    res = rdb_handle.find({str(id):str(values)})
    return(pd.DataFrame(list(res)))


def add_retron(json_str=None, new_property=False, update=False):
    """
    Add a single retron from a JSON string. The JSON must include a unique
    identifier key called "_id" with a string-encoded integer, e.g., "_id":"0".
    All properties will be checked against current database properties. By 
    default, preexisting retrons will not be updated; conflicting entries will 
    be rejected (see update parameter), and unrecognized properties will be 
    rejected (see new_property parameter).

    Parameters
    ----------
    json_str : A string of JSON data
    new_property : (Boolean) Whether to accept novel properties. Default is False.
    update : (Boolean) Whether to update records with same _id. Default is False.

    Returns
    -------
    None.

    """

def add_retrons_by_csv(csv_file=None, new_property=False, update=False):
    """
    Add one or more retrons from a CSV file. The first column must contain a 
    unique integer identifier and be named "_id" or "node", otherwise unique
    identifiers will be automatically assigned. By default, preexisting retrons
    will not be updated; conflicting entries will be rejected (see update parameter), 
    and unrecognized properties will be rejected (see new_property parameter).

    Parameters
    ----------
    csv_file : Path to CSV file
    new_property : (Boolean) Whether to accept novel properties. Default is False.
    update : (Boolean) Whether to update records with same _id. Default is False.
    
    Returns
    -------
    None

    """
    

 
# INTERNAL FUNCTIONS
# def print_table(result, doc_keys=None, headers=None):
#     """
#     Utility function to print a query result as a table.
#     Params:
#         doc_keys : A list of keys for data to be extracted from each document.
#         result : A MongoDB cursor.
#         headers : A list of headers for the table. If not provided, attempts to
#             generate something sensible from the provided `doc_keys`
#     """
#     if doc_keys is None:
#         doc_keys = set()
#         for doc in result:
#             doc_keys.update(set(doc.keys()))
#     doc_keys = list(doc_keys)
    
#     if headers is None:
#         headers = [key.replace("_", " ").replace("-", " ").title() for key in doc_keys]
   
#     records = (extract_tuple(doc, doc_keys) for doc in result)
#     print(tabulate(records, headers=headers))


# def extract_tuple(mapping, keys):
#     """
#     Extract a tuple from a mapping by requesting a sequence of keys.
    
#     Missing keys will result in `None` values in the resulting tuple.
#     """
#     return tuple([mapping.get(key) for key in keys])
    