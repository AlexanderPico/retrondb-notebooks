#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
The retrondb module of utility functions for interacting with the retronDB
database hosted on MongoDB Atlas.

Usage: import retrondb

Created on Mon Jul 25 09:58:00 2022
@author: alexpico
"""

import pymongo as pm
from dotenv import load_dotenv
from bson import json_util, son
import re
import pandas as pd
import os
import getpass
import json
import functools

#CONSTANTS
ansiRed = "\033[91m {}\033[00m"
ansiGreen = "\033[92m {}\033[00m"
ansiBlue = "\033[94m {}\033[00m"


###############################################################################
# GENERAL FUNCTIONS
def connect_retronDB(db_name='retronDB'):
    """
    A utility function to connect to the retron databases hosted by MongoDB 
    Atlas. This function will create a database if it does not already exist,
    then connects (or creates) a Collection called 'retrons' within that 
    database, and finally returns a ``pymongo.Collections`` obj to serve as a
    convenient handle for other functions (e.g., **rdb_handle**). 
    
    Note that this function will also confirm (or create) a unique index on 
    the "node" property to prevent duplicate records.
    
    Parameters
    ----------
    db_name : ``str``, optional
        Name of the database to connect to, e.g., 'sandbox'. Default is the
        official 'retronDB' database.
    
    Returns
    -------
    Collection obj
        A retron database collection object intended to be used as the 
        **rdb_handle** parameter in other functions.
    
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
    client = pm.MongoClient(RETRONDB_URI)
    # print(client.database_names())

    # Get the retronDB
    db = client[db_name]
    try:
        db.list_collection_names()
    except:
        print("\n" + ansiRed.format("Error")+": Failed to connect to " +
              db_name + ". Check your username and password (or .env file).\n")
    else:
        print("\n" + ansiGreen.format("Success")+": Connected to "+
              db_name + " with " + 
              str(db['retrons'].count_documents({})) + 
              " retrons\n" )
        # Confirm index on node; create if not
        ind_set = False
        for rdb_ind in list(db['retrons'].list_indexes()):
            if son.SON(rdb_ind['key']).to_dict() == {'node':1}:
                ind_set = True
        if not ind_set:
            db['retrons'].create_index("node", unique=True)
        # Return the retrons collection
        return db['retrons']
    
def save_retronDB(rdb_handle=None, filename=None):
    """
    Save the entire retron database to a CSV file in your current working
    directory (See ``os.getcwd()``). Consider using ``os.chdir()`` to change the
    destination prior to running this function, or provide a full path as 
    the **filename** parameter.
    
    Also see ``restore_retronDB()`` to recreate a retron database from a saved 
    file.

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    filename : ``str``
        Full path or path relavtive to current working directory, in addition 
        to the name of the file to be written. The ``.csv`` extension is 
        automatically added. 

    Returns
    -------
    str
        Path to saved retron database file.

    """
    if re.search('.csv$', filename) is None: filename += '.csv'
    if os.path.exists(filename):
        return print(ansiRed.format("Error")+
                     ": This file already exists and cannot be overwritten "+
                     "with this function. Consider using a new filename.")
 
    rdb_df = get_all_retrons(rdb_handle)
    rdb_df.to_csv(filename, index=False,)
    print("Saved retron database to:")
    return print(os.path.abspath(filename))
    
    
def restore_retronDB(filename=None, db_name=None):
    """
    This function will create a new retron database from a previously saved
    CSV file. See ``save_retronDB()``. The returned pymongo.Collections obj is
    the same type returned by ``connect_retronDB()`` and is ready to be used in
    subsequent functions.

    Parameters
    ----------
    filename :  ``str``
        Full path or path relavtive to current working directory, in addition 
        to the name of the file to be read. The ``.csv`` extension is 
        automatically added. 
    db_name : ``str``
        Name of the database to create from file.

    Returns
    -------
    Collection obj
        A retron database collection object intended to be used as the 
        **rdb_handle** parameter in other functions.

    """
    # connect to new database instance
    rdb_handle = connect_retronDB(db_name)
    
    # load data
    if re.search('.csv$', filename) is None: filename += '.csv'  
    #TODO
    
    return rdb_handle
    
    

###############################################################################
# GET FUNCTIONS
def get_all_retrons(rdb_handle=None, format="df"):
    """
    Returns a ``pandas.DataFrame`` of all fields (columns) for all retrons (rows).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    format : ``str``, optional
        Either "raw", "json", "dict", or "df" (default)
    
    Returns
    -------
    str, dict, pandas.DataFrame, or pymongo.Cursor obj
        Retron properties as JSON (str), dictionary, DataFrame, or Cursor
        depending on specified format
    
    """       
    format = format.lower() 
    if format not in ["raw","json","dict","df"]:
        raise ValueError ('format must be "raw", "json", "dict", "df" or empty')
        
    res = rdb_handle.find()
    return format_result(res, format)
    
    
def get_retron(rdb_handle=None, node=None, format="df"):
    """
    Returns a single retron as either a JSON string, Python dictionary or 
    Pandas DataFrame (default).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    node : ``str`` or int(automatically coverted to str)
        Unique retron node ID 
    format : ``str``, optional
        Either "raw", "json", "dict", or "df" (default)
    
    Returns
    -------
    str, dict, pandas.DataFrame, or pymongo.Cursor obj
        Retron properties as JSON (str), dictionary, DataFrame, or Cursor
        depending on specified format
    
    """
    format = format.lower()
    if format not in ["raw","json","dict","df"]:
        raise ValueError ('format must be "raw", "json", "dict", "df" or empty')
        
    res = rdb_handle.find_one({"node":str(node)})
    return format_result(res, format)

    
def get_retrons_by(rdb_handle=None, key="node", value=None, format="df"):
    """
    Returns one or more retrons by a particular **key** and **value**. The default
    key is "node". Value can be a set of conditions using MongoDB query
    comparison operators, e.g., {"$gt":5} or {"$gt":2,"$lt":8} or {"$in":[3,5,7]}
        
    https://www.mongodb.com/docs/v4.4/reference/operator/query-comparison/
    
    * $eq - Matches values that are equal to a specified value.
    * $ne - Matches all values that are not equal to a specified value.
    * $gt - Matches values that are greater than a specified value.
    * $gte - Matches values that are greater than or equal to a specified value.
    * $lt - Matches values that are less than a specified value.
    * $lte - Matches values that are less than or equal to a specified value.
    * $in - Matches any of the values specified in an array. Sensitive to type.
    * $nin - Matches none of the values specified in an array. Sensitive to type.
    
    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    key : ``str``, optional
        Property key or name, e.g., "genus"
    value : ``str``, ``int``, ``float`` or ``set``
        Propery value, e.g., "Escherichia" or set of conditions, e.g., {"$gt":5}
    format : ``str``, optional
        Either "raw", "json", "dict", or "df" (default)
    
    Returns
    -------
    str, dict, pandas.DataFrame, or pymongo.Cursor obj
        Retron properties as JSON (str), dictionary, DataFrame, or Cursor
        depending on specified format
    
    """
    format = format.lower()
    if format not in ["raw","json","dict","df"]:
        raise ValueError ('format must be "raw", "json", "dict", "df" or empty')
        
    if isinstance(value, list):
        return print('Sorry, more than one value is not supported. Consider using a set.')
        
    if key == "node" and isinstance(value, int):
        value = str(value)
        
    res = rdb_handle.find({str(key):value})
    return format_result(res, format)


###############################################################################
# ADD FUNCTIONS
def add_retron(rdb_handle=None, ret_dict=None, new_property=False):
    """
    Add a single retron given a dictionary. The ``dict`` must include a unique
    identifier key called "node" with a string-encoded integer, e.g., "node":"0".
    
    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    ret_dict : ``dict``
        A dictionary with retron data
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    
    # Check: does ret_dict contain "node" property?
    radd_props = set(ret_dict.keys())
    if "node" not in radd_props:
        # SKIP: assigning new node ID if missing; might be too dangerous?
        # res = rdb_handle.find().sort("node", -1)
        # max_node = format_result(res[0], "df")['node'][0]
        # ret_dict['node'] = str(int(max_node) +1)
        # print("Assigning unique node ID...\n")
        return print(ansiRed.format("MissingKeyError")+": Must provide a node ID")
    else:
        # force string values for node IDs
        ret_dict['node'] = str(ret_dict['node'])
        # "node" is indexed so, duplicates will be caught automatically
       
    # Check: does ret_dict contain novel property names?
    rdb_props = functools.reduce( lambda all_keys, rec_keys: all_keys | set(rec_keys), map(lambda d: d.keys(), rdb_handle.find()), set() )
    radd_new = radd_props.difference(rdb_props)
    if len(radd_new) > 0 and not new_property:
        print(ansiRed.format("Error")+": Failed to add retron." +
              " One or more unrecognized properties detected:" )
        for n in radd_new:
            print("\t"+n)
        print("\nDouble check your property names or consider setting"+
              " new_property=True")
        return
    else:
        try:
            radd_obj = rdb_handle.insert_one(ret_dict)
        except pm.errors.DuplicateKeyError:
            print(ansiRed.format("DuplicateKeyError")+": A retron with the" +
                  " same node ID already exists in the database. Either" +
                  " change the node ID, remove the node property,"+
                  " or consider using update_retron().")
        except Exception as e:
            print(ansiRed.format("Error")+": Failed to add retron.\n", e)
        else:
            radd_id = radd_obj.inserted_id
            print("Added the following retron to the database:")
            radd_res = rdb_handle.find_one({"_id":radd_id})
            return format_result(radd_res)
        


def add_retrons_by_csv(rdb_handle=None, csv_file=None, new_property=False):
    """
    Add one or more retrons given a CSV file. The first column must contain a 
    unique integer identifier and be named "node". 
    
    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    csv_file : ``str``
        Path to CSV file
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    
    print("Added the following retrons to the database:")
    return
    
###############################################################################
# UPDATE FUNCTIONS

def update_retron(rdb_handle=None, ret_dict=None, new_property=False, replace=False):
    """
    Update an existing retron given a dictionaty. The ``dict`` must include a unique
    identifier key called "node" with a string-encoded integer, e.g., "node":"0".
    
    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    ret_dict : ``dict``
        A dictionary with retron data
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.
    replace : ``bool``, optional
        Whether to replace all properties of existing retron with new 
        information, e.g., remove then add.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    
    
    print("Updated the following retron in the database:")
    return

def update_retrons_by_csv(rdb_handle=None, csv_file=None, new_property=False, replace=False):
    """
    Update one or more existing retrons given a CSV file. The first column must 
    contain a unique integer identifier and be named "node".

    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    csv_file : str
        Path to CSV file
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.
    replace : ``bool``, optional
        Whether to replace all properties of existing retron with new 
        information, e.g., remove then add.
            
    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    
    print("Updates the following retrons in the database:")
    return
    

###############################################################################
# REMOVE FUNCTIONS
def remove_retron(rdb_handle=None, node=None):
    """
    Remove a retron given its node identifier.
    
    IMPORTANT: This action will delete a retron and all of its properties from 
    the database!

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    node : ``str`` or ``int`` (automatically converted to ``str``)
        A single retron node ID

    Returns
    -------
    pandas.DataFrame 
        DataFrame of removed retron properties. Last chance to recover deleted 
        data!

    """
    if isinstance(node, list):
        return (print(ansiRed.format("TypeError")+
                      ": node should be a single string. Make a loop if you have a list."))

    gone = get_retron(rdb_handle, node)
    rdb_handle.delete_one({"node":str(node)})
    print("Removed the following retron from the database:")
    return(gone)
    
    
def remove_retrons_by(rdb_handle=None, key="node", value=None ):
    """
    Remove one or more retrons by a particular **key** and **value**. The default
    key is "node". Value can be a set of conditions using MongoDB query
    comparison operators, e.g., {"$gt":5} or {"$gt":2,"$lt":8} or {"$in":[3,5,7]}
        
    https://www.mongodb.com/docs/v4.4/reference/operator/query-comparison/
    
    * $eq - Matches values that are equal to a specified value.
    * $ne - Matches all values that are not equal to a specified value.
    * $gt - Matches values that are greater than a specified value.
    * $gte - Matches values that are greater than or equal to a specified value.
    * $lt - Matches values that are less than a specified value.
    * $lte - Matches values that are less than or equal to a specified value.
    * $in - Matches any of the values specified in an array. Sensitive to type.
    * $nin - Matches none of the values specified in an array. Sensitive to type.
    
    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    key : ``str``, optional
        Property key or name, e.g., "genus"
    value : ``str``, ``int``, ``float`` or ``set``
        Propery value, e.g., "Escherichia" or set of conditions, e.g., {"$gt":5}
    
    Returns
    -------
    pandas.DataFrame 
        DataFrame of removed retrons and their properties. Last chance to 
        recover deleted data!

    """
    if isinstance(value, list):
        return print('Sorry, more than one value is not supported. Consider using a set.')
        
    if key == "node" and isinstance(value, int):
        value = str(value)
        
    gone = get_retrons_by(rdb_handle, key, value)
    rdb_handle.delete_many({str(key):value})
    print("Removed the following retrons from the database:")
    return(gone)
    
    
###############################################################################
# INTERNAL FUNCTIONS
def format_result(result=None, format="df"):
    """
    Transform one or more retronDB query results into useful formats. Takes 
    either a singular dictionary result or ``pymongo.Cursor`` results as input. 
    Returns eiter raw, JSON, dictionary or ``pandas.DataFrame`` (default).

    Parameters
    ----------
    result : ``dict`` or ``pymongo.Cursor``
        Represents the result of pymongo.find() or .find_one()
    format : ``str``, optional
        Either "raw", "json", "dict", or "df" (default)

    Returns
    -------
    str, dict, pandas.DataFrame, or pymongo.Cursor obj
        Retron properties as JSON (str), dictionary, DataFrame, or Cursor
        depending on specified format

    """
    format = format.lower()
    if format not in ["raw","json","dict","df"]:
        raise ValueError ('format must be "raw", "json", "dict", "df" or empty')
        
    # Easy case
    if format == "raw":
        return result
      
    # Return in desired format
    if format == "json":
        return json_util.dumps(result)
    elif format == "dict":
        return json.loads(json_util.dumps(result))
    else: #DataFrame. This one is finicky
        # Check: dict or Cursor?
        res = None
        if isinstance(result, dict):
            res = [result]
        else:
            # Check: single or multiple results
            if len(list(result.clone())) < 2:
                res = [result.clone()][0]
            else:
                res = list(result.clone())
        return pd.DataFrame(res)

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
    