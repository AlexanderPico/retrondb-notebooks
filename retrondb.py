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
# ansiBlue = "\033[94m {}\033[00m"


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
    
def save_retronDB(rdb_handle=None, filename=None, overwrite=False):
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
        automatically added is missing. 
    overwrite : ``bool``
        Whether to allow existing files to be overwritten. Default is False.

    Returns
    -------
    str
        Path to saved retron database file.

    """
    if re.search('.csv$', filename) is None: filename += '.csv'
    if os.path.exists(filename) and not overwrite:
        raise ProtectedFileError()
 
    rdb_df = get_all_retrons(rdb_handle)
    rdb_df.to_csv(filename, index=False)
    print("Saved retron database.")
    return os.path.abspath(filename)
    
    
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
        automatically added is missing. 
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
    add_retrons_by_csv(rdb_handle,filename, True) 
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
    res = rdb_handle.find_one({"node":str(node)})
    return format_result(res, format)

    
def get_retrons_by(rdb_handle=None, key="node", value=None, format="df"):
    """
    Returns one or more retrons by a particular **key** and **value**. The default
    key is "node". Value can be a set of conditions using MongoDB query
    comparison operators, e.g., {"$eq":"5"} or {"$in":["3","5","7"]}
        
    https://www.mongodb.com/docs/v4.4/reference/operator/query-comparison/
    
    * $eq - Matches values that are equal to a specified value.
    * $ne - Matches all values that are not equal to a specified value.
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
    if isinstance(value, list):
        raise TypeError ("Sorry, more than one value is not supported." +
                         " Consider using the set syntax with comparison" +
                         " operators.")
        
    if key == "node" and isinstance(value, int):
        value = str(value)
        
    res = rdb_handle.find({str(key):value})
    return format_result(res, format)


###############################################################################
# ADD FUNCTIONS
def add_retron(rdb_handle=None, retron_dict=None, new_property=False):
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
    retron_dict : ``dict``
        A dictionary with retron data
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    radd_props = set(retron_dict.keys())
    if "node" not in radd_props:
        # SKIP: assigning new node ID if missing; might be too dangerous?
        # res = rdb_handle.find().sort("node", -1)
        # max_node = format_result(res[0], "df")['node'][0]
        # retron_dict['node'] = str(int(max_node) +1)
        # print("Assigning unique node ID...\n")
        raise MissingKeyError()
    else:
        # force string values for node IDs
        retron_dict['node'] = str(retron_dict['node'])
        # "node" is indexed so, duplicates will be caught automatically
       
    check_new_property(rdb_handle, radd_props, new_property)
    
    try:
        radd_obj = rdb_handle.insert_one(retron_dict)
    except pm.errors.DuplicateKeyError:
        print(ansiRed.format("DuplicateKeyError")+": A retron with the" +
              " same node ID already exists in the database. Either" +
              " change the node ID, remove the \"node\" property,"+
              " or consider using update_retron().")
    except Exception as e:
        print(ansiRed.format("Error")+": Failed to add retron.\n", e)
    else:
        radd_id = radd_obj.inserted_id
        print("Added retron to the database.")
        radd_res = rdb_handle.find_one({"_id":radd_id})
        return format_result(radd_res)
        


def add_retrons_by_csv(rdb_handle=None, filename=None, new_property=False):
    """
    Add one or more retrons given a CSV file. There must be
    a unique integer identifier for each row in a column named "node".
    
    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    filename : ``str``
        Full path or path relavtive to current working directory, in addition 
        to the name of the file to be read. The ``.csv`` extension is 
        automatically added is missing. 
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of added retron properties.

    """
    ret_df = read_retron_csv(rdb_handle=rdb_handle, filename=filename)
    radd_props = set(ret_df.columns)
    check_new_property(rdb_handle, radd_props, new_property)
    
    # DF to dict
    ret_js = ret_df.to_json(orient='records')
    ret_dict = json.loads(ret_js)
    try:
        radd_obj = rdb_handle.insert_many(ret_dict)
    except pm.errors.BulkWriteError as e:
        if "duplicate key error" in e.args[0]:
            print(ansiRed.format("DuplicateKeyError")+": A retron with the" +
              " same node ID already exists in the database. Either" +
              " change the node ID, remove the \"node\" property,"+
              " or consider using update_retrons_by_csv().")
        else:
            print(ansiRed.format("Error")+": Failed to add retrons.\n", e)
    except Exception as e:
        print(ansiRed.format("Error")+": Failed to add retrons.\n", e)
    else:
        print("Added retrons to the database.")
        radd_ids = radd_obj.inserted_ids
        radd_res = rdb_handle.find({"_id":{"$in":radd_ids}})
        return format_result(radd_res)
    
###############################################################################
# UPDATE FUNCTIONS

def update_retron(rdb_handle=None, retron_dict=None, new_property=False, replace=False):
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
    retron_dict : ``dict``
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
    rupd_props = set(retron_dict.keys())
    if "node" not in rupd_props:
        raise MissingKeyError()
    else:
        # force string values for node IDs
        retron_dict['node'] = str(retron_dict['node'])
       
    check_new_property(rdb_handle, rupd_props, new_property)
    
    # TODO: impl replace param (or add)
    
    # Get node list for update keys
    rupd_node = str(retron_dict['node'])
    try:
        rdb_handle.update_one({"node":rupd_node},{"$set":retron_dict})
    except Exception as e:
        print(ansiRed.format("Error")+": Failed to update retron.\n", e)
    else:
        print("Updated retron in the database.")
        rupd_res = rdb_handle.find({"node":{"$eq":rupd_node}})
        return format_result(rupd_res)

def update_retrons_by_csv(rdb_handle=None, filename=None, new_property=False, replace=False):
    """
    Update one or more existing retrons given a CSV file. There must be
    a unique integer identifier for each row in a column named "node".

    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    filename : ``str``
        Full path or path relavtive to current working directory, in addition 
        to the name of the file to be read. The ``.csv`` extension is 
        automatically added is missing. 
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
    ret_df = read_retron_csv(rdb_handle=rdb_handle, filename=filename)
    rupd_props = set(ret_df.columns)
    check_new_property(rdb_handle, rupd_props, new_property)
    
    # TODO: impl replace param (or add)
    
    #DF to dict
    ret_js = ret_df.to_json(orient='records')
    ret_dict = json.loads(ret_js)
    # Get node list for update keys
    rupd_nodes = list(ret_df['node'])
    try:
        rdb_handle.update_many({"node":{"$in":rupd_nodes}}, ret_dict) #TODO https://stackoverflow.com/questions/68530571/update-many-mongo-document-with-pymongo
    except Exception as e:
        print(ansiRed.format("Error")+": Failed to update retrons.\n", e)
    else:
        print("Updated retrons in the database.")
        rupd_res = rdb_handle.find({"node":{"$in":rupd_nodes}})
        return format_result(rupd_res)

    

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
        raise TypeError ("\"node\" should be a single string. Make a loop if" + 
                         " you have a list, or consider using " +
                         " remove_retrons_by() with the set syntax.")

    gone = get_retron(rdb_handle, node)
    rdb_handle.delete_one({"node":str(node)})
    print("Removed a retron from the database.")
    return gone
    
    
def remove_retrons_by(rdb_handle=None, key="node", value=None ):
    """
    Remove one or more retrons by a particular **key** and **value**. The default
    key is "node". Value can be a set of conditions using MongoDB query
    comparison operators, e.g., {"$eq":"5"} or {"$in":["3","5","7"]}
        
    https://www.mongodb.com/docs/v4.4/reference/operator/query-comparison/
    
    * $eq - Matches values that are equal to a specified value.
    * $ne - Matches all values that are not equal to a specified value.
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
        raise TypeError ("Sorry, more than one value is not supported." +
                         " Consider using the set syntax with comparison" +
                         " operators.")
        
    if key == "node" and isinstance(value, int):
        value = str(value)
        
    gone = get_retrons_by(rdb_handle, key, value)
    rdb_handle.delete_many({str(key):value})
    print("Removed retrons from the database.")
    return gone
    
    
###############################################################################
# INTERNAL FUNCTIONS
def read_retron_csv(rdb_handle=None, filename=None):
    """
    Read, clean and validate retron data from a CSV file. There must be
    a unique integer identifier for each row in a column named "node".
    
    All properties will be checked against current database properties. By 
    default, unrecognized properties will be rejected (see **new_property** parameter).

    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
    filename : ``str``
        Full path or path relavtive to current working directory, in addition 
        to the name of the file to be read. The ``.csv`` extension is 
        automatically added is missing. 
        
    Returns
    -------
    pandas.DataFrame 
        DataFrame of cleaned and validated csv data

    """
    if re.search('.csv$', filename) is None: filename += '.csv'  
    ret_df = pd.read_csv(filename, dtype=str)
    ret_cols = ret_df.columns
    
    # Drop "_id" if present (e.g., from backup file)
    if "_id" in ret_cols:
        ret_df = ret_df.drop('_id',1)
    
    # Check node IDs
    if "node" not in ret_cols:
       raise MissingKeyError("There must be a \"node\" column.")
    ret_df = ret_df[ret_df['node']>="0"]
    
    # Strip leading and trailing blanks
    ret_df = ret_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    return ret_df
    
def check_new_property(rdb_handle=None, props=None, new_property=False):
    """
    Check incoming properties against existing properties in retron database.
    If new_property is False (default), then novel properties will be rejected.
    
    This check is intended to help protect against inserting typos and other
    unintended property names into the database.
    
    Parameters
    ----------
    rdb_handle : ``pymongo.Collection`` obj
        A retron database collection object, e.g., the output of 
        ``get_retronDB()``
        
    new_property : ``bool``, optional
        Whether to accept novel properties. Default is ``False``.  
        
    Returns
    -------
    None
    
    """
    rdb_props = functools.reduce( lambda all_keys, rec_keys: all_keys | set(rec_keys), map(lambda d: d.keys(), rdb_handle.find()), set() )
    radd_new = props.difference(rdb_props)
    if len(radd_new) > 0 and not new_property:
        raise UnrecognizedPropertyError(radd_new)


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

###############
# CLASSES

class MissingKeyError(ValueError):
    '''When the node ID key is missing from incoming retron data'''
    def __init__(self, message='Please provide a node ID'):
        self.message = message
        super().__init__(self.message)


class UnrecognizedPropertyError(ValueError):
    '''When a new property is found in incoming retron data'''
    def __init__(self, new_props, message='Failed to add or update retron. '+
                  "\nOne or more unrecognized properties detected:\n\n"):
        self.message = message
        for n in new_props:
            self.message += "\t"+n+"\n"
        self.message += "\nDouble check your property names or consider setting new_property=True"
        super().__init__(self.message)
        
class ProtectedFileError(ValueError):
    '''When a file already exists. Overwrite is not allowed.'''
    def __init__(self, message="This file already exists and cannot be" +
                 " overwritten with this function. Consider using a new" +
                 " filename."):
        self.message = message
        super().__init__(self.message)

