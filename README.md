# Introdution
This repo contains python notebooks for interacting with the retronDB hosted on MongoDB Atlas.


# Installation
While the database is hosted by a cloud service, the notebooks in this repo can either be run locally or in the cloud via Google Colab. Find a Colab launch button at the top of each notebook.

For local runs, you'll need to have a recent version of [Python](https://www.python.org/downloads/) installed along with [Jupyter](https://jupyter.org/install) (or a tool that provides Jupyter, like [Anaconda.Navigator](https://anaconda.org/anaconda/anaconda-navigator)). 

Finally, you will also need install the following packages in your local environment:
```
pip install pymongo
pip install dnspython
pip install python-dotenv
```

# Usage
With the installations complete, you can simply launch Jupyter (e.g., `jupyter notebook`) and open any of the notebooks in this repo and start interacting with the retronDB!  I recommend starting with the [Getting Started](getting-started.ipynb) notebook.

_Note: A username and password are required in order to connect to retronDB. Ask a team member if you don't have these already._ You can either enter them manually each time you connect via one of these notebooks, or you can add the following lines to a `.env` file that you place in the directory as the notebooks, replacing asterisks with your username and password. 
```
export RETRONDB_USR='***'
export RETRONDB_PWD='***'
```

__IMPORTANT: Do not commit or share your .env file and credentials.__

# Development
In addition to the `.ipynb` notebooks there is also a `retrondb.py` module with basic utilities and helper functions for connecting to and interacting with retronDB.  Most users can ingore the module, but it will be critical for debugging and further development.

Feel free to file Issues or submit Pull Requests!
