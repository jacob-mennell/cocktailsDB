import gzip
import pandas as pd
import requests
import logging
import time
import sqlite3


# functions
def cocktail_extract(drink):
    """
    Function takes a drink and submits a GET request to the cocktaildb API and returns a dataframe.

    :param drink:
    :return: dataframe
    """
    try:

        r = requests.request(method='GET',
                             url=f'https://www.thecocktaildb.com/api/json/v1/1/search.php?s={drink}'
                             )

        # error capture
        if r.status_code != 200:
            logging.info(drink, r.status_code)

        df = pd.DataFrame(r.json()['drinks'])

    except Exception as e:
        df = pd.DataFrame()
        logging.error("Error getting cocktail data", e)

    return df


def execute_sql_script(sql_script_string, db_name):
    """
    Function initialises connection to sql db and executes a script on the database.
    :param sql_script_string:
    :param db_name:
    :return: nothing
    """
    try:
        # Connect to sqlite3 database.then run the cursor
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        # Run the sql script string
        cursor.executescript(sql_script_string)
        # Commit operation
        conn.commit()
        # Close cursor object
        cursor.close()
        # Close connection object
        conn.close()
        logging.info('Execute sql script complete.')
    except Exception as e:
        logging.error("SQL script failed to execute", e)


# This function will read the sql script text from the external sql file and then run the sql script text.
def execute_external_sql_script_file(script_file_path, db_name):
    """
    Function opens external file and inputs that into hte execute_sql_script function.

    :param script_file_path:
    :param db_name:
    :return: nothing
    """
    # Open the external sql file.
    file = open(script_file_path, 'r')
    # Read out the sql script text in the file.
    sql_script_string = file.read()
    # Close the sql file object.
    file.close()
    # Execute the read out sql script string.
    execute_sql_script(sql_script_string, db_name)


# initialise log file
logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

# return error and critical logs to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console_format = '%(asctime)s | %(levelname)s: %(message)s'
console.setFormatter(logging.Formatter(console_format))
logger.addHandler(console)

# create log file to capture all logging
file_handler = logging.FileHandler('drinks_db.log')
file_handler.setLevel(logging.INFO)
file_handler_format = '%(asctime)s | %(levelname)s | %(lineno)d: %(message)s'
file_handler.setFormatter(logging.Formatter(file_handler_format))
logger.addHandler(file_handler)

# import bar data
bar_stock_df = pd.read_csv('data/bar_data.csv'
                           , header=0
                           , sep=',')
# minor cleaning
bar_stock_df = bar_stock_df.reset_index().rename\
    (columns={'index': 'stockID', 'glass_type': 'glassType'}).set_index('stockID')
bar_stock_df['glassType'] = bar_stock_df['glassType'].map(lambda x: x.upper())
bar_stock_df['bar'] = bar_stock_df['bar'].map(lambda x: x.upper())
bar_stock_df['stock'] = bar_stock_df['stock'].str.extract('(\d+)', expand=False)
bar_stock_df['stock'] = bar_stock_df['stock'].astype(int)

# set column names
col_names = ['dateOfSale', 'drink', 'price']

# import budapest data
budapest_df = pd.read_csv('data/budapest.csv.gz'
                          , compression='gzip'
                          , header=0
                          , sep=','
                          , names=col_names
                          , parse_dates=['dateOfSale'])
budapest_df['bar'] = 'budapest'

# import london data
london_df = pd.read_csv('data/london_transactions.csv.gz'
                        , compression='gzip'
                        , header=None
                        , sep='\t'
                        , names=col_names
                        , parse_dates=['dateOfSale'])
london_df['bar'] = 'london'

# import new york data
new_york_df = pd.read_csv('data/ny.csv.gz'
                          , compression='gzip'
                          , header=0
                          , sep=','
                          , names=col_names
                          , parse_dates=['dateOfSale'])
new_york_df['bar'] = 'new york'

# concat dataframes
global_df = pd.concat([budapest_df, london_df, new_york_df])
# minor cleaning
global_df = global_df.reset_index().rename(columns={'index': 'saleID'}).set_index('saleID')
global_df['drink'] = global_df['drink'].map(lambda x: x.upper())
global_df['bar'] = global_df['bar'].map(lambda x: x.upper())
global_df['price'] = global_df['price'].astype(float)

# get drinks to query API with
master_drinks = list(set((london_df.drink.unique().tolist())
                         + (new_york_df.drink.unique().tolist())
                         + (budapest_df.drink.unique().tolist())))

# apply cocktail_extract function
counter = 1
dfs = []
for drink in master_drinks:
    # extract data from the offset value
    df = cocktail_extract(drink)
    # append data
    dfs.append(df)
    # print output to see status
    print(f'Current loop number: {counter} out of {len(master_drinks)}')
    counter += 1

# combine dfs
cocktails_df = pd.concat(dfs)
# minor cleaning
cocktails_df['strDrink'] = cocktails_df['strDrink'].map(lambda x: x.upper())
cocktails_df['strGlass'] = cocktails_df['strGlass'].map(lambda x: x.upper())

# execute sql script to create tables
execute_external_sql_script_file('data_tables.sql', 'bar_db')

# populate sql tables

# Connect to sqlite3 database.
conn = sqlite3.connect('bar_db')

# send each dataframe to sql
cocktails_df.to_sql('cocktails', conn, if_exists='replace')
global_df.to_sql('global_sales', conn, if_exists='replace')
bar_stock_df.to_sql('bar_stock', conn, if_exists='replace')

# Close the connection object.
conn.close()

# Create POC Table for analysis
execute_external_sql_script_file('poc_tables.sql', 'bar_db')

# next steps
# add new data without batch refreshing
# use date to filter insert in future