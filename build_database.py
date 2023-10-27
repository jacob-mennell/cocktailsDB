import gzip
import pandas as pd
import requests
import logging
import time
import sqlite3
import datetime


# functions
def cocktail_extract(drink):
    """
    Function takes a drink and submits a GET request to the cocktaildb API and returns a dataframe.

    :param drink:
    :return: dataframe
    """
    try:
        r = requests.request(
            method="GET",
            url=f"https://www.thecocktaildb.com/api/json/v1/1/search.php?s={drink}",
        )

        # error capture
        if r.status_code != 200:
            logging.info(drink, r.status_code)

        df = pd.DataFrame(r.json()["drinks"])

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
        logging.info("Execute sql script complete.")
    except Exception as e:
        logging.error("SQL script failed to execute", e)


def execute_external_sql_script_file(script_file_path, db_name):
    """
    Function opens external file and inputs that into the execute_sql_script function to run the query.

    :param script_file_path:
    :param db_name:
    :return: nothing
    """
    # Open the external sql file.
    file = open(script_file_path, "r")
    # Read out the sql script text in the file.
    sql_script_string = file.read()
    # Close the sql file object.
    file.close()
    # Execute the read out sql script string.
    execute_sql_script(sql_script_string, db_name)


if __name__ == "__main__":
    # initialise log file
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    # return error and critical logs to console
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    console_format = "%(asctime)s | %(levelname)s: %(message)s"
    console.setFormatter(logging.Formatter(console_format))
    logger.addHandler(console)

    # create log file to capture all logging
    file_handler = logging.FileHandler("drinks_db.log")
    file_handler.setLevel(logging.INFO)
    file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
    file_handler.setFormatter(logging.Formatter(file_handler_format))
    logger.addHandler(file_handler)

    # create dicts of dates from text file for date filtering prior to upload
    date_dict = {}
    with open("last_update.txt") as f:
        for line in f:
            k, v = line.split(" ", 1)
            v = v[:-1]
            date_dict[k] = v

    # import bar data
    bar_stock_df = pd.read_csv("data/bar_data.csv", header=0, sep=",")
    # minor cleaning
    bar_stock_df = (
        bar_stock_df.reset_index()
        .rename(columns={"index": "stockID", "glass_type": "glassType"})
        .set_index("stockID")
    )
    bar_stock_df["stock"] = bar_stock_df["stock"].str.extract("(\d+)", expand=False)
    bar_stock_df["stock"] = bar_stock_df["stock"].astype(int)
    bar_stock_df = bar_stock_df.applymap(lambda s: s.lower() if type(s) == str else s)

    # set global df column names
    col_names = ["dateOfSale", "drink", "price"]

    # import budapest data
    budapest_df = pd.read_csv(
        "data/budapest.csv.gz",
        compression="gzip",
        header=0,
        sep=",",
        names=col_names,
        parse_dates=["dateOfSale"],
    )
    budapest_df["bar"] = "budapest"
    budapest_df = budapest_df.loc[
        budapest_df["dateOfSale"] > date_dict["BUDA_date_max"]
    ]
    budapest_max_date = str(budapest_df.dateOfSale.max())

    # import london data
    london_df = pd.read_csv(
        "data/london_transactions.csv.gz",
        compression="gzip",
        header=None,
        sep="\t",
        names=col_names,
        parse_dates=["dateOfSale"],
    )
    london_df["bar"] = "london"
    london_df = london_df.loc[london_df["dateOfSale"] > date_dict["LON_date_max"]]
    london_max_date = str(london_df.dateOfSale.max())

    # import new york data
    new_york_df = pd.read_csv(
        "data/ny.csv.gz",
        compression="gzip",
        header=0,
        sep=",",
        names=col_names,
        parse_dates=["dateOfSale"],
    )
    new_york_df["bar"] = "new york"
    new_york_df = new_york_df.loc[new_york_df["dateOfSale"] > date_dict["NYC_date_max"]]
    ny_max_date = str(new_york_df.dateOfSale.max())

    # set new dates to limit size of future uploads
    with open("last_update.txt", "w") as f:
        f.write(
            f"NYC_date_max {ny_max_date}\nLON_date_max {london_max_date}\nBUDA_date_max {budapest_max_date}\n"
        )
    
    date_dict = {}
    with open("last_update.txt") as f:
        for line in f:
            k, v = line.split(" ", 1)
            v = v[:-1]
            date_dict[k] = v

    # concat dataframes
    global_df = pd.concat([budapest_df, london_df, new_york_df], ignore_index=True)
    # minor cleaning
    global_df = (
        global_df.reset_index().rename(columns={"index": "saleID"}).set_index("saleID")
    )
    global_df["price"] = global_df["price"].astype(float)
    global_df = global_df.applymap(lambda s: s.lower() if type(s) == str else s)

    # get drinks to query API
    master_drinks = list(
        set(
            (london_df.drink.unique().tolist())
            + (new_york_df.drink.unique().tolist())
            + (budapest_df.drink.unique().tolist())
        )
    )

    # apply cocktail_extract function
    counter = 1
    dfs = []
    for drink in master_drinks:
        # extract data from the offset value
        df = cocktail_extract(drink)
        df = df[
            [
                "idDrink",
                "strDrink",
                "strCategory",
                "strIBA",
                "strAlcoholic",
                "strGlass",
                "dateModified",
            ]
        ]
        # append data
        dfs.append(df)
        # logging.info output to see status
        logging.info(f"Current loop number: {counter} out of {len(master_drinks)}")
        counter += 1

    # combine dfs
    cocktails_df = pd.concat(dfs, ignore_index=True)
    # minor cleaning
    cocktails_df = cocktails_df.sort_values(by="dateModified", ascending=False)
    cocktails_df["dateModified"] = pd.to_datetime(
        cocktails_df["dateModified"], infer_datetime_format=True
    )
    cocktails_df = cocktails_df.drop_duplicates(
        subset=[
            "idDrink",
            "strDrink",
            "strCategory",
            "strIBA",
            "strAlcoholic",
            "strGlass",
        ],
        keep="first",
    )
    cocktails_df = cocktails_df.applymap(lambda s: s.lower() if type(s) == str else s)

    # execute sql script to create tables
    execute_external_sql_script_file("data_tables.sql", "bar_db")

    # populate sql tables

    # Connect to sqlite3 database.
    conn = sqlite3.connect("bar_db")

    # send each dataframe to sql
    global_df.to_sql(
        "global_sales", conn, if_exists="append", index=True, index_label="saleID"
    )
    bar_stock_df.to_sql(
        "bar_stock", conn, if_exists="append", index=True, index_label="stockID"
    )
    cocktails_df.to_sql("cocktails", conn, if_exists="append", index=False)

    # Close the connection object.
    conn.close()

    # Create POC Table for analysis
    execute_external_sql_script_file("poc_tables.sql", "bar_db")
