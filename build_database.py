import gzip
import pandas as pd
import requests
import logging
import sqlite3
import datetime


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.NOTSET)

# Return error and critical logs to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console_format = "%(asctime)s | %(levelname)s: %(message)s"
console.setFormatter(logging.Formatter(console_format))
logger.addHandler(console)

# Create log file to capture all logging
file_handler = logging.FileHandler("drinks_db.log")
file_handler.setLevel(logging.INFO)
file_handler_format = "%(asctime)s | %(levelname)s | %(lineno)d: %(message)s"
file_handler.setFormatter(logging.Formatter(file_handler_format))
logger.addHandler(file_handler)


# Functions
def cocktail_extract(drink):
    """
    Function takes a drink and submits a GET request to the cocktaildb API and returns a dataframe.

    :param drink:
    :return: dataframe
    """
    try:
        url = f"https://www.thecocktaildb.com/api/json/v1/1/search.php?s={drink}"
        response = requests.get(url)

        # Error capture
        if response.status_code != 200:
            logging.error(
                f"Error getting cocktail data for {drink}: {response.status_code}"
            )
            return pd.DataFrame()

        df = pd.DataFrame(response.json()["drinks"])

    except Exception as e:
        logging.error(f"Error getting cocktail data for {drink}: {e}")
        df = pd.DataFrame()

    return df


def execute_sql_script(sql_script_string, db_name):
    """
    Function initializes connection to SQL database and executes a script on the database.

    :param sql_script_string:
    :param db_name:
    :return: nothing
    """
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_script_string)
            conn.commit()
        logging.info("Execute SQL script complete.")
    except Exception as e:
        logging.error("SQL script failed to execute", e)


def execute_external_sql_script_file(script_file_path, db_name):
    """
    Function opens external file and inputs that into the execute_sql_script function to run the query.

    :param script_file_path:
    :param db_name:
    :return: nothing
    """
    # Open the external SQL file.
    with open(script_file_path, "r") as file:
        sql_script_string = file.read()
    # Execute the SQL script string.
    execute_sql_script(sql_script_string, db_name)


def create_tables(db_name):
    """
    Function creates necessary tables in the SQLite database.

    :param db_name:
    :return: nothing
    """
    execute_external_sql_script_file("create_tables.sql", db_name)


def insert_data(df, table_name, db_name):
    """
    Function inserts data from a DataFrame into a specified table in the SQLite database.

    :param df: DataFrame
    :param table_name: str
    :param db_name: str
    :return: nothing
    """
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)
    logging.info(f"Data inserted into {table_name} table.")


if __name__ == "__main__":
    # create dicts of dates from text file for date filtering prior to upload
    date_dict = {}
    with open("last_update.txt") as f:
        for line in f:
            k, v = line.split(" ", 1)
            v = v[:-1]
            date_dict[k] = v

    # Import bar data
    bar_stock_df = pd.read_csv("data/bar_data.csv", header=0, sep=",")

    # Minor cleaning
    bar_stock_df = (
        bar_stock_df.reset_index()
        .rename(columns={"index": "stockID", "glass_type": "glassType"})
        .set_index("stockID")
    )
    bar_stock_df["stock"] = bar_stock_df["stock"].str.extract("(\d+)", expand=False)
    bar_stock_df["stock"] = bar_stock_df["stock"].astype(int)
    bar_stock_df = bar_stock_df.applymap(
        lambda s: s.lower() if isinstance(s, str) else s
    )

    # Set global df column names
    col_names = ["dateOfSale", "drink", "price"]

    # Import budapest data
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
        budapest_df["dateOfSale"] > date_dict.get("BUDA_date_max", "1900-01-01")
    ]
    budapest_max_date = str(budapest_df.dateOfSale.max())

    # Import london data
    london_df = pd.read_csv(
        "data/london_transactions.csv.gz",
        compression="gzip",
        header=None,
        sep="\t",
        names=col_names,
        parse_dates=["dateOfSale"],
    )
    london_df["bar"] = "london"
    london_df = london_df.loc[
        london_df["dateOfSale"] > date_dict.get("LON_date_max", "1900-01-01")
    ]
    london_max_date = str(london_df.dateOfSale.max())

    # Import new york data
    new_york_df = pd.read_csv(
        "data/ny.csv.gz",
        compression="gzip",
        header=0,
        sep=",",
        names=col_names,
        parse_dates=["dateOfSale"],
    )
    new_york_df["bar"] = "new york"
    new_york_df = new_york_df.loc[
        new_york_df["dateOfSale"] > date_dict.get("NYC_date_max", "1900-01-01")
    ]
    ny_max_date = str(new_york_df.dateOfSale.max())

    # Set new dates to limit size of future uploads
    date_dict.update(
        {
            "NYC_date_max": ny_max_date,
            "LON_date_max": london_max_date,
            "BUDA_date_max": budapest_max_date,
        }
    )
    with open("last_update.txt", "w") as f:
        for k, v in date_dict.items():
            f.write(f"{k} {v}\n")

    # Concatenate dataframes
    global_df = pd.concat([budapest_df, london_df, new_york_df], ignore_index=True)
    # Minor cleaning
    global_df = (
        global_df.reset_index().rename(columns={"index": "saleID"}).set_index("saleID")
    )
    global_df["price"] = global_df["price"].astype(float)
    global_df = global_df.applymap(lambda s: s.lower() if isinstance(s, str) else s)

    # Get drinks to query API
    master_drinks = list(
        set(
            london_df.drink.unique().tolist()
            + new_york_df.drink.unique().tolist()
            + budapest_df.drink.unique().tolist()
        )
    )

    dfs = []
    for counter, drink in enumerate(master_drinks, start=1):
        # Extract data from the offset value
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
        # Append data
        dfs.append(df)
        # Logging info output to see status
        logging.info(f"Current loop number: {counter} out of {len(master_drinks)}")
    # Combine dataframes
    cocktails_df = pd.concat(dfs, ignore_index=True)
    # Minor cleaning
    cocktails_df = cocktails_df.sort_values(by="dateModified", ascending=False)
    cocktails_df["dateModified"] = pd.to_datetime(cocktails_df["dateModified"])
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
    cocktails_df = cocktails_df.applymap(
        lambda s: s.lower() if isinstance(s, str) else s
    )

    # Create tables and insert data
    db_name = "bar_db"
    create_tables(db_name)
    insert_data(global_df, "global_sales", db_name)
    insert_data(bar_stock_df, "bar_stock", db_name)
    insert_data(cocktails_df, "cocktails", db_name)
