import gzip
import pandas as pd
import requests
import logging
import sqlite3
import datetime


def setup_logging():
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


def get_cocktail_data(drink):
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
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.executescript(sql_script_string)
            conn.commit()
        logging.info("Execute SQL script complete.")
    except Exception as e:
        logging.error("SQL script failed to execute", e)


def execute_external_sql_script_file(script_file_path, db_name):
    with open(script_file_path, "r") as file:
        sql_script_string = file.read()
    execute_sql_script(sql_script_string, db_name)


def create_tables(db_name):
    execute_external_sql_script_file("database/data_tables.sql", db_name)


def insert_data_into_table(df, table_name, db_name):
    with sqlite3.connect(db_name) as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)
    logging.info(f"Data inserted into {table_name} table.")


def process_bar_data():
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

    return bar_stock_df


def process_sales_data():
    # create dicts of dates from text file for date filtering prior to upload
    date_dict = {}
    with open("last_update.txt") as f:
        for line in f:
            k, v = line.split(" ", 1)
            v = v[:-1]
            date_dict[k] = v

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

    # Set new dates to limit the size of future uploads
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

    return global_df


def query_cocktail_data():
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
        df = get_cocktail_data(drink)
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

    return cocktails_df


def main():

    # Initialize logging
    setup_logging()

    # Create tables in the SQLite database
    db_name = "database/bar_db"
    create_tables(db_name)
    logging.info("Tables created in the SQLite database.")

    # Read and process bar data
    bar_stock_df = process_bar_data()
    logging.info("Bar data processed successfully.")

    # Read and process sales data
    global_df = process_sales_data()
    logging.info("Sales data processed successfully.")

    # Query cocktail data
    cocktails_df = query_cocktail_data()
    logging.info("Cocktail data queried successfully.")

    # Insert data into respective tables
    insert_data_into_table(global_df, "global_sales", db_name)
    insert_data_into_table(bar_stock_df, "bar_stock", db_name)
    insert_data_into_table(cocktails_df, "cocktails", db_name)
    logging.info("Data inserted into respective tables.")


if __name__ == "__main__":
    main()
