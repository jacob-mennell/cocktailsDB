-- Use this file to write the SQL needed to create the basic tables for your databse
-- These tables are where you are putting the data generated by the bar as well as the relevant data from the cocktails database.

-- create bar stock table
CREATE TABLE bar_stock(
stockID integer  PRIMARY KEY NOT NULL,
glassType CHAR(50),
stock int,
bar CHAR(50)
);


-- create global sales tables from New York, London, Budapest data
CREATE TABLE global_sales(
saleID integer  PRIMARY KEY NOT NULL,
dateOfSale timestamp,
drink CHAR(50),
price float,
bar CHAR(50)
);

-- create cocktails table from cocktails API data
CREATE TABLE cocktails(
idDrink integer PRIMARY KEY NOT NULL,
strDrink CHAR(50),
strCategory CHAR(50),
strIBA CHAR(50),
strAlcoholic CHAR(50),
strGlass CHAR(50),
dateModified timestamp
);