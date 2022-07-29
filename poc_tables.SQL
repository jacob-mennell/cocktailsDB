-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions

CREATE TABLE poc_analysis AS

-- create table grouped on day level with glass for cocktail obtained from cocktail table
WITH grouped_drinks AS
(
SELECT
DATE(dateOfSale) as dayOfSale,
drink,
price,
bar,
c.strGlass,
count(drink) as drinkCount
from
global_sales gs
LEFT JOIN cocktails c
ON c.strDrink = gs.drink
GROUP BY DATE(dateOfSale), drink, price, bar, c.strGlass
)

-- use above CTE to join to bar stock
SELECT
gs.*,
bs.stock,
CASE
    WHEN drinkCount < stock THEN 'NO ISSUE'
    WHEN drinkCount >= stock THEN 'POTENTIAL ISSUE'
    END AS 'comment'
FROM grouped_drinks gs
LEFT JOIN bar_stock bs
ON gs.strGlass = bs.glassType
AND
gs.bar = bs.bar

;