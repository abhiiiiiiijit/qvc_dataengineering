----------------------------------Ans1----------------------------------
-- Calculate total order amounts for a specific date range
WITH cte_orders AS(
----Get all orders between '2019-01-02' AND '2019-01-04'
SELECT 
PID_ORDER
FROM [dbo].[ORDERS]
WHERE CAST(ORDER_TIME AS DATE) BETWEEN '2019-01-02' AND '2019-01-04'
), 
cte_join AS(
---Join orders with order items to calculate individual total order amounts
SELECT 
ord.PID_ORDER AS ORDER_ID,
ord_itm.UNTS * ord_itm.UNITPRICE AS ORDER_SUM
FROM cte_orders AS ord 
LEFT JOIN [dbo].[ORDER_ITEM] AS ord_itm ON ord.PID_ORDER = ord_itm.FID_ORDERS
)
-- Aggregate the total order amounts by ORDER_ID
SELECT 
ORDER_ID,
SUM(ORDER_SUM) AS ORDER_SUM
FROM cte_join
GROUP BY ORDER_ID
ORDER BY ORDER_SUM DESC


------------------------------------------Ans2--------------------------------------------
-- Retrieve RGB and hierarchy article's units sold
WITH cte_all_rgb_parents AS (
-- Recursive CTE to get all RGB parents and their children  (Parent, RGB Children, Article Name)
SELECT
FID_PARENT_ARTICLE AS RGB_PARENTS,
PID_ARTICLE AS CHILD_RGB,
[NAME] AS ARTICLE_NAME
FROM
[dbo].[ARTICLE] AS a1 
WHERE NAME IN ('RED', 'BLUE', 'GREEN')


UNION ALL

SELECT 
a2.FID_PARENT_ARTICLE,
a1.CHILD_RGB,
a1.ARTICLE_NAME
FROM 
[dbo].[ARTICLE] AS a2
INNER JOIN cte_all_rgb_parents AS a1
ON a1.RGB_PARENTS = a2.PID_ARTICLE
AND a2.FID_PARENT_ARTICLE IS NOT NULL

)
, cte_statr_part AS (
-- Get the multiplier for the 'BLOCKSET STARTER KIT' article which has RGB parts
SELECT 
a.PID_ARTICLE,
pl.FID_ARTICLE,
pl.UNITS AS MULTIPLIER

FROM [dbo].[ARTICLE] AS a 
INNER JOIN [dbo].[PARTS_LIST] AS pl 
ON a.PID_PARTS_LIST = pl.FID_PARTS_LIST
AND a.[NAME] = 'BLOCKSET STARTER KIT'
AND pl.FID_ARTICLE IN (SELECT CHILD_RGB FROM cte_all_rgb_parents)
),
cte_agg_all AS (
-- Aggregate all RGB parents and their children with parents multipliers will will be 1.
SELECT
RGB_PARENTS,
CHILD_RGB,
ARTICLE_NAME,
COALESCE(MULTIPLIER, 1) AS MULTIPLIER
FROM cte_all_rgb_parents AS ap 
LEFT JOIN cte_statr_part AS sp
ON ap.RGB_PARENTS = sp.PID_ARTICLE
AND ap.CHILD_RGB = sp.FID_ARTICLE

UNION 
--- Adding RGB itself as parent for ease of calculation
SELECT 
CHILD_RGB,
CHILD_RGB,
ARTICLE_NAME,
1
FROM cte_all_rgb_parents
)
-- Calculate total units sold for each RGB article
SELECT 
agg.CHILD_RGB AS ARTICLE_ID,
agg.ARTICLE_NAME,
SUM(agg.MULTIPLIER * oi.UNTS)AS UNITS_SOLD
FROM cte_agg_all AS agg
LEFT JOIN [dbo].[ORDER_ITEM] AS oi 
ON agg.RGB_PARENTS = oi.FID_ARTICLE
GROUP BY 
agg.CHILD_RGB,
agg.ARTICLE_NAME


-------------------------------------------Ans3--------------------------------------------
-- Calculate total sales for the top 3 articles in 2019 and compare with 2018
WITH cte_2019_sum AS(
-- Get the top 3 articles by total sales in 2019
SELECT TOP 3 --WITH TIES in case you want to consider ties
art.PID_ARTICLE,
art.NAME,
SUM(ord_itm.UNTS * ord_itm.UNITPRICE) AS SUM_2019
FROM [dbo].[ORDERS] AS ord 
LEFT  JOIN [dbo].[ORDER_ITEM] AS ord_itm ON ord.PID_ORDER = ord_itm.FID_ORDERS
LEFT JOIN [dbo].[ARTICLE] AS art ON ord_itm.FID_ARTICLE = art.PID_ARTICLE
WHERE YEAR(ORDER_TIME) = 2019
GROUP BY art.PID_ARTICLE,
art.NAME
ORDER BY SUM_2019 DESC

), 
cte_2018_sum AS(
-- Get the total sales for the same articles in 2018
SELECT 
art.PID_ARTICLE,
SUM(ord_itm.UNTS * ord_itm.UNITPRICE) AS SUM_2018
FROM [dbo].[ORDERS] AS ord 
LEFT  JOIN [dbo].[ORDER_ITEM] AS ord_itm ON ord.PID_ORDER = ord_itm.FID_ORDERS
INNER JOIN cte_2019_sum AS s9 ON s9.PID_ARTICLE = ord_itm.FID_ARTICLE
LEFT JOIN [dbo].[ARTICLE] AS art ON ord_itm.FID_ARTICLE = art.PID_ARTICLE
WHERE YEAR(ORDER_TIME) = 2018
GROUP BY art.PID_ARTICLE,
art.NAME

)
-- Final selection to compare 2019 and 2018 sales for the top articles
SELECT 
s9.PID_ARTICLE   AS Article_ID,
s9.NAME          AS Article_Name,
COALESCE(s9.SUM_2019, 0) AS Total_2019,
COALESCE(s8.SUM_2018, 0) AS Total_2018
FROM cte_2019_sum AS s9
LEFT JOIN cte_2018_sum AS s8 ON s9.PID_ARTICLE = s8.PID_ARTICLE
ORDER BY Total_2019 DESC