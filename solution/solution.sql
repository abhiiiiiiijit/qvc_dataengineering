----------------------------------Q1----------------------------------

WITH cte_orders AS(
SELECT 
PID_ORDER
FROM [dbo].[ORDERS]
WHERE CAST(ORDER_TIME AS DATE) BETWEEN '2019-01-02' AND '2019-01-04'
), 
cte_join AS(
SELECT 
ord.PID_ORDER AS ORDER_ID,
ord_itm.UNTS * ord_itm.UNITPRICE AS ORDER_SUM
FROM cte_orders AS ord 
LEFT JOIN [dbo].[ORDER_ITEM] AS ord_itm ON ord.PID_ORDER = ord_itm.FID_ORDERS
)
SELECT 
ORDER_ID,
SUM(ORDER_SUM) AS ORDER_SUM
FROM cte_join
GROUP BY ORDER_ID
ORDER BY ORDER_SUM DESC


------------------------------------------Q2--------------------------------------------




-------------------------------------------Q3--------------------------------------------

WITH cte_2019_sum AS(

SELECT TOP 3 WITH TIES
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

SELECT 
s9.PID_ARTICLE   AS Article_ID,
s9.NAME          AS Article_Name,
COALESCE(s9.SUM_2019, 0) AS Total_2019,
COALESCE(s8.SUM_2018, 0) AS Total_2018
FROM cte_2019_sum AS s9
LEFT JOIN cte_2018_sum AS s8 ON s9.PID_ARTICLE = s8.PID_ARTICLE
ORDER BY Total_2019 DESC