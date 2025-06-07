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


------------------------------------------Q2--------------------------------------------

