create Database Sales_DHW;
USE Sales_DHW;


create schema gold;


---------------------------------------
-- gold layer
---------------------------------------
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    CustomerKey AS customer_key, 
    CustomerID,
    CustomerName,
    Segment,
    Country,
    City,
    State,
    PostalCode,
    Region
FROM silver.customers;
GO


IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ProductKey AS product_key, 
    ProductID,
    ProductName,
    Category,
    SubCategory
FROM silver.products;
GO


IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    o.OrderID AS order_number,
    c.customer_key,
    p.product_key,
    TRY_CAST(o.OrderDate AS DATE) AS order_date,
    TRY_CAST(o.ShipDate AS DATE) AS ship_date,
    o.ShipMode,
    od.Sales AS sales_amount,
    od.Quantity,
    od.Discount,
    od.Profit,
    (od.Sales - od.Profit) AS cost_amount
FROM silver.orders o
INNER JOIN silver.orderdetails od
    ON o.OrderID = od.OrderID
INNER JOIN gold.dim_customers c
    ON o.CustomerID = c.CustomerID
INNER JOIN gold.dim_products p
    ON od.ProductID = p.ProductID;
GO

