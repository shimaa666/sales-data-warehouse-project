create Database Sales_DHW;
USE Sales_DHW;

create schema bronze;


-----------------------------
---bronze layer
-----------------------------
	SELECT *
	INTO bronze.customers_raw
	FROM SalesDB2.dbo.Customers
	WHERE 1=0;

	SELECT *
	INTO bronze.products_raw
	FROM SalesDB2.dbo.Products
	WHERE 1=0;

	SELECT *
	INTO bronze.orders_raw
	FROM SalesDB2.dbo.Orders
	WHERE 1=0;

	SELECT *
	INTO bronze.orderdetails_raw
	FROM SalesDB2.dbo.OrderDetails
	WHERE 1=0;



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

    TRUNCATE TABLE bronze.customers_raw;
    INSERT INTO bronze.customers_raw
    SELECT * FROM SalesDB2.dbo.Customers;

    TRUNCATE TABLE bronze.products_raw;
    INSERT INTO bronze.products_raw
    SELECT * FROM SalesDB2.dbo.Products;

    TRUNCATE TABLE bronze.orders_raw;
    INSERT INTO bronze.orders_raw
    SELECT * FROM SalesDB2.dbo.Orders;

    TRUNCATE TABLE bronze.orderdetails_raw;
    SET IDENTITY_INSERT bronze.orderdetails_raw ON;
    INSERT INTO bronze.orderdetails_raw (
        OrderDetailID,
        OrderID,
        ProductID,
        Sales,
        Quantity,
        Discount,
        Profit
    )
    SELECT
        OrderDetailID,
        OrderID,
        ProductID,
        Sales,
        Quantity,
        Discount,
        Profit
    FROM SalesDB2.dbo.OrderDetails;

    SET IDENTITY_INSERT bronze.orderdetails_raw OFF;

END;


