create Database Sales_DHW;
USE Sales_DHW;

create schema silver;

---------------------------------
-----silver layer
---------------------------------

IF OBJECT_ID('silver.customers','U') IS NOT NULL DROP TABLE silver.customers;
GO
CREATE TABLE silver.customers (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(50),
    CustomerName NVARCHAR(200),
    Segment NVARCHAR(50),
    Country NVARCHAR(50),
    City NVARCHAR(50),
    State NVARCHAR(50),
    PostalCode NVARCHAR(20),
    Region NVARCHAR(50),
    DWH_CreateDate DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.products','U') IS NOT NULL DROP TABLE silver.products;
GO
CREATE TABLE silver.products (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID NVARCHAR(50),
    ProductName NVARCHAR(300),
    Category NVARCHAR(100),
    SubCategory NVARCHAR(100),
    DWH_CreateDate DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.orders','U') IS NOT NULL DROP TABLE silver.orders;
GO
CREATE TABLE silver.orders (
    OrderKey INT IDENTITY(1,1) PRIMARY KEY,
    OrderID NVARCHAR(50),
    CustomerID NVARCHAR(50),
    OrderDate DATE,
    ShipDate DATE,
    ShipMode NVARCHAR(50),
    DWH_CreateDate DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.orderdetails','U') IS NOT NULL DROP TABLE silver.orderdetails;
GO
CREATE TABLE silver.orderdetails (
    OrderDetailKey INT IDENTITY(1,1) PRIMARY KEY,
    OrderID NVARCHAR(50),
    ProductID NVARCHAR(50),
    Sales FLOAT,
    Quantity INT,
    Discount FLOAT,
    Profit FLOAT,
    DWH_CreateDate DATETIME2 DEFAULT GETDATE()
);


/*============================================*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

	TRUNCATE TABLE silver.customers;
	INSERT INTO silver.customers
	(
	CustomerID,
	CustomerName,
	Segment,
	Country,
	City,
	State,
	PostalCode,
	Region
	)
	SELECT DISTINCT
	LTRIM(RTRIM(CustomerID)),
	LTRIM(RTRIM(CustomerName)),
	CASE 
		WHEN UPPER(LTRIM(RTRIM(Segment))) = 'CONSUMER' THEN 'Consumer'
		WHEN UPPER(LTRIM(RTRIM(Segment))) = 'CORPORATE' THEN 'Corporate'
		WHEN UPPER(LTRIM(RTRIM(Segment))) = 'HOME OFFICE' THEN 'Home Office'
		ELSE 'Unknown'
	END,
	LTRIM(RTRIM(Country)),
	CASE
		WHEN City IS NULL OR LTRIM(RTRIM(City)) = '' THEN 'Unknown'
		ELSE LTRIM(RTRIM(City))
	END,
	CASE
		WHEN State IS NULL OR LTRIM(RTRIM(State)) = '' THEN 'Unknown'
		ELSE LTRIM(RTRIM(State))
	END,
	PostalCode,
	LTRIM(RTRIM(Region))
	FROM bronze.customers_raw
	WHERE CustomerID IS NOT NULL;

	
	
---------------------------------------
-- Products
---------------------------------------

	TRUNCATE TABLE silver.products;
	INSERT INTO silver.products
	(
	ProductID,
	ProductName,
	Category,
	SubCategory
	)

	SELECT DISTINCT

	LTRIM(RTRIM(ProductID)),
	LTRIM(RTRIM(ProductName)),

	CASE 
		WHEN UPPER(LTRIM(RTRIM(Category))) = 'FURNITURE' THEN
'Furniture'
  WHEN
UPPER(LTRIM(RTRIM(Category))) = 'OFFICE SUPPLIES' THEN 'Office Supplies'
  WHEN UPPER(LTRIM(RTRIM(Category))) = 'TECHNOLOGY' THEN 'Technology'
  ELSE 'Other'
 END,
 LTRIM(RTRIM(SubCategory))
 FROM bronze.products_raw
 WHERE ProductID IS NOT NULL;
---------------------------------------
-- Orders
---------------------------------------
 
 TRUNCATE TABLE silver.orders;
 INSERT INTO silver.orders
 (
 OrderID,
 CustomerID,
 OrderDate,
 ShipDate,
 ShipMode
 )

 SELECT DISTINCT
 LTRIM(RTRIM(OrderID)),
 LTRIM(RTRIM(CustomerID)),
 OrderDate,
 ShipDate,

 CASE 
  WHEN UPPER(LTRIM(RTRIM(ShipMode))) = 'STANDARD CLASS' THEN 'Standard Class'
  WHEN UPPER(LTRIM(RTRIM(ShipMode))) = 'SECOND CLASS' THEN 'Second Class'
  WHEN UPPER(LTRIM(RTRIM(ShipMode))) = 'FIRST CLASS' THEN 'First Class'
  WHEN UPPER(LTRIM(RTRIM(ShipMode))) = 'SAME DAY' THEN 'Same Day'
  ELSE 'Standard Class'
 END

 FROM bronze.orders_raw
 WHERE OrderID IS NOT NULL;

---------------------------------------
-- OrderDetails
---------------------------------------

 TRUNCATE TABLE silver.orderdetails;
 INSERT INTO silver.orderdetails
 (
 OrderID,
 ProductID,
 Sales,
 Quantity,
 Discount,
 Profit
 )
 SELECT
 LTRIM(RTRIM(OrderID)),
 LTRIM(RTRIM(ProductID)),
 CASE
  WHEN Sales < 0 THEN ABS(Sales)
  ELSE Sales
 END,
 CASE
  WHEN Quantity <= 0 THEN 1
  ELSE Quantity
 END,
 CASE
  WHEN Discount < 0 OR Discount > 1 THEN 0
  ELSE Discount
 END,
 Profit
 FROM bronze.orderdetails_raw
 WHERE OrderID IS NOT NULL
 AND ProductID IS NOT NULL;
 END;

 

exec silver.load_silver

