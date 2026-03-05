create database  SalesDB2;
USE SalesDB2

create table Customers(
CustomerID varchar(50) primary key,
CustomerName  varchar(200),
Segment varchar(50),
Country varchar(50),
City varchar(50),
State varchar(50),
PostalCode varchar(50),
Region varchar(50)

)

create table Products(
    ProductID varchar(50) primary key,
    ProductName varchar(200),
    Category varchar(100),
    SubCategory varchar(100)
)

create table Orders(
OrderID varchar(50) primary key,
OrderDate Date,
ShipDate Date,
ShipMode varchar(50),
CustomerID Varchar(50),
foreign key (CustomerID) references Customers(CustomerID)

)

create table OrderDatails(
OrderDatailID int identity(1,1) primary key ,
OrderID varchar(50),
ProuducID varchar(50),
Sales Float,
Quantity int ,
Discount float,
profit float,
foreign key (OrderID) references Orders(OrderID),
foreign key (ProuducID) references Prouducs(ProuducID)
)
