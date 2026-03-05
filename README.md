# Sales Data Warehouse Project

## Project Overview
This project builds a complete data pipeline from Excel files into a SQL Server Data Warehouse.

## Data Source
Sales data from 2022 to 2025 stored in CSV files.

## Architecture
Excel Files → Python ETL → SQL Server Database → Data Warehouse → Power BI Dashboard

## Technologies Used
- Python
- Pandas
- SQL Server
- Power BI

## Data Warehouse Layers

### Bronze Layer
Stores raw data loaded from the operational database.

### Silver Layer
Data cleaning and transformations.

### Gold Layer
Star schema for analytics:
- Fact_Sales
- Dim_Customers
- Dim_Products

## Dashboard
Power BI dashboard connected to the Gold layer.
