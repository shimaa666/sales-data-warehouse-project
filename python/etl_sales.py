!pip install pandas pyodbc sqlalchemy

import pandas as pd
from sqlalchemy import create_engine
import os

# ----------------- اتصال SQL Server -----------------
server = 'localhost'
database = 'SalesDB2'
username = 'sa'
password = 'sa123456'

engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

print("✅ Connected Successfully")

# ----------------- مسار ملفات CSV -----------------
folder_path = r"E:\data analysis (self study)\Sql\4 Table Denormalized"
files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# ----------------- ETL لكل ملف -----------------
for file in files:
    
    file_path = os.path.join(folder_path, file)
    print(f"🔹 Processing {file} ...")
    
    # قراءة CSV بدون تحويل تاريخ
    df = pd.read_csv(file_path)

    # ----------------- اصلاح مشكلة التاريخ -----------------
    df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce').dt.date
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], errors='coerce').dt.date

    # ----------------- Customers -----------------
    customers = df[['Customer ID','Customer Name','Segment','Country','City','State','Postal Code','Region']].drop_duplicates()
    customers.columns = ['CustomerID','CustomerName','Segment','Country','City','State','PostalCode','Region']

    for col in ['CustomerName','Segment','Country','City','State','Region']:
        customers[col] = customers[col].astype(str).str.strip()

    customers['Segment'] = customers['Segment'].str.upper()

    existing_customers = pd.read_sql("SELECT CustomerID FROM Customers", engine)
    customers = customers[~customers['CustomerID'].isin(existing_customers['CustomerID'])]

    if not customers.empty:
        customers.to_sql('Customers', engine, if_exists='append', index=False)

    # ----------------- Products -----------------
    products = df[['Product ID','Product Name','Category','Sub-Category']].drop_duplicates()
    products.columns = ['ProductID','ProductName','Category','SubCategory']

    for col in ['ProductName','Category','SubCategory']:
        products[col] = products[col].astype(str).str.strip()

    existing_products = pd.read_sql("SELECT ProductID FROM Products", engine)
    products = products[~products['ProductID'].isin(existing_products['ProductID'])]

    if not products.empty:
        products.to_sql('Products', engine, if_exists='append', index=False)

    # ----------------- Orders -----------------
    orders = df[['Order ID','Order Date','Ship Date','Ship Mode','Customer ID']].drop_duplicates()
    orders.columns = ['OrderID','OrderDate','ShipDate','ShipMode','CustomerID']

    orders['OrderID'] = orders['OrderID'].astype(str).str.strip()
    orders['ShipMode'] = orders['ShipMode'].astype(str).str.strip()
    orders['CustomerID'] = orders['CustomerID'].astype(str).str.strip()

    # التاريخ بعد الإصلاح
    orders['OrderDate'] = pd.to_datetime(orders['OrderDate'], errors='coerce').dt.date
    orders['ShipDate'] = pd.to_datetime(orders['ShipDate'], errors='coerce').dt.date

    existing_orders = pd.read_sql("SELECT OrderID FROM Orders", engine)
    orders = orders[~orders['OrderID'].isin(existing_orders['OrderID'])]

    if not orders.empty:
        orders.to_sql('Orders', engine, if_exists='append', index=False)

    # ----------------- OrderDetails -----------------
    details = df[['Order ID','Product ID','Sales','Quantity','Discount','Profit']].drop_duplicates()
    details.columns = ['OrderID','ProductID','Sales','Quantity','Discount','Profit']

    details['OrderID'] = details['OrderID'].astype(str).str.strip()
    details['ProductID'] = details['ProductID'].astype(str).str.strip()

    existing_details = pd.read_sql("SELECT OrderID, ProductID FROM OrderDetails", engine)

    details = details.merge(existing_details, on=['OrderID','ProductID'], how='left', indicator=True)
    details = details[details['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not details.empty:
        details.to_sql('OrderDetails', engine, if_exists='append', index=False)

    print(f"✅ {file} loaded successfully!\n")

print("🎉 All CSV files processed successfully without duplicates!")