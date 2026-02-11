"""
This Module is responsible for the data Extraction of the database.

It does:
- Connects to the SQLite base
- Executes SQL queries
- Returns pandas dataframes

"""

import sqlite3
import pandas as pd


class DataExtractor:
    """
    This class manages the data extraction
    """

    def __init__(self, db_path):
        
        self.db_path = db_path

        #sqlite3 connection
        self.conn = sqlite3.connect(db_path)

        print(f"Connected to the database: {db_path}")

    def extract_sales_data(self):
        """
        Extract complete sales data joining multiple tables.
        It makes a query and then returns a pandas dataframe with all the sales. 

        """
        print("Extracting sales data...")

        query = """
        SELECT
            -- Order Data
            o.OrderID,
            o.OrderDate,
            o.ShippedDate,
            o.ShipCountry,
            o.Freight,

            -- Customers Data
            c.CustomerID,
            c.CompanyName,
            c.ContactName,
            c.Country AS CustomerCountry,
            c.City AS CustomerCity,

            -- Product Data
            p.ProductID,
            p.ProductName,
            p.UnitPrice AS ProductUnitPrice,

            -- Category Data
            cat.CategoryID,
            cat.CategoryName,
            cat.Description AS CategoryDescription,

            -- Supplier Data
            s.CompanyName AS SupplierName,
            s.Country AS SupplierCountry,

            -- Order Detail Data
            od.Quantity,
            od.UnitPrice,
            od.Discount,

            -- Calculations
            (od.Quantity * od.UnitPrice * (1 - od.Discount)) AS Total,
            (od.Quantity * od.UnitPrice * od.Discount) AS DiscountAmount

        From Orders o
        INNER JOIN "Order Details" od ON o.OrderID = od.OrderID
        INNER JOIN Products p ON od.ProductID = p.ProductID
        INNER JOIN Categories cat ON p.CategoryID = cat.CategoryID
        INNER JOIN Customers c ON o.CustomerID = c.CustomerID
        INNER JOIN Suppliers s ON p.SupplierID = s.SupplierID

        WHERE 
            o.OrderDate IS NOT NULL
            AND od.Quantity > 0
            AND od.UnitPrice >= 0

        ORDER BY o.OrderDate DESC;
        
        """
        df = pd.read_sql_query(query , self.conn ) 
        print(f"✅ {len(df):,} extracted registers!\n")
        print(f"Period: {df['OrderDate'].min()} until {df['OrderDate'].max()}")

        return df
    
    def extract_customers_summary(self):
        """

        Extracts an aggregated custommer summary.
        Ex:
        Customer A makes 5 orders = GROUP BY the 5 orders in 1 line
        """
        print("Extracting customers summary...")

        query = """
        SELECT 
            c.CustomerID,
            c.CompanyName,
            c.Country,
            c.City,
            COUNT(DISTINCT o.OrderID) AS TotalOrders,
            SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalRevenue,
            AVG(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS AvgOrderValue,
            MAX(o.OrderDate) AS LastOrderDate

        FROM Customers c
        LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
        LEFT JOIN "Order Details" od ON o.OrderID = od.OrderID

        GROUP BY 
            c.CustomerID, 
            c.CompanyName, 
            c.Country,
            c.City

        ORDER BY TotalRevenue DESC;


        """
        df = pd.read_sql_query(query, self.conn)
    
        print(f"✅ {len(df)} extracted clients!")
    
        return df
    
    def extract_products_summary(self):
        """
        Extracts the products summary (sales, quantity, Revenue)
        """
        print("Extracting product summary!")

        query = """
        SELECT 
            p.ProductID,
            p.ProductName,
            cat.CategoryName,
            s.CompanyName AS SupplierName,
            COUNT(DISTINCT o.OrderID) AS TimesOrdered,
            SUM(od.Quantity) AS TotalUnitsSold,
            SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) AS TotalRevenue,
            AVG(od.UnitPrice) AS AvgSellingPrice
        
        FROM Products p
        INNER JOIN Categories cat ON p.CategoryID = cat.CategoryID
        INNER JOIN Suppliers s ON p.SupplierID = s.SupplierID
        LEFT JOIN "Order Details" od ON p.ProductID = od.ProductID
        LEFT JOIN Orders o ON od.OrderID = o.OrderID
        
        GROUP BY 
            p.ProductID,
            p.ProductName,
            cat.CategoryName,
            s.CompanyName

        ORDER BY TotalRevenue DESC;
        """
        df = pd.read_sql_query(query, self.conn)

        print(f"✅ {len(df)} products extracted")
    
        return df
    
    def get_database_info(self):
        """
        Returns info about the database(tables, count).
        
        """
        
        print("\n" + "="*60)
        print("📊 DATABASE INFO")
        print("="*60)
        
        # Lists all the tables
        query_tables = """
        SELECT name 
        FROM sqlite_master 
        WHERE type='table'
        ORDER BY name
        """
        
        tables = pd.read_sql_query(query_tables, self.conn)
        
        print(f"\n📁 Total of tables: {len(tables)}")
        print("\nAvailable Tables:")
        for idx, table in enumerate(tables['name'], 1):
            # Counts the registers in each table
            # ASPAS DUPLAS ao redor de {table} para lidar com espaços
            count_query = f'SELECT COUNT(*) as count FROM "{table}"'
            count = pd.read_sql_query(count_query, self.conn)['count'][0]
            print(f"  {idx}. {table:20s} - {count:,} registers")
        
        print("="*60 + "\n")
    
    
    def close(self):
        """
        Closes the connection with the database!
        
        """
        self.conn.close()
        print("🔒 Connection with db closed!")
    
    # ========== MODULE TEST==========

if __name__ == "__main__":
    
    print("\n" + "="*60)
    print("🧪 TESTING EXTRACTION MODULE")
    print("="*60 + "\n")
    
    # Creates an instance of the extractor
    extractor = DataExtractor('data/raw/northwind.db')
    
    # Shows database info
    extractor.get_database_info()
    
    # Tests sales extraction
    print("\n--- Test 1: Sales Data ---")
    df_sales = extractor.extract_sales_data()
    print("\n📋 First 3 rows:")
    print(df_sales.head(3))
    print(f"\n📊 Shape: {df_sales.shape} (rows, columns)")
    print(f"📊 Columns: {list(df_sales.columns)}")
    
    # Tests customer extraction
    print("\n--- Test 2: Customers Summary ---")
    df_customers = extractor.extract_customers_summary()
    print("\n📋 Top 5 customers:")
    print(df_customers.head(5)[['CompanyName', 'TotalOrders', 'TotalRevenue']])
    
    # Tests product extraction
    print("\n--- Test 3: Products Summary ---")
    df_products = extractor.extract_products_summary()
    print("\n📋 Top 5 products:")
    print(df_products.head(5)[['ProductName', 'TotalUnitsSold', 'TotalRevenue']])
    
    # Closes connection
    extractor.close()
    
    print("\n" + "="*60)
    print("✅ ALL TESTS PASSED!")
    print("="*60 + "\n")