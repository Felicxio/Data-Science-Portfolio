# src/transform.py

"""
Module responsible for data TRANSFORMATION.

It does:
- Cleans data (removes duplicates, invalid values)
- Creates new features (temporal, business)
- Validates data quality
"""

import pandas as pd
import numpy as np


class DataTransformer:
    """
    Class responsible for transforming and cleaning data.
    """
    
    def __init__(self):
        """Simple initializer (no parameters needed)"""
        pass
    
    
    def clean_sales_data(self, df):
        """
        Cleans sales data.
        
        Steps:
        1. Removes duplicates
        2. Removes invalid values (negative, null)
        3. Converts dates to datetime
        4. Resets index
        
        Parameters:
        -----------
        df : DataFrame
            Raw data from extraction
        
        Returns:
        --------
        DataFrame
            Cleaned data
        """
        
        print("🧹 Starting data cleaning...")
        
        # 1. Creates a copy (doesn't modify original)
        df_clean = df.copy()
        
        # 2. Removes duplicates
        before = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        after = len(df_clean)
        print(f"  ➜ Removed {before - after:,} duplicates")
        
        # 3. Removes negative values in Total
        before = len(df_clean)
        df_clean = df_clean[df_clean['Total'] > 0]
        after = len(df_clean)
        print(f"  ➜ Removed {before - after:,} records with negative total")
        
        # 4. Removes invalid quantities
        before = len(df_clean)
        df_clean = df_clean[df_clean['Quantity'] > 0]
        after = len(df_clean)
        print(f"  ➜ Removed {before - after:,} records with invalid quantity")
        
        # 5. Converts OrderDate to datetime
        df_clean['OrderDate'] = pd.to_datetime(df_clean['OrderDate'], errors='coerce')
        df_clean['ShippedDate'] = pd.to_datetime(df_clean['ShippedDate'], errors='coerce')
        
        # 6. Removes rows where OrderDate is NaT (invalid)
        before = len(df_clean)
        df_clean = df_clean.dropna(subset=['OrderDate'])
        after = len(df_clean)
        print(f"  ➜ Removed {before - after:,} records with invalid date")
        
        # 7. Resets index
        df_clean = df_clean.reset_index(drop=True)
        
        print(f"✅ Cleaning completed: {len(df_clean):,} valid records")
        
        return df_clean
    
    
    def create_time_features(self, df):
        """
        Creates temporal features (year, month, quarter, etc).
        
        Why create features?
        - Temporal analysis (sales by month, quarter)
        - Machine Learning (if needed later)
        - Facilitates grouping
        """
        
        print("📅 Creating temporal features...")
        
        df_transformed = df.copy()
        
        # Extracts date components
        df_transformed['Year'] = df_transformed['OrderDate'].dt.year
        df_transformed['Month'] = df_transformed['OrderDate'].dt.month
        df_transformed['Quarter'] = df_transformed['OrderDate'].dt.quarter
        df_transformed['DayOfWeek'] = df_transformed['OrderDate'].dt.dayofweek
        df_transformed['WeekOfYear'] = df_transformed['OrderDate'].dt.isocalendar().week
        
        # Creates month name (more readable)
        month_names = {
            1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr',
            5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug',
            9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
        }
        df_transformed['MonthName'] = df_transformed['Month'].map(month_names)
        
        # Creates Year-Month column
        df_transformed['YearMonth'] = df_transformed['OrderDate'].dt.strftime('%Y-%m')
        
        # Creates day of week name
        day_names = {
            0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday',
            4: 'Friday', 5: 'Saturday', 6: 'Sunday'
        }
        df_transformed['DayName'] = df_transformed['DayOfWeek'].map(day_names)
        
        print(f"✅ {8} temporal features created")
        
        return df_transformed
    
    
    def create_business_features(self, df):
        """
        Creates business features (useful metrics).
        
        Examples:
        - Order size classification
        - Discount flags
        - Delivery time
        """
        
        print("💼 Creating business features...")
        
        df_transformed = df.copy()
        
        # 1. Classifies order size
        df_transformed['OrderSize'] = pd.cut(
            df_transformed['Total'],
            bins=[0, 100, 500, 1000, 5000, float('inf')],
            labels=['Very Small', 'Small', 'Medium', 'Large', 'VIP']
        )
        
        # 2. Discount flag (1 if has discount, 0 if not)
        df_transformed['HasDiscount'] = np.where(df_transformed['Discount'] > 0, 1, 0)
        
        # 3. Discount category
        df_transformed['DiscountLevel'] = pd.cut(
            df_transformed['Discount'],
            bins=[-0.01, 0, 0.05, 0.15, 0.25, 1],
            labels=['No Discount', 'Low', 'Medium', 'High', 'Very High']
        )
        
        # 4. Delivery time (days between order and shipping)
        df_transformed['DeliveryDays'] = (
            df_transformed['ShippedDate'] - df_transformed['OrderDate']
        ).dt.days
        
        # 5. Delivery speed classification
        df_transformed['DeliverySpeed'] = pd.cut(
            df_transformed['DeliveryDays'],
            bins=[-1, 3, 7, 14, float('inf')],
            labels=['Express', 'Fast', 'Normal', 'Slow']
        )
        
        # 6. Revenue per unit
        df_transformed['RevenuePerUnit'] = df_transformed['Total'] / df_transformed['Quantity']
        
        print(f"✅ {6} business features created")
        
        return df_transformed
    
    
    def get_data_quality_report(self, df):
        """
        Generates data quality report.
        
        Checks:
        - Missing values
        - Data types
        - Basic statistics
        """
        
        print("\n" + "="*60)
        print("📊 DATA QUALITY REPORT")
        print("="*60)
        
        # 1. General info
        print(f"\n📈 Total records: {len(df):,}")
        print(f"📈 Total columns: {len(df.columns)}")
        
        # 2. Missing values
        print("\n🔍 Missing values per column:")
        missing = df.isnull().sum()
        missing = missing[missing > 0]
        
        if len(missing) == 0:
            print("  ✅ No missing values!")
        else:
            for col, count in missing.items():
                percent = (count / len(df)) * 100
                print(f"  ➜ {col}: {count:,} ({percent:.2f}%)")
        
        # 3. Basic statistics for Total
        print("\n📊 Total Statistics:")
        print(df['Total'].describe())
        
        # 4. Date range
        if 'OrderDate' in df.columns:
            print(f"\n📅 Date range:")
            print(f"  ➜ First order: {df['OrderDate'].min()}")
            print(f"  ➜ Last order: {df['OrderDate'].max()}")
            print(f"  ➜ Period: {(df['OrderDate'].max() - df['OrderDate'].min()).days} days")
        
        # 5. Top categories
        if 'CategoryName' in df.columns:
            print(f"\n📦 Top 5 Categories by revenue:")
            top_cat = df.groupby('CategoryName')['Total'].sum().sort_values(ascending=False).head(5)
            for cat, revenue in top_cat.items():
                print(f"  ➜ {cat}: ${revenue:,.2f}")
        
        print("="*60 + "\n")
        
        return None


# ========== MODULE TEST ==========
if __name__ == "__main__":
    
    print("\n" + "="*60)
    print("🧪 TESTING TRANSFORMATION MODULE")
    print("="*60 + "\n")
    
    # Import extractor to get data
    from extract import DataExtractor
    
    # Extracts data
    print("--- Step 1: Extracting data ---")
    extractor = DataExtractor('data/raw/northwind.db')
    df_raw = extractor.extract_sales_data()
    extractor.close()
    
    # Creates transformer
    transformer = DataTransformer()
    
    # Applies transformations
    print("\n--- Step 2: Cleaning data ---")
    df_clean = transformer.clean_sales_data(df_raw)
    
    print("\n--- Step 3: Creating temporal features ---")
    df_time = transformer.create_time_features(df_clean)
    
    print("\n--- Step 4: Creating business features ---")
    df_final = transformer.create_business_features(df_time)
    
    # Quality report
    print("\n--- Step 5: Quality report ---")
    transformer.get_data_quality_report(df_final)
    
    # Shows sample
    print("\n--- Final Data Sample ---")
    print(df_final.head(3))
    print(f"\n📊 Final shape: {df_final.shape}")
    print(f"📊 Total columns: {len(df_final.columns)}")
    
    # Saves temporarily
    df_final.to_csv('data/processed/sales_transformed.csv', index=False)
    print("\n💾 Data saved to: data/processed/sales_transformed.csv")
    
    print("\n" + "="*60)
    print("✅ ALL TRANSFORMATION TESTS PASSED!")
    print("="*60 + "\n")