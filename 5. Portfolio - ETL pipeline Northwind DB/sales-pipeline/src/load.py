# src/load.py

"""
Module responsible for LOADING processed data.

It does:
- Saves data in different formats (CSV, Excel)
- Creates summary reports
- Exports aggregated metrics
"""

import pandas as pd
import os
from datetime import datetime


class DataLoader:
    """
    Class responsible for loading/saving processed data.
    """
    
    def __init__(self, output_dir='data/processed'):
        """
        Initializes with output directory.
        
        Parameters:
        -----------
        output_dir : str
            Where to save files
        """
        self.output_dir = output_dir
        
        # Creates directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        print(f"📁 Output directory: {output_dir}")
    
    
    def save_to_csv(self, df, filename):
        """
        Saves DataFrame to CSV.
        
        CSV = Comma Separated Values
        - Universal format
        - Opens in Excel, Python, R, etc
        - Lightweight
        """
        filepath = os.path.join(self.output_dir, filename)
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"💾 CSV saved: {filepath} ({file_size:.2f} MB)")
        
        return filepath
    
    
    def save_to_excel(self, dataframes_dict, filename):
        """
        Saves multiple DataFrames to Excel (multiple sheets).
        
        Parameters:
        -----------
        dataframes_dict : dict
            Dictionary with {sheet_name: dataframe}
        filename : str
            Excel file name
        
        Example:
        --------
        loader.save_to_excel({
            'Sales': df_sales,
            'Summary': df_summary
        }, 'report.xlsx')
        """
        filepath = os.path.join(self.output_dir, filename)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            for sheet_name, df in dataframes_dict.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        print(f"💾 Excel saved: {filepath} ({file_size:.2f} MB)")
        print(f"   Sheets: {list(dataframes_dict.keys())}")
        
        return filepath
    
    
    def create_summary_report(self, df):
        """
        Creates summary report with key metrics.
        
        Returns:
        --------
        DataFrame with aggregated metrics
        """
        print("📊 Generating summary report...")
        
        summary = {
            'Metric': [],
            'Value': []
        }
        
        # General metrics
        summary['Metric'].append('Total Revenue')
        summary['Value'].append(f"${df['Total'].sum():,.2f}")
        
        summary['Metric'].append('Number of Orders')
        summary['Value'].append(f"{df['OrderID'].nunique():,}")
        
        summary['Metric'].append('Number of Customers')
        summary['Value'].append(f"{df['CustomerID'].nunique():,}")
        
        summary['Metric'].append('Number of Products')
        summary['Value'].append(f"{df['ProductID'].nunique():,}")
        
        summary['Metric'].append('Average Order Value')
        summary['Value'].append(f"${df.groupby('OrderID')['Total'].sum().mean():,.2f}")
        
        summary['Metric'].append('Average Items per Order')
        summary['Value'].append(f"{df.groupby('OrderID')['Quantity'].sum().mean():.2f}")
        
        summary['Metric'].append('Total Units Sold')
        summary['Value'].append(f"{df['Quantity'].sum():,}")
        
        summary['Metric'].append('Date Range')
        # Converte para datetime primeiro (caso seja string)
        date_col = pd.to_datetime(df['OrderDate'])
        summary['Value'].append(f"{date_col.min().date()} to {date_col.max().date()}")
        
        summary_df = pd.DataFrame(summary)
        
        print(f"✅ Summary created with {len(summary_df)} metrics")
        
        return summary_df
    
    
    def create_category_analysis(self, df):
        """
        Creates analysis by product category.
        """
        print("📦 Analyzing categories...")
        
        category_stats = df.groupby('CategoryName').agg({
            'OrderID': 'nunique',
            'Quantity': 'sum',
            'Total': 'sum',
            'DiscountAmount': 'sum'
        }).reset_index()
        
        category_stats.columns = ['Category', 'Orders', 'Units Sold', 'Revenue', 'Total Discount']
        
        # Calculates percentages
        category_stats['Revenue %'] = (
            category_stats['Revenue'] / category_stats['Revenue'].sum() * 100
        ).round(2)
        
        # Sorts by revenue
        category_stats = category_stats.sort_values('Revenue', ascending=False)
        
        print(f"✅ {len(category_stats)} categories analyzed")
        
        return category_stats
    
    
    def create_monthly_analysis(self, df):
        """
        Creates monthly sales analysis.
        """
        print("📅 Analyzing monthly trends...")
        
        monthly_stats = df.groupby('YearMonth').agg({
            'OrderID': 'nunique',
            'Total': 'sum',
            'Quantity': 'sum'
        }).reset_index()
        
        monthly_stats.columns = ['Month', 'Orders', 'Revenue', 'Units Sold']
        
        # Sorts by month
        monthly_stats = monthly_stats.sort_values('Month')
        
        print(f"✅ {len(monthly_stats)} months analyzed")
        
        return monthly_stats
    
    
    def create_top_products(self, df, top_n=20):
        """
        Creates ranking of top products.
        """
        print(f"🏆 Finding top {top_n} products...")
        
        product_stats = df.groupby('ProductName').agg({
            'OrderID': 'nunique',
            'Quantity': 'sum',
            'Total': 'sum'
        }).reset_index()
        
        product_stats.columns = ['Product', 'Orders', 'Units Sold', 'Revenue']
        
        # Top by revenue
        top_products = product_stats.sort_values('Revenue', ascending=False).head(top_n)
        
        print(f"✅ Top {top_n} products identified")
        
        return top_products
    
    
    def create_top_customers(self, df, top_n=20):
        """
        Creates ranking of top customers.
        """
        print(f"👥 Finding top {top_n} customers...")
        
        customer_stats = df.groupby(['CustomerID', 'CompanyName']).agg({
            'OrderID': 'nunique',
            'Total': 'sum',
            'OrderDate': 'max'
        }).reset_index()
        
        customer_stats.columns = ['CustomerID', 'Company', 'Orders', 'Revenue', 'Last Order']
        
        # Top by revenue
        top_customers = customer_stats.sort_values('Revenue', ascending=False).head(top_n)
        
        print(f"✅ Top {top_n} customers identified")
        
        return top_customers
    
    
    def create_complete_report(self, df):
        """
        Creates complete report with multiple analyses.
        
        Returns:
        --------
        dict with multiple DataFrames
        """
        print("\n" + "="*60)
        print("📊 CREATING COMPLETE REPORT")
        print("="*60 + "\n")
        
        report = {}
        
        report['Summary'] = self.create_summary_report(df)
        report['Categories'] = self.create_category_analysis(df)
        report['Monthly'] = self.create_monthly_analysis(df)
        report['Top Products'] = self.create_top_products(df)
        report['Top Customers'] = self.create_top_customers(df)
        
        print("\n✅ Complete report created with 5 analyses")
        
        return report
    
    
    def save_complete_output(self, df):
        """
        Saves all outputs (CSV + Excel with reports).
        
        This is the main method to use in the pipeline.
        """
        print("\n" + "="*60)
        print("💾 SAVING ALL OUTPUTS")
        print("="*60 + "\n")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 1. Saves complete data in CSV
        print("--- Saving complete data ---")
        self.save_to_csv(df, 'sales_complete.csv')
        
        # 2. Creates and saves reports in Excel
        print("\n--- Creating reports ---")
        reports = self.create_complete_report(df)
        
        print("\n--- Saving reports to Excel ---")
        self.save_to_excel(reports, 'sales_reports.xlsx')
        
        # 3. Also saves a version with timestamp (history)
        print("\n--- Saving versioned copy ---")
        self.save_to_csv(df, f'sales_complete_{timestamp}.csv')
        
        print("\n" + "="*60)
        print("✅ ALL OUTPUTS SAVED SUCCESSFULLY")
        print("="*60)
        print(f"\n📂 Files created in: {self.output_dir}/")
        print("  ➜ sales_complete.csv (full data)")
        print("  ➜ sales_reports.xlsx (5 analysis sheets)")
        print(f"  ➜ sales_complete_{timestamp}.csv (versioned backup)")
        print("")


# ========== MODULE TEST ==========
if __name__ == "__main__":
    
    print("\n" + "="*60)
    print("🧪 TESTING LOAD MODULE")
    print("="*60 + "\n")
    
    # Loads transformed data
    print("--- Loading transformed data ---")
    df = pd.read_csv('data/processed/sales_transformed.csv')
    print(f"✅ Loaded {len(df):,} records")
    
    # Creates loader
    loader = DataLoader()
    
    # Saves complete output
    loader.save_complete_output(df)
    
    print("\n" + "="*60)
    print("✅ ALL LOAD TESTS PASSED!")
    print("="*60 + "\n")