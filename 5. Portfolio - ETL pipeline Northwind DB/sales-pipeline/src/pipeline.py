# src/pipeline.py

"""
Complete ETL Pipeline - Sales Analysis

This pipeline:
1. EXTRACTS data from SQLite database
2. TRANSFORMS data (cleaning + feature engineering)
3. LOADS processed data and reports

Author: João Victor Assunção Pereira
Date: 02/2026
"""

import time
from datetime import datetime
from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader


def print_header(title):
    """Prints formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")


def print_step(step_num, total_steps, description):
    """Prints formatted step indicator"""
    print(f"\n[{step_num}/{total_steps}] {description}")
    print("-" * 70)


def run_etl_pipeline(db_path='data/raw/northwind.db', output_dir='data/processed'):
    """
    Runs the complete ETL pipeline.
    
    Parameters:
    -----------
    db_path : str
        Path to the SQLite database
    output_dir : str
        Directory to save processed data
    
    Returns:
    --------
    dict
        Pipeline execution statistics
    """
    
    # ========== INITIALIZATION ==========
    print_header("🚀 ETL PIPELINE - SALES ANALYSIS")
    
    start_time = time.time()
    pipeline_stats = {
        'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'Running'
    }
    
    print(f"📅 Started at: {pipeline_stats['start_time']}")
    print(f"📂 Database: {db_path}")
    print(f"📂 Output: {output_dir}")
    
    try:
        # ========== STEP 1: EXTRACT ==========
        print_step(1, 3, "📥 EXTRACT - Extracting data from database")
        
        extractor = DataExtractor(db_path)
        df_raw = extractor.extract_sales_data()
        extractor.close()
        
        pipeline_stats['raw_records'] = len(df_raw)
        pipeline_stats['raw_columns'] = len(df_raw.columns)
        
        print(f"\n✅ Extraction completed!")
        print(f"   Records: {pipeline_stats['raw_records']:,}")
        print(f"   Columns: {pipeline_stats['raw_columns']}")
        
        # ========== STEP 2: TRANSFORM ==========
        print_step(2, 3, "🔄 TRANSFORM - Cleaning and engineering features")
        
        transformer = DataTransformer()
        
        # 2.1 Cleaning
        print("\n➤ Cleaning data...")
        df_clean = transformer.clean_sales_data(df_raw)
        
        # 2.2 Temporal features
        print("\n➤ Creating temporal features...")
        df_time = transformer.create_time_features(df_clean)
        
        # 2.3 Business features
        print("\n➤ Creating business features...")
        df_final = transformer.create_business_features(df_time)
        
        # 2.4 Quality report
        print("\n➤ Generating quality report...")
        transformer.get_data_quality_report(df_final)
        
        pipeline_stats['clean_records'] = len(df_final)
        pipeline_stats['final_columns'] = len(df_final.columns)
        pipeline_stats['records_removed'] = pipeline_stats['raw_records'] - pipeline_stats['clean_records']
        pipeline_stats['features_created'] = pipeline_stats['final_columns'] - pipeline_stats['raw_columns']
        
        print(f"\n✅ Transformation completed!")
        print(f"   Clean records: {pipeline_stats['clean_records']:,}")
        print(f"   Records removed: {pipeline_stats['records_removed']:,}")
        print(f"   Features created: {pipeline_stats['features_created']}")
        print(f"   Final columns: {pipeline_stats['final_columns']}")
        
        # ========== STEP 3: LOAD ==========
        print_step(3, 3, "💾 LOAD - Saving processed data and reports")
        
        loader = DataLoader(output_dir=output_dir)
        loader.save_complete_output(df_final)
        
        pipeline_stats['output_dir'] = output_dir
        
        # ========== SUCCESS ==========
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        pipeline_stats['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        pipeline_stats['duration_seconds'] = round(elapsed_time, 2)
        pipeline_stats['duration_formatted'] = f"{int(elapsed_time // 60)}m {int(elapsed_time % 60)}s"
        pipeline_stats['status'] = 'Success'
        
        print_header("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        
        print("📊 EXECUTION SUMMARY:")
        print(f"   Started: {pipeline_stats['start_time']}")
        print(f"   Ended: {pipeline_stats['end_time']}")
        print(f"   Duration: {pipeline_stats['duration_formatted']}")
        print(f"   Records processed: {pipeline_stats['clean_records']:,}")
        print(f"   Features created: {pipeline_stats['features_created']}")
        print(f"\n📂 Output files in: {output_dir}/")
        print(f"   ➜ sales_complete.csv")
        print(f"   ➜ sales_reports.xlsx")
        print(f"   ➜ sales_complete_[timestamp].csv")
        
        print("\n💡 Next steps:")
        print("   1. Open sales_reports.xlsx to view analyses")
        print("   2. Use sales_complete.csv for further analysis")
        print("   3. Check notebooks/ for exploratory analysis")
        
        return pipeline_stats
        
    except Exception as e:
        # ========== ERROR HANDLING ==========
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        pipeline_stats['status'] = 'Failed'
        pipeline_stats['error'] = str(e)
        pipeline_stats['duration_seconds'] = round(elapsed_time, 2)
        
        print_header("❌ PIPELINE FAILED")
        print(f"Error: {e}")
        print(f"\nDuration before failure: {elapsed_time:.2f}s")
        
        raise


def run_quick_test():
    """
    Runs a quick test with sample data (first 10000 records).
    Useful for testing without processing all data.
    """
    
    print_header("🧪 QUICK TEST MODE - Processing sample data")
    
    # Extract
    extractor = DataExtractor('data/raw/northwind.db')
    df_raw = extractor.extract_sales_data()
    extractor.close()
    
    # Sample first 10000 records
    df_sample = df_raw.head(10000).copy()
    print(f"📊 Using sample: {len(df_sample):,} records (from {len(df_raw):,} total)")
    
    # Transform
    transformer = DataTransformer()
    df_clean = transformer.clean_sales_data(df_sample)
    df_time = transformer.create_time_features(df_clean)
    df_final = transformer.create_business_features(df_time)
    
    # Load
    loader = DataLoader(output_dir='data/processed/test')
    loader.save_complete_output(df_final)
    
    print_header("✅ QUICK TEST COMPLETED")
    print(f"📂 Test outputs in: data/processed/test/")


# ========== MAIN EXECUTION ==========
if __name__ == "__main__":
    
    import sys
    
    # Check if test mode
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Run quick test
        run_quick_test()
    else:
        # Run full pipeline
        stats = run_etl_pipeline()
        
        # Optionally save stats
        import json
        stats_file = f"data/processed/pipeline_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"\n📊 Pipeline statistics saved to: {stats_file}")