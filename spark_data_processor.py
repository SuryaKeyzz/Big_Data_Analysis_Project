from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TBDataProcessor:
    """
    Apache Spark-based data processor for TB epidemic data
    Handles cleaning, transformation, and feature engineering
    """
    
    def __init__(self, app_name: str = "TB_Data_Processor"):
        self.spark = self._initialize_spark(app_name)
        self.processed_data_dir = Path('data/processed')
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
    def _initialize_spark(self, app_name: str) -> SparkSession:
        """Initialize Spark session with appropriate configurations"""
        logger.info("Initializing Spark session...")
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        # Set log level to reduce verbose output
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark session initialized: {spark.version}")
        return spark
    
    def load_raw_data(self, tb_data: pd.DataFrame, population_data: pd.DataFrame):
        """Load raw data into Spark DataFrames"""
        logger.info("Loading raw data into Spark DataFrames...")
        
        # Convert pandas DataFrames to Spark DataFrames
        tb_schema = StructType([
            StructField("country", StringType(), True),
            StructField("iso3", StringType(), True),
            StructField("g_whoregion", StringType(), True),
            StructField("indicator", StringType(), True),
            StructField("year", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        population_schema = StructType([
            StructField("country", StringType(), True),
            StructField("iso3", StringType(), True),
            StructField("year", StringType(), True),
            StructField("population", LongType(), True)
        ])
        
        # Create Spark DataFrames
        self.tb_df = self.spark.createDataFrame(tb_data, schema=tb_schema)
        self.pop_df = self.spark.createDataFrame(population_data, schema=population_schema)
        
        logger.info(f"Loaded TB data: {self.tb_df.count()} records")
        logger.info(f"Loaded population data: {self.pop_df.count()} records")
        
    def clean_tb_data(self):
        """Clean and validate TB data"""
        logger.info("Cleaning TB data...")
        
        # Remove null values and invalid data
        self.tb_df_clean = self.tb_df.filter(
            (col("value").isNotNull()) & 
            (col("value") >= 0) &
            (col("year").isNotNull()) &
            (col("iso3").isNotNull())
        )
        
        # Convert year to integer
        self.tb_df_clean = self.tb_df_clean.withColumn(
            "year", col("year").cast(IntegerType())
        )
        
        # Standardize country names
        self.tb_df_clean = self.tb_df_clean.withColumn(
            "country", 
            when(col("country") == "Lao People's Democratic Republic", "Laos")
            .when(col("country") == "Lao PDR", "Laos")
            .otherwise(col("country"))
        )
        
        logger.info(f"Cleaned TB data: {self.tb_df_clean.count()} records")
        
    def clean_population_data(self):
        """Clean and validate population data"""
        logger.info("Cleaning population data...")
        
        # Remove null values
        self.pop_df_clean = self.pop_df.filter(
            (col("population").isNotNull()) & 
            (col("population") > 0) &
            (col("year").isNotNull()) &
            (col("iso3").isNotNull())
        )
        
        # Convert year to integer
        self.pop_df_clean = self.pop_df_clean.withColumn(
            "year", col("year").cast(IntegerType())
        )
        
        # Standardize country names
        self.pop_df_clean = self.pop_df_clean.withColumn(
            "country",
            when(col("country") == "Lao People's Democratic Republic", "Laos")
            .when(col("country") == "Lao PDR", "Laos")
            .otherwise(col("country"))
        )
        
        logger.info(f"Cleaned population data: {self.pop_df_clean.count()} records")
        
    def create_pivoted_tb_data(self):
        """Pivot TB data to have indicators as columns"""
        logger.info("Creating pivoted TB data...")
        
        # Pivot the data to have indicators as columns
        self.tb_pivoted = self.tb_df_clean.groupBy("country", "iso3", "year") \
            .pivot("indicator") \
            .agg(first("value")) \
            .na.fill(0)
        
        # Rename columns for clarity
        column_mapping = {
            "e_inc_num": "total_cases",
            "c_newinc": "new_cases", 
            "e_mort_num": "deaths",
            "e_prev_num": "prevalence"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in self.tb_pivoted.columns:
                self.tb_pivoted = self.tb_pivoted.withColumnRenamed(old_col, new_col)
        
        # Ensure all required columns exist
        required_columns = ["total_cases", "new_cases", "deaths", "prevalence"]
        for col_name in required_columns:
            if col_name not in self.tb_pivoted.columns:
                self.tb_pivoted = self.tb_pivoted.withColumn(col_name, lit(0))
        
        logger.info(f"Pivoted TB data: {self.tb_pivoted.count()} records")
        
    def join_with_population(self):
        """Join TB data with population data"""
        logger.info("Joining TB data with population data...")
        
        # Join TB data with population data
        self.combined_df = self.tb_pivoted.join(
            self.pop_df_clean.select("iso3", "year", "population"),
            on=["iso3", "year"],
            how="left"
        )
        
        # Fill missing population with 0 (will be handled in rate calculations)
        self.combined_df = self.combined_df.na.fill({"population": 0})
        
        logger.info(f"Combined data: {self.combined_df.count()} records")
        
    def calculate_rates(self):
        """Calculate rates per 100,000 population"""
        logger.info("Calculating rates per 100,000 population...")
        
        # Calculate rates per 100,000 people
        self.final_df = self.combined_df.withColumn(
            "total_cases_per_100k",
            when(col("population") > 0, 
                 (col("total_cases") * 100000 / col("population")).cast(DecimalType(10, 2)))
            .otherwise(0)
        ).withColumn(
            "new_cases_per_100k",
            when(col("population") > 0,
                 (col("new_cases") * 100000 / col("population")).cast(DecimalType(10, 2)))
            .otherwise(0)
        ).withColumn(
            "deaths_per_100k", 
            when(col("population") > 0,
                 (col("deaths") * 100000 / col("population")).cast(DecimalType(10, 2)))
            .otherwise(0)
        ).withColumn(
            "case_fatality_rate",
            when(col("total_cases") > 0,
                 (col("deaths") * 100 / col("total_cases")).cast(DecimalType(5, 2)))
            .otherwise(0)
        )
        
        # Add additional calculated fields
        self.final_df = self.final_df.withColumn(
            "new_case_rate",
            when(col("total_cases") > 0,
                 (col("new_cases") * 100 / col("total_cases")).cast(DecimalType(5, 2)))
            .otherwise(0)
        )
        
        logger.info("Rate calculations completed")
        
    def create_aggregated_summaries(self):
        """Create aggregated summary tables"""
        logger.info("Creating aggregated summaries...")
        
        # Country-level aggregations (latest year)
        latest_year = self.final_df.agg(max("year")).collect()[0][0]
        
        self.country_summary = self.final_df.filter(col("year") == latest_year) \
            .select(
                "country", "iso3", "year",
                "total_cases", "new_cases", "deaths", "prevalence", "population",
                "total_cases_per_100k", "new_cases_per_100k", "deaths_per_100k",
                "case_fatality_rate", "new_case_rate"
            ).orderBy(desc("total_cases"))
        
        # Yearly trend aggregations
        self.yearly_trends = self.final_df.groupBy("year") \
            .agg(
                sum("total_cases").alias("total_cases_region"),
                sum("new_cases").alias("new_cases_region"),  
                sum("deaths").alias("deaths_region"),
                sum("population").alias("total_population"),
                avg("total_cases_per_100k").alias("avg_cases_per_100k"),
                avg("case_fatality_rate").alias("avg_case_fatality_rate")
            ).orderBy("year")
        
        # Country trend data (all years)
        self.country_trends = self.final_df.select(
            "country", "iso3", "year", 
            "total_cases", "new_cases", "deaths",
            "total_cases_per_100k", "new_cases_per_100k", "deaths_per_100k"
        ).orderBy("iso3", "year")
        
        logger.info("Aggregated summaries created")
        
    def save_processed_data(self):
        """Save processed data to various formats"""
        logger.info("Saving processed data...")
        
        # Save as parquet (for efficient querying)
        self.final_df.write.mode("overwrite").parquet(
            str(self.processed_data_dir / "tb_complete_data.parquet")
        )
        
        self.country_summary.write.mode("overwrite").parquet(
            str(self.processed_data_dir / "country_summary.parquet") 
        )
        
        self.yearly_trends.write.mode("overwrite").parquet(
            str(self.processed_data_dir / "yearly_trends.parquet")
        )
        
        self.country_trends.write.mode("overwrite").parquet(
            str(self.processed_data_dir / "country_trends.parquet")
        )
        
        # Also save as CSV for easy access
        self.country_summary.toPandas().to_csv(
            self.processed_data_dir / "country_summary.csv", index=False
        )
        
        self.yearly_trends.toPandas().to_csv(
            self.processed_data_dir / "yearly_trends.csv", index=False
        )
        
        self.country_trends.toPandas().to_csv(
            self.processed_data_dir / "country_trends.csv", index=False
        )
        
        logger.info(f"Processed data saved to {self.processed_data_dir}")
        
    def get_data_quality_report(self) -> Dict:
        """Generate data quality report"""
        logger.info("Generating data quality report...")
        
        # Basic statistics
        total_records = self.final_df.count()
        countries_count = self.final_df.select("iso3").distinct().count()
        years_range = self.final_df.select(min("year"), max("year")).collect()[0]
        
        # Missing data check
        missing_pop = self.final_df.filter(col("population") <= 0).count()
        missing_cases = self.final_df.filter(col("total_cases") <= 0).count()
        
        # Data completeness by country
        completeness = self.final_df.groupBy("iso3") \
            .agg(count("*").alias("records_count")) \
            .toPandas()
        
        report = {
            "total_records": total_records,
            "countries_processed": countries_count,
            "year_range": f"{years_range[0]} - {years_range[1]}",
            "missing_population_records": missing_pop,
            "zero_cases_records": missing_cases,
            "completeness_by_country": completeness.to_dict('records')
        }
        
        logger.info("Data quality report generated")
        return report
        
    def process_complete_pipeline(self, tb_data: pd.DataFrame, population_data: pd.DataFrame) -> Dict:
        """Run the complete processing pipeline"""
        logger.info("Starting complete data processing pipeline...")
        
        try:
            # Load data
            self.load_raw_data(tb_data, population_data)
            
            # Clean data
            self.clean_tb_data()
            self.clean_population_data()
            
            # Transform data
            self.create_pivoted_tb_data()
            self.join_with_population()
            self.calculate_rates()
            
            # Create summaries
            self.create_aggregated_summaries()
            
            # Save processed data
            self.save_processed_data()
            
            # Generate quality report
            quality_report = self.get_data_quality_report()
            
            logger.info("Data processing pipeline completed successfully!")
            
            return {
                "status": "success",
                "quality_report": quality_report,
                "processed_data_location": str(self.processed_data_dir)
            }
            
        except Exception as e:
            logger.error(f"Error in processing pipeline: {str(e)}")
            return {
                "status": "error",
                "error_message": str(e)
            }
    
    def get_processed_dataframes(self) -> Dict:
        """Return processed DataFrames as pandas for API use"""
        return {
            "country_summary": self.country_summary.toPandas(),
            "yearly_trends": self.yearly_trends.toPandas(), 
            "country_trends": self.country_trends.toPandas(),
            "complete_data": self.final_df.toPandas()
        }
    
    def cleanup(self):
        """Clean up Spark session"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")

# Utility functions for database operations
class DataWarehouse:
    """
    Data warehouse operations for storing processed data
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        
    def create_tables(self):
        """Create database tables for TB data"""
        from sqlalchemy import create_engine, text
        
        engine = create_engine(self.connection_string)
        
        # SQL for creating tables
        create_tables_sql = """
        -- Country summary table
        CREATE TABLE IF NOT EXISTS tb_country_summary (
            id SERIAL PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            iso3 VARCHAR(3) NOT NULL,
            year INTEGER NOT NULL,
            total_cases INTEGER DEFAULT 0,
            new_cases INTEGER DEFAULT 0,
            deaths INTEGER DEFAULT 0,
            prevalence INTEGER DEFAULT 0,
            population BIGINT DEFAULT 0,
            total_cases_per_100k DECIMAL(10,2) DEFAULT 0,
            new_cases_per_100k DECIMAL(10,2) DEFAULT 0,
            deaths_per_100k DECIMAL(10,2) DEFAULT 0,
            case_fatality_rate DECIMAL(5,2) DEFAULT 0,
            new_case_rate DECIMAL(5,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(iso3, year)
        );
        
        -- Yearly trends table
        CREATE TABLE IF NOT EXISTS tb_yearly_trends (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL UNIQUE,
            total_cases_region INTEGER DEFAULT 0,
            new_cases_region INTEGER DEFAULT 0,
            deaths_region INTEGER DEFAULT 0,
            total_population BIGINT DEFAULT 0,
            avg_cases_per_100k DECIMAL(10,2) DEFAULT 0,
            avg_case_fatality_rate DECIMAL(5,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Country trends table (historical data)
        CREATE TABLE IF NOT EXISTS tb_country_trends (
            id SERIAL PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            iso3 VARCHAR(3) NOT NULL,
            year INTEGER NOT NULL,
            total_cases INTEGER DEFAULT 0,
            new_cases INTEGER DEFAULT 0,
            deaths INTEGER DEFAULT 0,
            total_cases_per_100k DECIMAL(10,2) DEFAULT 0,
            new_cases_per_100k DECIMAL(10,2) DEFAULT 0,
            deaths_per_100k DECIMAL(10,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(iso3, year)
        );
        
        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_country_summary_iso3_year ON tb_country_summary(iso3, year);
        CREATE INDEX IF NOT EXISTS idx_country_trends_iso3 ON tb_country_trends(iso3);
        CREATE INDEX IF NOT EXISTS idx_yearly_trends_year ON tb_yearly_trends(year);
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_tables_sql))
            conn.commit()
        
        logger.info("Database tables created successfully")
        
    def save_to_database(self, dataframes: Dict):
        """Save processed DataFrames to database"""
        from sqlalchemy import create_engine
        
        engine = create_engine(self.connection_string)
        
        try:
            # Save country summary
            dataframes['country_summary'].to_sql(
                'tb_country_summary', 
                engine, 
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Save yearly trends
            dataframes['yearly_trends'].to_sql(
                'tb_yearly_trends',
                engine,
                if_exists='replace', 
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Save country trends
            dataframes['country_trends'].to_sql(
                'tb_country_trends',
                engine,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info("Data saved to database successfully")
            
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")
            raise

if __name__ == "__main__":
    # Example usage
    from tb_data_collection import TBDataCollector
    
    # Initialize components
    collector = TBDataCollector()
    processor = TBDataProcessor()
    
    try:
        # Collect raw data
        logger.info("Collecting raw data...")
        raw_data = collector.collect_all_data(2018, 2023)
        
        # Process data
        logger.info("Processing data with Spark...")
        result = processor.process_complete_pipeline(
            raw_data['tb_data'], 
            raw_data['population_data']
        )
        
        if result['status'] == 'success':
            print("Processing completed successfully!")
            print(f"Quality Report: {result['quality_report']}")
            
            # Get processed data for API use
            processed_dfs = processor.get_processed_dataframes()
            print(f"\nProcessed data shapes:")
            for name, df in processed_dfs.items():
                print(f"{name}: {df.shape}")
                
        else:
            print(f"Processing failed: {result['error_message']}")
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        
    finally:
        # Clean up
        processor.cleanup()