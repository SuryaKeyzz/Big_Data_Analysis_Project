from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional, List
import os
import sys
import subprocess
from hdfs import InsecureClient
import requests


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DockerOnlyHDFSManager:
    """
    HDFS manager that ONLY works with Docker containers - no CLI fallback
    """
    
    def __init__(self, hdfs_url: str = "http://localhost:9870", hdfs_user: str = "root"):
        self.hdfs_url = hdfs_url
        self.hdfs_user = hdfs_user
        self.client = None
        self.hdfs_base_path = "/tb_data"
        self.namenode_rpc = "hdfs://localhost:9000"
        self._initialize_hdfs()
    
    def _initialize_hdfs(self):
        """Initialize HDFS connection ONLY through Docker Python client"""
        try:
            # Check if Docker Hadoop is running first
            if not self._check_docker_hadoop_running():
                logger.warning("Docker Hadoop services not running - HDFS disabled")
                return
            
            # Only use Python HDFS client - NO CLI
            self.client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
            
            # Test connection
            if self._test_hdfs_connection():
                logger.info(f"Docker HDFS Python client connected successfully: {self.hdfs_url}")
            else:
                logger.warning("Docker HDFS Python client failed - HDFS disabled")
                self.client = None
            
        except Exception as e:
            logger.error(f"Docker HDFS initialization failed: {str(e)}")
            self.client = None
    
    def _check_docker_hadoop_running(self) -> bool:
        """Check if Docker Hadoop containers are running"""
        try:
            # Check NameNode web UI
            response = requests.get(f"{self.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", 
                                  timeout=10)
            if response.status_code == 200:
                data = response.json()
                namenode_info = data.get('beans', [{}])[0]
                state = namenode_info.get('State', 'Unknown')
                logger.info(f"Docker NameNode state: {state}")
                
                if state == 'active':
                    return True
            
            logger.warning("Docker Hadoop NameNode not active")
            return False
            
        except Exception as e:
            logger.warning(f"Cannot reach Docker Hadoop services: {e}")
            return False
    
    def _test_hdfs_connection(self) -> bool:
        """Test HDFS connection using Python client only"""
        if not self.client:
            return False
        
        try:
            # Test 1: List root directory
            root_contents = self.client.list('/')
            logger.info(f"Docker HDFS root contents: {root_contents}")
            
            # Test 2: Create base directory if needed
            if not self.client.status(self.hdfs_base_path, strict=False):
                self.client.makedirs(self.hdfs_base_path)
                logger.info(f"Created Docker HDFS directory: {self.hdfs_base_path}")
            
            # Test 3: Write a small test file
            test_content = "Docker HDFS connection test"
            test_path = f"{self.hdfs_base_path}/docker_connection_test.txt"
            
            with self.client.write(test_path, overwrite=True) as writer:
                writer.write(test_content.encode())
            
            # Test 4: Read back the test file
            with self.client.read(test_path) as reader:
                read_content = reader.read().decode()
            
            if read_content == test_content:
                # Clean up test file
                self.client.delete(test_path)
                logger.info("Docker HDFS connection test successful")
                return True
            else:
                logger.error("Docker HDFS read/write test failed - content mismatch")
                return False
                
        except Exception as e:
            logger.error(f"Docker HDFS connection test failed: {str(e)}")
            return False
    
    def save_to_hdfs(self, local_path: str, hdfs_filename: str) -> bool:
        """Save file to Docker HDFS using Python client only"""
        if not os.path.exists(local_path):
            logger.error(f"Local file does not exist: {local_path}")
            return False
        
        if not self.client:
            logger.warning("Docker HDFS client not available")
            return False
        
        hdfs_path = f"{self.hdfs_base_path}/{hdfs_filename}"
        
        try:
            # Check file size
            file_size = os.path.getsize(local_path)
            logger.info(f"Uploading to Docker HDFS: {os.path.basename(local_path)} ({file_size} bytes)")
            
            # Remove existing file if it exists
            if self.client.status(hdfs_path, strict=False):
                self.client.delete(hdfs_path)
            
            # Upload with optimized chunk size for Docker
            self.client.upload(hdfs_path, local_path, overwrite=True, chunk_size=65536)
            logger.info(f"Successfully uploaded to Docker HDFS: {hdfs_path}")
            return True
            
        except Exception as e:
            logger.error(f"Docker HDFS upload failed: {str(e)}")
            return False
    
    def load_from_hdfs(self, hdfs_filename: str, local_path: str) -> bool:
        """Load file from Docker HDFS using Python client only"""
        if not self.client:
            logger.warning("Docker HDFS client not available")
            return False
        
        hdfs_path = f"{self.hdfs_base_path}/{hdfs_filename}"
        
        try:
            if self.client.status(hdfs_path, strict=False):
                self.client.download(hdfs_path, local_path, overwrite=True)
                logger.info(f"Downloaded from Docker HDFS: {hdfs_path}")
                return True
            else:
                logger.warning(f"File not found in Docker HDFS: {hdfs_path}")
                return False
        except Exception as e:
            logger.error(f"Docker HDFS download failed: {str(e)}")
            return False
    
    def list_hdfs_files(self) -> List[str]:
        """List files in Docker HDFS directory"""
        try:
            if self.client:
                return self.client.list(self.hdfs_base_path)
        except Exception as e:
            logger.error(f"Error listing Docker HDFS files: {str(e)}")
        
        return []
    
    def get_hdfs_status(self) -> Dict:
        """Get Docker HDFS status information"""
        status = {
            "hdfs_available": False,
            "docker_python_client_working": False,
            "docker_containers_running": False,
            "namenode_active": False,
            "safe_mode": "Unknown",
            "live_datanodes": 0,
            "files_count": 0
        }
        
        # Check Docker services
        status["docker_containers_running"] = self._check_docker_hadoop_running()
        status["docker_python_client_working"] = self.client is not None and self._test_hdfs_connection()
        status["hdfs_available"] = status["docker_python_client_working"]
        
        # Get detailed status from Docker NameNode
        try:
            # NameNode status
            response = requests.get(f"{self.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", 
                                  timeout=5)
            if response.status_code == 200:
                data = response.json()
                namenode_info = data.get('beans', [{}])[0]
                status["namenode_active"] = namenode_info.get('State', '') == 'active'
            
            # Safe mode status
            response = requests.get(f"{self.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem", 
                                  timeout=5)
            if response.status_code == 200:
                data = response.json()
                fs_info = data.get('beans', [{}])[0]
                status["safe_mode"] = fs_info.get('Safemode', 'Unknown')
            
            # DataNode count
            response = requests.get(f"{self.hdfs_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", 
                                  timeout=5)
            if response.status_code == 200:
                data = response.json()
                fs_state = data.get('beans', [{}])[0]
                status["live_datanodes"] = fs_state.get('NumLiveDataNodes', 0)
            
            # File count
            files = self.list_hdfs_files()
            status["files_count"] = len(files)
            
        except Exception as e:
            logger.warning(f"Could not get detailed Docker HDFS status: {e}")
        
        return status


class FixedTBDataProcessor:
    """
    Fixed Apache Spark-based data processor for TB epidemic data
    """
    
    def __init__(self, app_name: str = "TB_Data_Processor"):
        self.processed_data_dir = Path('data/processed')
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize Docker-only HDFS manager
        self.hdfs_manager = DockerOnlyHDFSManager()
        
        # Get HDFS status for logging
        hdfs_status = self.hdfs_manager.get_hdfs_status()
        logger.info(f"Docker HDFS Status: {hdfs_status}")
        
        # Create required directories first
        self._create_required_directories()
        
        # Configure Python paths properly
        self._configure_python_paths()
        
        # Initialize Spark with Docker Hadoop configuration
        self.spark = self._initialize_spark_with_docker_hadoop(app_name)
        
    def _configure_python_paths(self):
        """Configure Python paths for PySpark on Windows"""
        logger.info("Configuring Python paths for PySpark...")
        
        current_python = sys.executable
        logger.info(f"Current Python executable: {current_python}")
        
        os.environ['PYSPARK_PYTHON'] = current_python
        os.environ['PYSPARK_DRIVER_PYTHON'] = current_python
        
        logger.info(f"PYSPARK_PYTHON set to: {os.environ['PYSPARK_PYTHON']}")
        logger.info(f"PYSPARK_DRIVER_PYTHON set to: {os.environ['PYSPARK_DRIVER_PYTHON']}")
    
    def _initialize_spark_with_docker_hadoop(self, app_name: str) -> SparkSession:
        """Initialize Spark session configured for Docker Hadoop"""
        logger.info("Initializing Spark session for Docker Hadoop...")
        
        # Build Spark configuration
        spark_builder = SparkSession.builder.appName(app_name)
        
        # Basic Spark optimizations
        spark_builder = spark_builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        # Python configuration
        current_python = sys.executable
        spark_builder = spark_builder \
            .config("spark.pyspark.python", current_python) \
            .config("spark.pyspark.driver.python", current_python)
        
        # Memory configuration with better settings for small datasets
        spark_builder = spark_builder \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "4")
        
        # Docker Hadoop configuration
        hdfs_status = self.hdfs_manager.get_hdfs_status()
        if hdfs_status["docker_containers_running"]:
            logger.info("Configuring Spark for Docker Hadoop cluster")
            spark_builder = spark_builder \
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
                .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "false") \
                .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "false") \
                .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        else:
            logger.info("Docker Hadoop not running, using local Spark mode")
        
        # Local temporary directories
        temp_dir = os.path.abspath("tmp")
        warehouse_dir = os.path.abspath("spark-warehouse")
        
        spark_builder = spark_builder \
            .config("spark.local.dir", temp_dir) \
            .config("spark.sql.warehouse.dir", warehouse_dir)
        
        # Create the session
        spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # Test Python worker creation
        try:
            test_rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
            test_result = test_rdd.map(lambda x: x * 2).collect()
            logger.info(f"Python worker test successful: {test_result}")
        except Exception as e:
            logger.error(f"Python worker test failed: {e}")
            raise
        
        logger.info(f"Spark session initialized successfully: {spark.version}")
        return spark
        
    def _create_required_directories(self):
        """Create required directories for Spark operations"""
        directories = [
            'tmp',
            'spark-warehouse',
            str(self.processed_data_dir)
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        logger.info("Required directories created")
    
    def load_raw_data(self, tb_data: pd.DataFrame, population_data: pd.DataFrame):
        """Load raw data into Spark DataFrames with proper data type handling"""
        logger.info("Loading raw data into Spark DataFrames...")
        
        # Deep copy to avoid modifying original data
        tb_data = tb_data.copy()
        population_data = population_data.copy()
        
        # CRITICAL FIX: Convert population to integer before creating Spark DataFrame
        population_data['population'] = population_data['population'].astype(float).round().astype('int64')
        logger.info(f"Population data type after conversion: {population_data['population'].dtype}")
        
        # Rest of data type handling
        population_data['year'] = population_data['year'].astype(str)
        population_data['country'] = population_data['country'].astype(str)
        population_data['iso3'] = population_data['iso3'].astype(str)
        
        # Handle TB data as before
        if tb_data['year'].dtype == object and tb_data['indicator'].dtype == object:
            sample_year = tb_data['indicator'].iloc[0]
            if str(sample_year).isdigit() and int(sample_year) >= 2000:
                logger.warning("Detected swapped year and indicator columns - fixing...")
                tb_data = tb_data.rename(columns={'year': 'temp_col'})
                tb_data = tb_data.rename(columns={'indicator': 'year'})
                tb_data = tb_data.rename(columns={'temp_col': 'indicator'})
        
        # Clean and convert data types
        tb_data['value'] = pd.to_numeric(tb_data['value'], errors='coerce').fillna(0)
        tb_data['year'] = pd.to_numeric(tb_data['year'], errors='coerce')
        
        # Validate and clean data
        tb_data = tb_data[
            tb_data['year'].notna() & 
            (tb_data['year'] >= 2000) & 
            (tb_data['year'] <= 2030)
        ]
        
        # Convert year to string after validation
        tb_data['year'] = tb_data['year'].astype('int64').astype(str)
        
        # Basic string cleaning
        tb_data['country'] = tb_data['country'].astype(str)
        tb_data['iso3'] = tb_data['iso3'].astype(str)
        tb_data['g_whoregion'] = tb_data['g_whoregion'].astype(str).fillna('SEA')
        tb_data['indicator'] = tb_data['indicator'].astype(str)
        
        logger.info(f"Data types after cleaning: {tb_data.dtypes}")
        logger.info(f"Unique indicators: {tb_data['indicator'].unique()}")
        logger.info(f"Year range: {tb_data['year'].min()} - {tb_data['year'].max()}")
        
        # Create Spark schemas with population as LongType
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
        
        # Create Spark DataFrames with optimization
        self.tb_df = self.spark.createDataFrame(tb_data, schema=tb_schema).cache()
        self.pop_df = self.spark.createDataFrame(population_data, schema=population_schema).cache()
        
        logger.info(f"Loaded TB data: {self.tb_df.count()} records")
        logger.info(f"Loaded population data: {self.pop_df.count()} records")
            
    def clean_tb_data(self):
        """Clean and validate TB data with corrected column handling"""
        logger.info("Cleaning TB data...")
        
        # Basic validation without type casting
        self.tb_df_clean = self.tb_df.filter(
            (col("value").isNotNull()) & 
            (col("value") >= 0) &
            (col("year").isNotNull()) &
            (col("iso3").isNotNull()) &
            (col("indicator").isNotNull())
        )
        
        # Log intermediate count
        intermediate_count = self.tb_df_clean.count()
        logger.info(f"TB data after basic filtering: {intermediate_count} records")
        
        # Check if year and indicator are swapped
        sample_rows = self.tb_df_clean.select("year", "indicator").limit(5).collect()
        logger.info("Sample rows before correction:")
        for row in sample_rows:
            logger.info(f"  Year: {row.year}, Indicator: {row.indicator}")
        
        # If year contains indicator names, swap the columns
        if any('e_' in str(row.year) for row in sample_rows):
            logger.info("Detected swapped year/indicator columns - fixing...")
            self.tb_df_clean = self.tb_df_clean.select(
                "country", "iso3", "g_whoregion",
                col("indicator").alias("temp_year"),
                col("year").alias("temp_indicator"),
                "value"
            ).withColumnRenamed("temp_year", "year") \
             .withColumnRenamed("temp_indicator", "indicator")
        
        # Now safely convert year to integer using try_cast
        self.tb_df_clean = self.tb_df_clean.withColumn(
            "year",
            expr("CAST(year AS int)")
        )
        
        # Filter valid years
        self.tb_df_clean = self.tb_df_clean.filter(
            col("year").isNotNull() &
            (col("year") >= 2000) & 
            (col("year") <= 2030)
        )
        
        # Identify actual indicators in the data
        indicators = self.tb_df_clean.select("indicator").distinct()
        indicator_list = [row.indicator for row in indicators.collect()]
        logger.info(f"Found indicators: {indicator_list}")
        
        # Only keep valid indicators
        valid_indicators = [
            'e_inc_num', 'c_newinc', 'e_mort_num', 'e_prev_num',
            'e_inc_100k', 'e_mort_100k', 'e_prev_100k', 'c_newinc_100k'
        ]
        
        self.tb_df_clean = self.tb_df_clean.filter(
            col("indicator").isin(valid_indicators)
        )
        
        logger.info(f"Clean data count: {self.tb_df_clean.count()}")
        
        # Debug output for verification
        sample = self.tb_df_clean.select(
            "country", "iso3", "year", "indicator", "value"
        ).limit(5).collect()
        
        logger.info("Sample of cleaned data:")
        for row in sample:
            logger.info(f"  {row}")

    def clean_population_data(self):
        """Clean and validate population data"""
        logger.info("Cleaning population data...")
        
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
        
        self.combined_df = self.tb_pivoted.join(
            self.pop_df_clean.select("iso3", "year", "population"),
            on=["iso3", "year"],
            how="left"
        )
        
        # Fill missing population with 0
        self.combined_df = self.combined_df.na.fill({"population": 0})
        
        logger.info(f"Combined data: {self.combined_df.count()} records")
        
    def calculate_rates(self):
        """Calculate rates per 100,000 population"""
        logger.info("Calculating rates per 100,000 population...")
        
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
        ).withColumn(
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
        """Save processed data as CSV files for API compatibility"""
        logger.info("Saving processed data...")
        
        try:
            logger.info("Saving as CSV files for API compatibility...")
            
            # Convert to Pandas DataFrames and save as CSV
            country_summary_pd = self.country_summary.toPandas()
            yearly_trends_pd = self.yearly_trends.toPandas()
            country_trends_pd = self.country_trends.toPandas()
            
            # Clean DataFrames
            country_summary_pd = self._clean_dataframe_for_api(country_summary_pd, 'country_summary')
            yearly_trends_pd = self._clean_dataframe_for_api(yearly_trends_pd, 'yearly_trends')
            country_trends_pd = self._clean_dataframe_for_api(country_trends_pd, 'country_trends')
            
            # Save CSV files
            country_summary_pd.to_csv(self.processed_data_dir / "country_summary.csv", index=False)
            yearly_trends_pd.to_csv(self.processed_data_dir / "yearly_trends.csv", index=False)
            country_trends_pd.to_csv(self.processed_data_dir / "country_trends.csv", index=False)
            
            logger.info("CSV files saved successfully for API use")
            logger.info(f"Country summary: {len(country_summary_pd)} records")
            logger.info(f"Yearly trends: {len(yearly_trends_pd)} records")  
            logger.info(f"Country trends: {len(country_trends_pd)} records")
            
        except Exception as e:
            logger.error(f"Error saving CSV files: {str(e)}")
            raise
        
        logger.info(f"All processed data saved to {self.processed_data_dir}")
    
    def save_with_docker_hdfs_backup(self):
        """Save processed data with Docker HDFS backup"""
        logger.info("Saving processed data with Docker HDFS backup...")
        
        # First save locally
        self.save_processed_data()
        
        # Check Docker HDFS availability
        hdfs_status = self.hdfs_manager.get_hdfs_status()
        
        if not hdfs_status["hdfs_available"]:
            logger.warning("Docker HDFS not available - skipping backup")
            logger.info("To enable Docker HDFS backup:")
            logger.info("1. Ensure Docker containers are running: docker-compose up -d")
            logger.info("2. Wait for services to start (may take 2-3 minutes)")
            logger.info("3. Check NameNode UI at http://localhost:9870")
            return
        
        # Backup to Docker HDFS
        csv_files = ['country_summary.csv', 'yearly_trends.csv', 'country_trends.csv']
        successful_backups = 0
        
        for csv_file in csv_files:
            local_path = str(self.processed_data_dir / csv_file)
            if Path(local_path).exists():
                success = self.hdfs_manager.save_to_hdfs(local_path, csv_file)
                if success:
                    successful_backups += 1
                    logger.info(f"✓ Backed up {csv_file} to Docker HDFS")
                else:
                    logger.warning(f"✗ Failed to backup {csv_file} to Docker HDFS")
        
        if successful_backups > 0:
            logger.info(f"Docker HDFS backup completed: {successful_backups}/{len(csv_files)} files")
        else:
            logger.warning("Docker HDFS backup failed for all files")
            
        # List current Docker HDFS contents
        hdfs_files = self.hdfs_manager.list_hdfs_files()
        if hdfs_files:
            logger.info(f"Current Docker HDFS files: {hdfs_files}")
    
    def _clean_dataframe_for_api(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Clean DataFrame for API consumption"""
        df = df.copy()

        # Fill numeric NaNs
        numeric_columns = df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            df[col] = df[col].fillna(0)

        # Fill string NaNs
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].fillna('')

        # Special handling for decimal columns
        if 'avg_cases_per_100k' in df.columns:
            df['avg_cases_per_100k'] = pd.to_numeric(df['avg_cases_per_100k'], errors='coerce').fillna(0.0)

        return df
    
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
    # Test the Docker-only HDFS manager
    hdfs_manager = DockerOnlyHDFSManager()
    
    # Get comprehensive status
    status = hdfs_manager.get_hdfs_status()
    print("\n=== Docker HDFS Status Report ===")
    for key, value in status.items():
        print(f"{key}: {value}")
    
    # Test file operations if Docker HDFS is available
    if status["hdfs_available"]:
        # Create a test file
        test_file = "test_docker_hdfs.txt"
        with open(test_file, 'w') as f:
            f.write("Docker HDFS connection test from fixed manager\n")
        
        # Try to save it
        success = hdfs_manager.save_to_hdfs(test_file, "test_docker_connection.txt")
        print(f"\nTest file upload: {'SUCCESS' if success else 'FAILED'}")
        
        # List files
        files = hdfs_manager.list_hdfs_files()
        print(f"Docker HDFS files: {files}")
        
        # Clean up
        os.remove(test_file)
    else:
        print("\nDocker HDFS not available for testing")
        print("\nTo start Docker Hadoop:")
        print("1. docker-compose up -d")
        print("2. Wait 2-3 minutes for services to initialize")
        print("3. Check http://localhost:9870")