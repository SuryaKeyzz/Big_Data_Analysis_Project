#Flask_api_server.py

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import json
import os
import psycopg2
from sqlalchemy.engine import URL
from sqlalchemy import create_engine, text
import urllib.parse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration
CORS(app, origins=[
    "http://localhost:3000",  # React dev server
    "http://127.0.0.1:3000",
    "https://your-production-domain.com"  # Add your production domain
], methods=["GET", "POST", "OPTIONS"], allow_headers=["Content-Type"])


# Configure caching
cache = Cache(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600  # 1 hour cache
})

class PostgreSQLDataManager:
    """
    PostgreSQL database manager for distributed data storage
    """
    
    def __init__(self, connection_params: Dict = None):
        if connection_params is None:
            connection_params = {
                'host': 'localhost',
                'port': 5433,
                'database': 'tb_data_warehouse',
                'user': 'postgres',
                'password': 'gilbert123'
            }
        self.connection_params = connection_params
        self.connection_string = self._build_connection_string()
        self.engine = None
        self._initialize_connection()
    
    def _build_connection_string(self):
        # Force psycopg2 driver explicitly
        password = urllib.parse.quote_plus(self.connection_params['password'])
        return (
            f"postgresql+psycopg2://{self.connection_params['user']}:{password}"
            f"@{self.connection_params['host']}:{self.connection_params['port']}"
            f"/{self.connection_params['database']}"
        )
        
        
    def _initialize_connection(self):
        try:
            import platform
            system_platform = platform.system().lower()

            # Force UTF-8 encoding globally
            os.environ['PYTHONIOENCODING'] = 'utf-8'
            os.environ['PGCLIENTENCODING'] = 'UTF8'

            # Clean connection parameters for encoding issues
            params = {}
            for key, value in self.connection_params.items():
                if isinstance(value, str):
                    # Clean non-ASCII characters from the parameter value
                    cleaned_value = ''.join(char for char in value if ord(char) < 128)
                    params[key] = cleaned_value
                    logger.debug(f"Connection param {key}: {params[key]}")  # Log the cleaned parameters
                else:
                    params[key] = value

            # Special cleanup for database name, if necessary
            params['database'] = ''.join(char for char in params['database'] if ord(char) < 128)
            logger.debug(f"Cleaned database name: {params['database']}")  # Log the cleaned database name

            def pg_creator():
                # Ubuntu/Linux: Force TCP/IP instead of Unix socket
                if system_platform in ['linux', 'ubuntu']:
                    host = '127.0.0.1' if params['host'] == 'localhost' else params['host']
                else:
                    host = params['host']

                return psycopg2.connect(
                    host=host,
                    port=int(params['port']),
                    user=params['user'],
                    password=params['password'],
                    dbname=params['database'],
                    client_encoding='UTF8',
                    options="-c client_encoding=UTF8"
                )

            # Empty URL + creator -> SQLAlchemy skips DSN parsing entirely
            self.engine = create_engine(
                "postgresql+psycopg2://", 
                creator=pg_creator, 
                future=True,
                connect_args={"client_encoding": "utf8"}
            )

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("PostgreSQL connection established successfully")
            self._create_tables()

        except Exception as e:
            logger.warning(f"PostgreSQL connection failed: {e}")
            self.engine = None    
            
    def _create_tables(self):
        """Create PostgreSQL tables for TB data"""
        if not self.engine:
            return
        
        create_tables_sql = """
        -- Create schema if not exists
        CREATE SCHEMA IF NOT EXISTS tb_analytics;
        
        -- Country summary table with partitioning
        CREATE TABLE IF NOT EXISTS tb_analytics.country_summary (
            id SERIAL PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            iso3 VARCHAR(3) NOT NULL,
            year INTEGER NOT NULL,
            total_cases BIGINT DEFAULT 0,
            new_cases BIGINT DEFAULT 0,
            deaths BIGINT DEFAULT 0,
            prevalence BIGINT DEFAULT 0,
            population BIGINT DEFAULT 0,
            total_cases_per_100k NUMERIC(10,2) DEFAULT 0,
            new_cases_per_100k NUMERIC(10,2) DEFAULT 0,
            deaths_per_100k NUMERIC(10,2) DEFAULT 0,
            case_fatality_rate NUMERIC(5,2) DEFAULT 0,
            new_case_rate NUMERIC(5,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(iso3, year)
        );
        
        -- Yearly trends table
        CREATE TABLE IF NOT EXISTS tb_analytics.yearly_trends (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL UNIQUE,
            total_cases_region BIGINT DEFAULT 0,
            new_cases_region BIGINT DEFAULT 0,
            deaths_region BIGINT DEFAULT 0,
            total_population BIGINT DEFAULT 0,
            avg_cases_per_100k NUMERIC(10,2) DEFAULT 0,
            avg_case_fatality_rate NUMERIC(5,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Country trends table (time-series data)
        CREATE TABLE IF NOT EXISTS tb_analytics.country_trends (
            id SERIAL PRIMARY KEY,
            country VARCHAR(100) NOT NULL,
            iso3 VARCHAR(3) NOT NULL,
            year INTEGER NOT NULL,
            total_cases BIGINT DEFAULT 0,
            new_cases BIGINT DEFAULT 0,
            deaths BIGINT DEFAULT 0,
            total_cases_per_100k NUMERIC(10,2) DEFAULT 0,
            new_cases_per_100k NUMERIC(10,2) DEFAULT 0,
            deaths_per_100k NUMERIC(10,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(iso3, year)
        );
        
        -- Create indexes for performance
        CREATE INDEX IF NOT EXISTS idx_country_summary_iso3_year ON tb_analytics.country_summary(iso3, year);
        CREATE INDEX IF NOT EXISTS idx_country_summary_year ON tb_analytics.country_summary(year);
        CREATE INDEX IF NOT EXISTS idx_country_trends_iso3 ON tb_analytics.country_trends(iso3);
        CREATE INDEX IF NOT EXISTS idx_country_trends_year ON tb_analytics.country_trends(year);
        CREATE INDEX IF NOT EXISTS idx_yearly_trends_year ON tb_analytics.yearly_trends(year);
        
        -- Create triggers for updated_at
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_country_summary_updated_at 
            BEFORE UPDATE ON tb_analytics.country_summary 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            
        CREATE TRIGGER update_yearly_trends_updated_at 
            BEFORE UPDATE ON tb_analytics.yearly_trends 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            
        CREATE TRIGGER update_country_trends_updated_at 
            BEFORE UPDATE ON tb_analytics.country_trends 
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_tables_sql))
                conn.commit()
            logger.info("PostgreSQL tables created successfully")
        except Exception as e:
            logger.error(f"Error creating PostgreSQL tables: {str(e)}")
    
    def save_data(self, dataframes: Dict):
        """Save DataFrames to PostgreSQL"""
        if not self.engine:
            return False
        
        try:
            table_mappings = {
                'country_summary': 'tb_analytics.country_summary',
                'yearly_trends': 'tb_analytics.yearly_trends',
                'country_trends': 'tb_analytics.country_trends'
            }
            
            for df_name, df in dataframes.items():
                if df_name in table_mappings:
                    table_name = table_mappings[df_name]
                    
                    # Use upsert for better data management
                    df.to_sql(
                        table_name.split('.')[1], 
                        self.engine,
                        schema='tb_analytics',
                        if_exists='replace',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                    logger.info(f"Saved {len(df)} records to {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {str(e)}")
            return False
    
    def load_data(self, table_name: str, filters: Dict = None):
        """Load data from PostgreSQL with optional filters"""
        if not self.engine:
            return None
        
        try:
            query = f"SELECT * FROM tb_analytics.{table_name}"
            params = {}
            
            if filters:
                conditions = []
                for key, value in filters.items():
                    conditions.append(f"{key} = %({key})s")
                    params[key] = value
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            
            df = pd.read_sql_query(query, self.engine, params=params)
            logger.info(f"Loaded {len(df)} records from tb_analytics.{table_name}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading from PostgreSQL: {str(e)}")
            return None







class TBAPIService:
    """
    Service class for TB data API operations
    """
    
    def __init__(self, data_dir: str = 'data/processed'):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / 'tb_data.sqlite'
        self.csv_backup_dir = self.data_dir
        
        self.postgres_manager = PostgreSQLDataManager()
        
        # Southeast Asian countries mapping
        self.country_mapping = {
            'KHM': {'name': 'Cambodia', 'coords': [12.5657, 104.9910]},
            'IDN': {'name': 'Indonesia', 'coords': [-0.7893, 113.9213]}, 
            'LAO': {'name': 'Laos', 'coords': [19.8563, 102.4955]},
            'MYS': {'name': 'Malaysia', 'coords': [4.2105, 101.9758]},
            'MMR': {'name': 'Myanmar', 'coords': [21.9162, 95.9560]},
            'PHL': {'name': 'Philippines', 'coords': [12.8797, 121.7740]},
            'SGP': {'name': 'Singapore', 'coords': [1.3521, 103.8198]},
            'THA': {'name': 'Thailand', 'coords': [15.8700, 100.9925]},
            'VNM': {'name': 'Vietnam', 'coords': [14.0583, 108.2772]},
            'TLS': {'name': 'Timor-Leste', 'coords': [-8.8742, 125.7275]}
        }
        
        self._initialize_database()
        
    def _initialize_database(self):
        """Initialize SQLite database if needed"""
        try:
            # Create database if it doesn't exist
            if not self.db_path.exists():
                self._create_sqlite_tables()
                self._load_csv_to_sqlite()
            
            logger.info("Database initialized successfully")
            
        except Exception as e:
            logger.error(f"Database initialization error: {str(e)}")
            # Fallback to CSV files
            logger.info("Falling back to CSV file operations")
            
    def _create_sqlite_tables(self):
        """Create SQLite tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Country summary table
        cursor.execute('''
            CREATE TABLE tb_country_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL,
                iso3 TEXT NOT NULL,
                year INTEGER NOT NULL,
                total_cases INTEGER DEFAULT 0,
                new_cases INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0,
                prevalence INTEGER DEFAULT 0,
                population INTEGER DEFAULT 0,
                total_cases_per_100k REAL DEFAULT 0,
                new_cases_per_100k REAL DEFAULT 0,
                deaths_per_100k REAL DEFAULT 0,
                case_fatality_rate REAL DEFAULT 0,
                new_case_rate REAL DEFAULT 0,
                UNIQUE(iso3, year)
            )
        ''')
        
        # Yearly trends table
        cursor.execute('''
            CREATE TABLE tb_yearly_trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER NOT NULL UNIQUE,
                total_cases_region INTEGER DEFAULT 0,
                new_cases_region INTEGER DEFAULT 0,
                deaths_region INTEGER DEFAULT 0,
                total_population INTEGER DEFAULT 0,
                avg_cases_per_100k REAL DEFAULT 0,
                avg_case_fatality_rate REAL DEFAULT 0
            )
        ''')
        
        # Country trends table
        cursor.execute('''
            CREATE TABLE tb_country_trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country TEXT NOT NULL,
                iso3 TEXT NOT NULL,
                year INTEGER NOT NULL,
                total_cases INTEGER DEFAULT 0,
                new_cases INTEGER DEFAULT 0,
                deaths INTEGER DEFAULT 0,
                total_cases_per_100k REAL DEFAULT 0,
                new_cases_per_100k REAL DEFAULT 0,
                deaths_per_100k REAL DEFAULT 0,
                UNIQUE(iso3, year)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    
    def get_map_data_from_postgres(self, year: Optional[int] = None) -> Dict:
        """Get map data from PostgreSQL if available"""
        if not self.postgres_manager.engine:
            return self.get_map_data(year)  # Fallback to SQLite
        
        try:
            # Use latest year if not specified
            if year is None:
                year_df = self.postgres_manager.load_data('country_summary')
                if year_df is not None and not year_df.empty:
                    year = year_df['year'].max()
                else:
                    year = 2023
            
            # Get data with filters
            filters = {'year': year}
            df = self.postgres_manager.load_data('country_summary', filters)
            
            if df is None or df.empty:
                return self.get_map_data(year)  # Fallback to SQLite
            
            # Format data for map (same logic as existing method)
            map_features = []
            
            for _, row in df.iterrows():
                iso3 = row['iso3']
                if iso3 in self.country_mapping:
                    country_info = self.country_mapping[iso3]
                    
                    feature = {
                        'iso3': iso3,
                        'country': row['country'],
                        'coordinates': country_info['coords'],
                        'data': {
                            'year': int(row['year']),
                            'total_cases': int(row['total_cases']) if pd.notna(row['total_cases']) else 0,
                            'new_cases': int(row['new_cases']) if pd.notna(row['new_cases']) else 0,
                            'deaths': int(row['deaths']) if pd.notna(row['deaths']) else 0,
                            'population': int(row['population']) if pd.notna(row['population']) else 0,
                            'total_cases_per_100k': float(row['total_cases_per_100k']) if pd.notna(row['total_cases_per_100k']) else 0,
                            'new_cases_per_100k': float(row['new_cases_per_100k']) if pd.notna(row['new_cases_per_100k']) else 0,
                            'deaths_per_100k': float(row['deaths_per_100k']) if pd.notna(row['deaths_per_100k']) else 0,
                            'case_fatality_rate': float(row['case_fatality_rate']) if pd.notna(row['case_fatality_rate']) else 0
                        }
                    }
                    map_features.append(feature)
            
            # Calculate regional statistics
            total_cases = df['total_cases'].sum()
            total_deaths = df['deaths'].sum()
            avg_cases_per_100k = df['total_cases_per_100k'].mean()
            
            return {
                'year': year,
                'features': map_features,
                'regional_stats': {
                    'total_cases': int(total_cases),
                    'total_deaths': int(total_deaths),
                    'avg_cases_per_100k': round(float(avg_cases_per_100k), 2),
                    'countries_count': len(map_features)
                },
                'data_source': 'postgresql'
            }
            
        except Exception as e:
            logger.error(f"Error getting map data from PostgreSQL: {str(e)}")
            return self.get_map_data(year)  # Fallback to SQLite
        
    
    
    def _load_csv_to_sqlite(self):
        """Force-load processed CSVs into the API's SQLite DB, replacing rows."""
        try:
            conn = sqlite3.connect(self.db_path)
            csv_files = {
                'tb_country_summary': self.csv_backup_dir / 'country_summary.csv',
                'tb_yearly_trends':   self.csv_backup_dir / 'yearly_trends.csv',
                'tb_country_trends':  self.csv_backup_dir / 'country_trends.csv',
            }

            for table, path in csv_files.items():
                if not path.exists():
                    logger.error(f"[CSV MISSING] {path} (expected for {table})")
                    continue

                df = pd.read_csv(path)

                # Replace NaNs in numerics with 0 / 0.0
                for col in df.select_dtypes(include=['number']).columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

                if 'avg_cases_per_100k' in df.columns:
                    df['avg_cases_per_100k'] = pd.to_numeric(df['avg_cases_per_100k'], errors='coerce').fillna(0.0)

                # Replace table contents
                df.to_sql(table, conn, if_exists='replace', index=False)
                logger.info(f"[CSVÃ¢â€ â€™SQLite] Imported {len(df)} rows into {table}")

            conn.close()
            logger.info("CSV reload into SQLite completed.")
        except Exception as e:
            logger.error(f"CSV reload into SQLite failed: {e}")
            raise
     
    
    def _create_sample_data(self):
        """Create sample data in SQLite if CSV loading fails"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Sample data for testing
            sample_countries = [
                ('Indonesia', 'IDN', 2023, 969000, 800000, 93000, 1000000, 275000000),
                ('Philippines', 'PHL', 2023, 650000, 550000, 67000, 700000, 115000000),
                ('Vietnam', 'VNM', 2023, 174000, 150000, 14000, 180000, 98000000),
                ('Thailand', 'THA', 2023, 92000, 80000, 9100, 95000, 72000000),
                ('Malaysia', 'MYS', 2023, 25000, 22000, 1600, 27000, 34000000),
            ]
            
            for country, iso3, year, total_cases, new_cases, deaths, prevalence, population in sample_countries:
                total_cases_per_100k = (total_cases * 100000 / population) if population > 0 else 0
                new_cases_per_100k = (new_cases * 100000 / population) if population > 0 else 0
                deaths_per_100k = (deaths * 100000 / population) if population > 0 else 0
                case_fatality_rate = (deaths * 100 / total_cases) if total_cases > 0 else 0
                new_case_rate = (new_cases * 100 / total_cases) if total_cases > 0 else 0
                
                cursor.execute('''
                    INSERT OR REPLACE INTO tb_country_summary 
                    (country, iso3, year, total_cases, new_cases, deaths, prevalence, population,
                    total_cases_per_100k, new_cases_per_100k, deaths_per_100k, case_fatality_rate, new_case_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (country, iso3, year, total_cases, new_cases, deaths, prevalence, population,
                    total_cases_per_100k, new_cases_per_100k, deaths_per_100k, case_fatality_rate, new_case_rate))
            
            conn.commit()
            conn.close()
            logger.info("Sample data created in SQLite database")
            
        except Exception as e:
            logger.error(f"Error creating sample data: {str(e)}")
            
    def get_map_data(self, year: Optional[int] = None) -> Dict:
        """Serve map data strictly from PostgreSQL (no SQLite fallback)."""
        if not self.postgres_manager.engine:
            logger.error("PostgreSQL engine unavailable. Cannot serve map-data.")
            return {'error': 'PostgreSQL unavailable'}, 500

        try:
            engine = self.postgres_manager.engine
            # Determine target year (latest if not provided)
            if year is None:
                with engine.connect() as cx:
                    res = cx.execute(text("SELECT MAX(year) FROM tb_analytics.country_summary"))
                    year = int(res.scalar() or 2023)

            df = self.postgres_manager.load_data('country_summary', {'year': year})
            if df is None or df.empty:
                logger.error(f"No PostgreSQL data for year={year}")
                return {'error': f'No data in PostgreSQL for year={year}'}, 404

            logger.info(f"Serving map-data from PostgreSQL for year={year} (rows={len(df)})")

            features = []
            for _, row in df.iterrows():
                iso3 = row.get('iso3')
                if iso3 in self.country_mapping:
                    info = self.country_mapping[iso3]
                    features.append({
                        'iso3': iso3,
                        'country': row.get('country', ''),
                        'coordinates': info['coords'],
                        'data': {
                            'year': int(row.get('year', year)),
                            'total_cases': int(row.get('total_cases', 0) or 0),
                            'new_cases': int(row.get('new_cases', 0) or 0),
                            'deaths': int(row.get('deaths', 0) or 0),
                            'population': int(row.get('population', 0) or 0),
                            'total_cases_per_100k': float(row.get('total_cases_per_100k', 0.0) or 0.0),
                            'new_cases_per_100k': float(row.get('new_cases_per_100k', 0.0) or 0.0),
                            'deaths_per_100k': float(row.get('deaths_per_100k', 0.0) or 0.0),
                            'case_fatality_rate': float(row.get('case_fatality_rate', 0.0) or 0.0)
                        }
                    })

            avg_cases = float(pd.to_numeric(df['total_cases_per_100k'], errors='coerce').fillna(0).mean() or 0.0)

            return {
                'year': year,
                'features': features,
                'regional_stats': {
                    'total_cases': int(pd.to_numeric(df['total_cases'], errors='coerce').fillna(0).sum()),
                    'total_deaths': int(pd.to_numeric(df['deaths'], errors='coerce').fillna(0).sum()),
                    'avg_cases_per_100k': round(avg_cases, 2),
                    'countries_count': len(features)
                },
                'data_source': 'postgresql'
            }
        except Exception as e:
            logger.error(f"PostgreSQL map-data error: {e}")
            return {'error': 'Failed to load map-data from PostgreSQL'}, 500
      
    def get_country_trends(self, iso3: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> Dict:
        if not self.postgres_manager.engine:
            return {'error': 'PostgreSQL unavailable'}, 500
        filters = {'iso3': iso3}
        df = self.postgres_manager.load_data('country_trends', filters)
        if df is None or df.empty:
            return {'error': f'No data for {iso3} in PostgreSQL'}, 404
        if start_year is not None:
            df = df[df['year'] >= start_year]
        if end_year is not None:
            df = df[df['year'] <= end_year]
        df = df.sort_values('year')
        logger.info(f"Serving country-trends from PostgreSQL for {iso3} (rows={len(df)})")
        out = []
        for _, r in df.iterrows():
            out.append({
                'year': int(r['year']),
                'total_cases': int(r.get('total_cases', 0) or 0),
                'new_cases': int(r.get('new_cases', 0) or 0),
                'deaths': int(r.get('deaths', 0) or 0),
                'total_cases_per_100k': float(r.get('total_cases_per_100k', 0.0) or 0.0),
                'new_cases_per_100k': float(r.get('new_cases_per_100k', 0.0) or 0.0),
                'deaths_per_100k': float(r.get('deaths_per_100k', 0.0) or 0.0),
                'case_fatality_rate': float(r.get('case_fatality_rate', 0.0) or 0.0)
            })
        return {'iso3': iso3, 'trends': out, 'data_source': 'postgresql'}     
     
    def get_regional_comparison(self, year: Optional[int] = None) -> Dict:
        if not self.postgres_manager.engine:
            return {'error': 'PostgreSQL unavailable'}, 500
        # Pick latest year if none given
        if year is None:
            with self.postgres_manager.engine.connect() as cx:
                res = cx.execute(text("SELECT MAX(year) FROM tb_analytics.country_summary"))
                year = int(res.scalar() or 2023)
        df = self.postgres_manager.load_data('country_summary', {'year': year})
        if df is None or df.empty:
            return {'error': f'No data for year={year} in PostgreSQL'}, 404
        logger.info(f"Serving regional comparison from PostgreSQL for year={year} (rows={len(df)})")
        payload = df[['iso3','country','total_cases','new_cases','deaths','population',
                    'total_cases_per_100k','new_cases_per_100k','deaths_per_100k','case_fatality_rate']].fillna(0)
        return {'year': year, 'countries': payload.to_dict(orient='records'), 'data_source': 'postgresql'}  
    
        
    def get_yearly_trends(self) -> Dict:
        if not self.postgres_manager.engine:
            return {'error': 'PostgreSQL unavailable'}, 500
        df = self.postgres_manager.load_data('yearly_trends')
        if df is None or df.empty:
            return {'error': 'No yearly trends in PostgreSQL'}, 404
        df = df.sort_values('year')
        logger.info(f"Serving yearly-trends from PostgreSQL (rows={len(df)})")
        out = []
        for _, r in df.iterrows():
            out.append({
                'year': int(r['year']),
                'total_cases': int(r.get('total_cases_region', 0) or 0),
                'new_cases': int(r.get('new_cases_region', 0) or 0),
                'deaths': int(r.get('deaths_region', 0) or 0),
                'population': int(r.get('total_population', 0) or 0),
                'avg_cases_per_100k': float(r.get('avg_cases_per_100k', 0.0) or 0.0),
                'avg_case_fatality_rate': float(r.get('avg_case_fatality_rate', 0.0) or 0.0)
            })
        return {'trends': out, 'years_available': len(out), 'latest_year': out[-1]['year'] if out else None, 'data_source': 'postgresql'}
    
    
    def _get_sample_map_data(self, year: int) -> Dict:
        """Fallback sample data for map"""
        sample_data = [
            {'iso3': 'IDN', 'cases': 969000, 'deaths': 93000, 'rate': 354.5},
            {'iso3': 'PHL', 'cases': 650000, 'deaths': 67000, 'rate': 562.8},
            {'iso3': 'VNM', 'cases': 174000, 'deaths': 14000, 'rate': 178.2},
            {'iso3': 'MMR', 'cases': 176000, 'deaths': 25000, 'rate': 324.7},
            {'iso3': 'THA', 'cases': 92000, 'deaths': 9100, 'rate': 128.4},
            {'iso3': 'MYS', 'cases': 25000, 'deaths': 1600, 'rate': 73.6},
            {'iso3': 'KHM', 'cases': 56000, 'deaths': 7800, 'rate': 334.0},
            {'iso3': 'LAO', 'cases': 4300, 'deaths': 230, 'rate': 57.1},
            {'iso3': 'SGP', 'cases': 1800, 'deaths': 12, 'rate': 30.4},
            {'iso3': 'TLS', 'cases': 4900, 'deaths': 350, 'rate': 365.4}
        ]
        
        features = []
        for data in sample_data:
            iso3 = data['iso3']
            if iso3 in self.country_mapping:
                country_info = self.country_mapping[iso3]
                features.append({
                    'iso3': iso3,
                    'country': country_info['name'],
                    'coordinates': country_info['coords'],
                    'data': {
                        'year': year,
                        'total_cases': data['cases'],
                        'deaths': data['deaths'],
                        'total_cases_per_100k': data['rate']
                    }
                })
        
        return {
            'year': year,
            'features': features,
            'regional_stats': {
                'total_cases': sum(d['cases'] for d in sample_data),
                'total_deaths': sum(d['deaths'] for d in sample_data)
            }
        }

# Initialize service
tb_service = TBAPIService()

# API Endpoints
@app.route('/api/map-data', methods=['GET'])
@cache.cached(timeout=3600)
def get_map_data():
    year = request.args.get('year', type=int)
    data = tb_service.get_map_data(year)
    if isinstance(data, tuple):
        body, status = data
        return jsonify(body), status
    return jsonify(data)


@app.route('/api/trends/<iso3>', methods=['GET'])
@cache.cached(timeout=3600)
def get_country_trends(iso3):
    """Get historical trends for a specific country"""
    start_year = request.args.get('start_year', type=int)
    end_year = request.args.get('end_year', type=int)
    
    data = tb_service.get_country_trends(iso3.upper(), start_year, end_year)
    return jsonify(data)

@app.route('/api/comparison', methods=['GET'])
@cache.cached(timeout=3600)
def get_comparison():
    """Get comparison data across countries"""
    year = request.args.get('year', type=int)
    data = tb_service.get_regional_comparison(year)
    return jsonify(data)

@app.route('/api/yearly-trends', methods=['GET'])
@cache.cached(timeout=3600)
def get_yearly_trends():
    """Get regional yearly trends"""
    data = tb_service.get_yearly_trends()
    return jsonify(data)

@app.route('/api/countries', methods=['GET'])
@cache.cached(timeout=3600)
def get_countries_list():
    """Get list of all countries"""
    countries = [
        {'iso3': iso3, 'name': info['name'], 'coordinates': info['coords']}
        for iso3, info in tb_service.country_mapping.items()
    ]
    return jsonify({'countries': countries})

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'TB Data API'
    })

@app.route('/api/stats', methods=['GET'])
def get_api_stats():
    try:
        if not tb_service.postgres_manager.engine:
            return jsonify({'error': 'PostgreSQL unavailable'}), 500
        with tb_service.postgres_manager.engine.connect() as cx:
            total_records = cx.execute(text("SELECT COUNT(*) FROM tb_analytics.country_summary")).scalar() or 0
            min_year = cx.execute(text("SELECT MIN(year) FROM tb_analytics.country_summary")).scalar()
            max_year = cx.execute(text("SELECT MAX(year) FROM tb_analytics.country_summary")).scalar()
            countries = cx.execute(text("SELECT COUNT(DISTINCT iso3) FROM tb_analytics.country_summary")).scalar() or 0
        return jsonify({
            'total_records': int(total_records),
            'year_range': f"{min_year}-{max_year}" if min_year is not None else "No data",
            'countries_count': int(countries),
            'last_updated': datetime.now().isoformat(),
            'data_source': 'postgresql'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    logger.info("Starting TB Data API server...")
    app.run(debug=True, host='0.0.0.0', port=5000)