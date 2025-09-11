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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration

# Configure caching
cache = Cache(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 3600  # 1 hour cache
})

class TBAPIService:
    """
    Service class for TB data API operations
    """
    
    def __init__(self, data_dir: str = 'data/processed'):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / 'tb_data.sqlite'
        self.csv_backup_dir = self.data_dir
        
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
        
    def _load_csv_to_sqlite(self):
        """Load CSV data into SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Load CSV files if they exist
            csv_files = {
                'tb_country_summary': 'country_summary.csv',
                'tb_yearly_trends': 'yearly_trends.csv', 
                'tb_country_trends': 'country_trends.csv'
            }
            
            for table_name, csv_file in csv_files.items():
                csv_path = self.csv_backup_dir / csv_file
                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                    logger.info(f"Loaded {csv_file} into {table_name}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error loading CSV to SQLite: {str(e)}")
            
    def get_map_data(self, year: Optional[int] = None) -> Dict:
        """Get data for interactive map visualization"""
        try:
            # Use latest year if not specified
            if year is None:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(year) FROM tb_country_summary")
                year = cursor.fetchone()[0] or 2023
                conn.close()
            
            # Get country data for the specified year
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT iso3, country, year, total_cases, new_cases, deaths, 
                       population, total_cases_per_100k, new_cases_per_100k, deaths_per_100k,
                       case_fatality_rate
                FROM tb_country_summary 
                WHERE year = ?
                ORDER BY total_cases DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(year,))
            conn.close()
            
            # Format data for map
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
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting map data: {str(e)}")
            # Return sample data as fallback
            return self._get_sample_map_data(year or 2023)
            
    def get_country_trends(self, iso3: str, start_year: Optional[int] = None, end_year: Optional[int] = None) -> Dict:
        """Get historical trends for a specific country"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Build query with optional year filters
            query = """
                SELECT year, total_cases, new_cases, deaths, 
                       total_cases_per_100k, new_cases_per_100k, deaths_per_100k
                FROM tb_country_trends 
                WHERE iso3 = ?
            """
            params = [iso3]
            
            if start_year:
                query += " AND year >= ?"
                params.append(start_year)
                
            if end_year:
                query += " AND year <= ?"
                params.append(end_year)
                
            query += " ORDER BY year"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            if df.empty:
                return {'error': f'No data found for country {iso3}'}
            
            # Format trend data
            trends = []
            for _, row in df.iterrows():
                trends.append({
                    'year': int(row['year']),
                    'total_cases': int(row['total_cases']) if pd.notna(row['total_cases']) else 0,
                    'new_cases': int(row['new_cases']) if pd.notna(row['new_cases']) else 0,
                    'deaths': int(row['deaths']) if pd.notna(row['deaths']) else 0,
                    'total_cases_per_100k': float(row['total_cases_per_100k']) if pd.notna(row['total_cases_per_100k']) else 0,
                    'new_cases_per_100k': float(row['new_cases_per_100k']) if pd.notna(row['new_cases_per_100k']) else 0,
                    'deaths_per_100k': float(row['deaths_per_100k']) if pd.notna(row['deaths_per_100k']) else 0
                })
            
            # Get country name
            country_name = self.country_mapping.get(iso3, {}).get('name', iso3)
            
            return {
                'iso3': iso3,
                'country': country_name,
                'trends': trends,
                'summary': {
                    'years_available': len(trends),
                    'latest_year': max(trend['year'] for trend in trends),
                    'latest_cases': trends[-1]['total_cases'] if trends else 0
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting country trends: {str(e)}")
            return {'error': str(e)}
            
    def get_regional_comparison(self, year: Optional[int] = None) -> Dict:
        """Get comparison data across all countries"""
        try:
            # Use latest year if not specified
            if year is None:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(year) FROM tb_country_summary")
                year = cursor.fetchone()[0] or 2023
                conn.close()
            
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT iso3, country, total_cases, new_cases, deaths, population,
                       total_cases_per_100k, new_cases_per_100k, deaths_per_100k,
                       case_fatality_rate, new_case_rate
                FROM tb_country_summary 
                WHERE year = ?
                ORDER BY total_cases DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(year,))
            conn.close()
            
            # Format comparison data
            countries = []
            for _, row in df.iterrows():
                countries.append({
                    'iso3': row['iso3'],
                    'country': row['country'],
                    'total_cases': int(row['total_cases']) if pd.notna(row['total_cases']) else 0,
                    'new_cases': int(row['new_cases']) if pd.notna(row['new_cases']) else 0,
                    'deaths': int(row['deaths']) if pd.notna(row['deaths']) else 0,
                    'population': int(row['population']) if pd.notna(row['population']) else 0,
                    'total_cases_per_100k': float(row['total_cases_per_100k']) if pd.notna(row['total_cases_per_100k']) else 0,
                    'new_cases_per_100k': float(row['new_cases_per_100k']) if pd.notna(row['new_cases_per_100k']) else 0,
                    'deaths_per_100k': float(row['deaths_per_100k']) if pd.notna(row['deaths_per_100k']) else 0,
                    'case_fatality_rate': float(row['case_fatality_rate']) if pd.notna(row['case_fatality_rate']) else 0,
                    'new_case_rate': float(row['new_case_rate']) if pd.notna(row['new_case_rate']) else 0
                })
            
            # Calculate rankings
            rankings = {
                'highest_cases': sorted(countries, key=lambda x: x['total_cases'], reverse=True)[:3],
                'highest_deaths': sorted(countries, key=lambda x: x['deaths'], reverse=True)[:3],
                'highest_rate': sorted(countries, key=lambda x: x['total_cases_per_100k'], reverse=True)[:3]
            }
            
            return {
                'year': year,
                'countries': countries,
                'rankings': rankings,
                'total_countries': len(countries)
            }
            
        except Exception as e:
            logger.error(f"Error getting regional comparison: {str(e)}")
            return {'error': str(e)}
            
    def get_yearly_trends(self) -> Dict:
        """Get regional yearly trends"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT year, total_cases_region, new_cases_region, deaths_region,
                       total_population, avg_cases_per_100k, avg_case_fatality_rate
                FROM tb_yearly_trends 
                ORDER BY year
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Format trends data
            trends = []
            for _, row in df.iterrows():
                trends.append({
                    'year': int(row['year']),
                    'total_cases': int(row['total_cases_region']) if pd.notna(row['total_cases_region']) else 0,
                    'new_cases': int(row['new_cases_region']) if pd.notna(row['new_cases_region']) else 0,
                    'deaths': int(row['deaths_region']) if pd.notna(row['deaths_region']) else 0,
                    'population': int(row['total_population']) if pd.notna(row['total_population']) else 0,
                    'avg_cases_per_100k': float(row['avg_cases_per_100k']) if pd.notna(row['avg_cases_per_100k']) else 0,
                    'avg_case_fatality_rate': float(row['avg_case_fatality_rate']) if pd.notna(row['avg_case_fatality_rate']) else 0
                })
            
            return {
                'trends': trends,
                'years_available': len(trends),
                'latest_year': max(trend['year'] for trend in trends) if trends else None
            }
            
        except Exception as e:
            logger.error(f"Error getting yearly trends: {str(e)}")
            return {'error': str(e)}
            
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
    """Get TB data for map visualization"""
    year = request.args.get('year', type=int)
    data = tb_service.get_map_data(year)
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
    """Get API statistics and data freshness"""
    try:
        conn = sqlite3.connect(tb_service.db_path)
        cursor = conn.cursor()
        
        # Get data statistics
        cursor.execute("SELECT COUNT(*) FROM tb_country_summary")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(year), MAX(year) FROM tb_country_summary")
        year_range = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(DISTINCT iso3) FROM tb_country_summary")
        countries_count = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_records': total_records,
            'year_range': f"{year_range[0]}-{year_range[1]}" if year_range[0] else "No data",
            'countries_count': countries_count,
            'last_updated': datetime.now().isoformat()
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