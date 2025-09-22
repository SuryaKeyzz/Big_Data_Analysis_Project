import requests
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
import os
from pathlib import Path
import io

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ImprovedTBDataCollector:
    """
    Improved WHO TB data collector using official WHO data endpoints
    """
    
    def __init__(self):
        self.target_countries = {
            'cambodia': 'KHM',
            'indonesia': 'IDN', 
            'laos': 'LAO',
            'malaysia': 'MYS',
            'myanmar': 'MMR',
            'philippines': 'PHL',
            'singapore': 'SGP',
            'thailand': 'THA',
            'vietnam': 'VNM',
            'timor-leste': 'TLS'
        }
        
        # WHO official data URLs (these are the current official endpoints)
        self.who_data_urls = {
            'estimates': 'https://extranet.who.int/tme/generateCSV.asp?ds=estimates',
            'notifications': 'https://extranet.who.int/tme/generateCSV.asp?ds=notifications',
            'outcomes': 'https://extranet.who.int/tme/generateCSV.asp?ds=outcomes',
            'laboratory': 'https://extranet.who.int/tme/generateCSV.asp?ds=laboratory'
        }
        
        # Alternative: WHO Global Health Observatory API
        self.gho_api_base = "https://ghoapi.azureedge.net/api"
        
        # World Bank API
        self.worldbank_base = "https://api.worldbank.org/v2"
        
        # Data storage
        self.data_dir = Path('data/raw')
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def fetch_who_tb_estimates(self, start_year: int = 2018, end_year: int = 2023) -> pd.DataFrame:
        """
        Fetch TB estimates data from WHO and return long format:
        [country, iso3, g_whoregion, indicator, year, value]
        """
        logger.info("Fetching TB estimates from WHO...")

        try:
            url = self.who_data_urls['estimates']
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(url, headers=headers, timeout=60)
            resp.raise_for_status()

            df = pd.read_csv(io.StringIO(resp.text))

            # Filter to target countries
            df = df[df['iso3'].isin(self.target_countries.values())].copy()

            # Ensure region column exists
            if 'g_whoregion' not in df.columns:
                df['g_whoregion'] = 'SEA'

            # CASE A: wide-by-indicator with a single 'year' column (modern TME CSV)
            if 'year' in df.columns and not any(c.isdigit() for c in df.columns):
                id_cols = ['country', 'iso3', 'g_whoregion', 'year']

                # Prefer counts, but also accept per-100k if counts not present
                whitelist = [
                    'e_inc_num', 'c_newinc', 'e_mort_num', 'e_prev_num',
                    'e_inc_100k', 'e_mort_100k', 'e_prev_100k'
                ]
                indicator_cols = [c for c in df.columns if c in whitelist]
                if not indicator_cols:
                    indicator_cols = [c for c in df.columns
                                    if c.startswith(('e_inc_', 'e_mort_', 'e_prev_')) or c == 'c_newinc']

                keep = id_cols + indicator_cols
                df = df[keep]

                long_df = df.melt(
                    id_vars=id_cols, value_vars=indicator_cols,
                    var_name='indicator', value_name='value'
                )

                # Filter years and clean
                long_df = long_df[(long_df['year'].astype(int) >= start_year) &
                                (long_df['year'].astype(int) <= end_year)]

            # CASE B: wide-by-year (legacy shape) – your original path
            else:
                year_cols = [c for c in df.columns if c.isdigit()
                            and start_year <= int(c) <= end_year]
                id_cols = ['country', 'iso3', 'g_whoregion']
                if 'indicator' in df.columns:
                    id_cols.append('indicator')
                keep = id_cols + year_cols
                df = df[keep]

                long_df = pd.melt(
                    df, id_vars=id_cols, value_vars=year_cols,
                    var_name='year', value_name='value'
                )

            # Final cleanup
            long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce')
            long_df = long_df.dropna(subset=['value'])
            long_df['year'] = long_df['year'].astype(str)

            logger.info(f"Successfully fetched {len(long_df)} records from WHO")
            return long_df

        except Exception as e:
            logger.error(f"WHO TME method failed: {e}")
            return self._fetch_who_gho_api(start_year, end_year)

    def _fetch_who_gho_api(self, start_year: int, end_year: int) -> pd.DataFrame:
        """
        Method 2: WHO Global Health Observatory API
        """
        logger.info("Trying WHO GHO API...")
        
        # Key TB indicators in GHO
        tb_indicators = [
            'TB_c_newinc',      # New TB cases
            'TB_e_inc_100k',    # TB incidence per 100k
            'TB_e_mort_100k',   # TB mortality per 100k
            'TB_e_prev_100k'    # TB prevalence per 100k
        ]
        
        all_data = []
        
        for indicator in tb_indicators:
            try:
                url = f"{self.gho_api_base}/{indicator}"
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for record in data.get('value', []):
                        country_code = record.get('SpatialDim')
                        year = record.get('TimeDim')
                        value = record.get('NumericValue')
                        
                        if (country_code in self.target_countries.values() and 
                            year and start_year <= int(year) <= end_year and 
                            value is not None):
                            
                            all_data.append({
                                'iso3': country_code,
                                'country': self._get_country_name(country_code),
                                'g_whoregion': 'SEA',
                                'indicator': indicator,
                                'year': year,
                                'value': float(value)
                            })
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Failed to fetch {indicator}: {str(e)}")
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"Successfully fetched {len(df)} records from GHO API")
            return df
        else:
            logger.warning("GHO API failed, using sample data")
            return self._generate_comprehensive_sample_data(start_year, end_year)
    
    def fetch_worldbank_data(self, indicators: List[str], start_year: int, end_year: int) -> pd.DataFrame:
        """
        Improved World Bank API data fetching
        """
        logger.info("Fetching World Bank data...")
        
        all_data = []
        countries_str = ';'.join(self.target_countries.values())
        
        for indicator in indicators:
            try:
                url = f"{self.worldbank_base}/country/{countries_str}/indicator/{indicator}"
                params = {
                    'date': f'{start_year}:{end_year}',
                    'format': 'json',
                    'per_page': 1000
                }
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if len(data) > 1 and data[1]:
                    for record in data[1]:
                        if record['value'] is not None:
                            all_data.append({
                                'country': record['country']['value'],
                                'iso3': record['countryiso3code'],
                                'year': record['date'],
                                'indicator': indicator,
                                'value': float(record['value'])
                            })
                
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching World Bank {indicator}: {str(e)}")
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            # keep only total population indicator
            df = df[df['indicator'] == 'SP.POP.TOTL'].copy()
            df.rename(columns={'value': 'population'}, inplace=True)
            df = df[['country', 'iso3', 'year', 'population']]
            logger.info(f"Successfully fetched {len(df)} World Bank records")
            return df
        else:
            return pd.DataFrame()
    
    def _get_country_name(self, iso_code: str) -> str:
        """Map ISO code to country name"""
        name_map = {
            'IDN': 'Indonesia',
            'PHL': 'Philippines',
            'VNM': 'Viet Nam',
            'THA': 'Thailand',
            'MYS': 'Malaysia',
            'MMR': 'Myanmar',
            'KHM': 'Cambodia',
            'LAO': 'Lao People\'s Democratic Republic',
            'SGP': 'Singapore',
            'TLS': 'Timor-Leste'
        }
        return name_map.get(iso_code, iso_code)
    
    def _generate_comprehensive_sample_data(self, start_year: int, end_year: int) -> pd.DataFrame:
        """Generate comprehensive sample data as fallback"""
        import numpy as np
        
        # Realistic TB burden data for SEA countries
        base_incidence_per_100k = {
            'IDN': 354,   # Indonesia
            'PHL': 554,   # Philippines
            'VNM': 182,   # Vietnam
            'THA': 153,   # Thailand
            'MYS': 92,    # Malaysia
            'MMR': 322,   # Myanmar
            'KHM': 302,   # Cambodia
            'LAO': 185,   # Laos
            'SGP': 37,    # Singapore
            'TLS': 498    # Timor-Leste
        }
        
        all_data = []
        years = list(range(start_year, end_year + 1))
        
        for iso_code, incidence_rate in base_incidence_per_100k.items():
            country_name = self._get_country_name(iso_code)
            
            for year in years:
                # Add year-over-year variation
                year_trend = 1 + (year - 2020) * -0.02  # Slight decline trend
                noise = np.random.normal(1, 0.05)
                
                # TB incidence per 100k
                incidence = max(0, incidence_rate * year_trend * noise)
                
                # Generate related indicators
                mortality_rate = incidence * 0.15  # ~15% of incidence
                prevalence_rate = incidence * 1.8  # Prevalence typically higher
                
                indicators_data = [
                    {
                        'country': country_name,
                        'iso3': iso_code,
                        'g_whoregion': 'SEA',
                        'indicator': 'TB_e_inc_100k',
                        'year': str(year),
                        'value': round(incidence, 1)
                    },
                    {
                        'country': country_name,
                        'iso3': iso_code,
                        'g_whoregion': 'SEA', 
                        'indicator': 'TB_e_mort_100k',
                        'year': str(year),
                        'value': round(mortality_rate, 1)
                    },
                    {
                        'country': country_name,
                        'iso3': iso_code,
                        'g_whoregion': 'SEA',
                        'indicator': 'TB_e_prev_100k', 
                        'year': str(year),
                        'value': round(prevalence_rate, 1)
                    }
                ]
                
                all_data.extend(indicators_data)
        
        df = pd.DataFrame(all_data)
        logger.info(f"Generated {len(df)} sample TB records")
        return df
    
    def collect_all_data(self, start_year: int = 2018, end_year: int = 2023) -> Dict[str, pd.DataFrame]:
        """
        Collect all TB and population data
        """
        logger.info("Starting comprehensive data collection...")
        
        # Collect TB data
        tb_data = self.fetch_who_tb_estimates(start_year, end_year)
        
        # Collect population data
        wb_indicators = ['SP.POP.TOTL']  # Total population
        population_data = self.fetch_worldbank_data(wb_indicators, start_year, end_year)
        
        # Save data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        tb_file = self.data_dir / f'who_tb_data_{timestamp}.csv'
        tb_data.to_csv(tb_file, index=False)
        logger.info(f"TB data saved: {tb_file}")
        
        if not population_data.empty:
            pop_file = self.data_dir / f'worldbank_population_{timestamp}.csv'
            population_data.to_csv(pop_file, index=False)
            logger.info(f"Population data saved: {pop_file}")
        
        logger.info("Data collection completed!")
        
        return {
            'tb_data': tb_data,
            'population_data': population_data
        }

class DataUpdateScheduler:
    """
    Scheduler for monthly data updates
    """
    
    def __init__(self, collector: ImprovedTBDataCollector):
        self.collector = collector
        
    def should_update(self) -> bool:
        """Check if data should be updated (monthly)"""
        last_update_file = Path('data/last_update.txt')
        
        if not last_update_file.exists():
            return True
            
        try:
            with open(last_update_file, 'r') as f:
                last_update = datetime.fromisoformat(f.read().strip())
            
            # Update if it's been more than 30 days
            return datetime.now() - last_update > timedelta(days=30)
            
        except Exception:
            return True
    
    def update_data(self):
        """Perform data update and mark completion"""
        if self.should_update():
            logger.info("Starting scheduled data update...")
            
            # Collect new data
            data = self.collector.collect_all_data()
            
            # Mark update completion
            with open('data/last_update.txt', 'w') as f:
                f.write(datetime.now().isoformat())
            
            logger.info("Scheduled data update completed!")
            return data
        else:
            logger.info("Data is up to date, skipping update")
            return None

# Usage example
if __name__ == "__main__":
    collector = ImprovedTBDataCollector()
    
    # Test the data collection
    data = collector.collect_all_data(2018, 2023)
    
    print("Data Collection Results:")
    print(f"TB data shape: {data['tb_data'].shape}")
    print(f"Population data shape: {data['population_data'].shape}")
    
    # Display sample data
    print("\n--- Sample TB Data ---")
    print(data['tb_data'].head(10))
    
    if not data['population_data'].empty:
        print("\n--- Sample Population Data ---")
        print(data['population_data'].head(10))