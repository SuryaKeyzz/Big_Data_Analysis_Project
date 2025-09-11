
import os
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import subprocess
import signal
import time
import threading

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our modules
from tb_data_collection import TBDataCollector, DataUpdateScheduler
from spark_data_processor import TBDataProcessor, DataWarehouse
from flask_api_server import app, tb_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tb_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TBPipelineOrchestrator:
    """
    Main orchestrator for the TB data processing pipeline
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._load_default_config()
        self.setup_directories()
        
        # Initialize components
        self.collector = TBDataCollector()
        self.processor = TBDataProcessor(app_name="TB_Southeast_Asia_Pipeline")
        self.scheduler = DataUpdateScheduler(self.collector)
        
        # Database connection string (PostgreSQL or SQLite)
        self.db_connection = self.config.get('database_url', 'sqlite:///data/processed/tb_data.sqlite')
        self.warehouse = DataWarehouse(self.db_connection) if 'postgresql' in self.db_connection else None
        
        # Pipeline state
        self.pipeline_running = False
        self.api_server_process = None
        
    def _load_default_config(self) -> Dict:
        """Load default configuration"""
        return {
            'data_years': {
                'start_year': 2018,
                'end_year': 2023
            },
            'database_url': 'sqlite:///data/processed/tb_data.sqlite',
            'api_host': '0.0.0.0',
            'api_port': 5000,
            'auto_update': True,
            'update_interval_days': 30,
            'cache_timeout': 3600
        }
    
    def setup_directories(self):
        """Create necessary directories"""
        directories = [
            'data/raw',
            'data/processed', 
            'logs',
            'backups'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        
        logger.info("Directory structure created")
    
    def run_data_collection(self, force_update: bool = False) -> Dict:
        """Run data collection phase"""
        logger.info("="*60)
        logger.info("PHASE 1: DATA COLLECTION")
        logger.info("="*60)
        
        try:
            # Check if update is needed
            if not force_update and not self.scheduler.should_update():
                logger.info("Data is up to date, skipping collection")
                return {'status': 'skipped', 'reason': 'data_up_to_date'}
            
            # Collect data
            start_time = datetime.now()
            logger.info("Starting data collection from WHO and World Bank APIs...")
            
            raw_data = self.collector.collect_all_data(
                start_year=self.config['data_years']['start_year'],
                end_year=self.config['data_years']['end_year']
            )
            
            collection_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Data collection completed in {collection_time:.2f} seconds")
            logger.info(f"TB data records: {len(raw_data['tb_data'])}")
            logger.info(f"Population data records: {len(raw_data['population_data'])}")
            
            return {
                'status': 'success',
                'data': raw_data,
                'collection_time': collection_time,
                'records_collected': {
                    'tb_data': len(raw_data['tb_data']),
                    'population_data': len(raw_data['population_data'])
                }
            }
            
        except Exception as e:
            logger.error(f"Data collection failed: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def run_data_processing(self, raw_data: Dict) -> Dict:
        """Run data processing phase with Apache Spark"""
        logger.info("="*60)
        logger.info("PHASE 2: DATA PROCESSING WITH APACHE SPARK")
        logger.info("="*60)
        
        try:
            start_time = datetime.now()
            logger.info("Starting Spark-based data processing...")
            
            # Process data using Spark
            result = self.processor.process_complete_pipeline(
                raw_data['tb_data'],
                raw_data['population_data']
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if result['status'] == 'success':
                logger.info(f"Data processing completed in {processing_time:.2f} seconds")
                logger.info("Quality Report:")
                for key, value in result['quality_report'].items():
                    logger.info(f"  {key}: {value}")
                
                # Get processed DataFrames for database storage
                processed_dfs = self.processor.get_processed_dataframes()
                
                # Save to database if PostgreSQL is configured
                if self.warehouse:
                    logger.info("Saving processed data to PostgreSQL database...")
                    self.warehouse.create_tables()
                    self.warehouse.save_to_database(processed_dfs)
                
                return {
                    'status': 'success',
                    'processing_time': processing_time,
                    'quality_report': result['quality_report'],
                    'processed_data': processed_dfs
                }
            else:
                logger.error(f"Data processing failed: {result['error_message']}")
                return {'status': 'error', 'error': result['error_message']}
                
        except Exception as e:
            logger.error(f"Data processing failed: {str(e)}")
            return {'status': 'error', 'error': str(e)}
        finally:
            # Always cleanup Spark session
            self.processor.cleanup()
    
    def start_api_server(self, background: bool = True) -> Dict:
        """Start Flask API server"""
        logger.info("="*60)
        logger.info("PHASE 3: STARTING API SERVER")
        logger.info("="*60)
        
        try:
            if background:
                # Start API server in background
                logger.info(f"Starting Flask API server on {self.config['api_host']}:{self.config['api_port']}")
                
                def run_server():
                    app.run(
                        host=self.config['api_host'],
                        port=self.config['api_port'],
                        debug=False,
                        use_reloader=False
                    )
                
                server_thread = threading.Thread(target=run_server, daemon=True)
                server_thread.start()
                
                # Wait a moment to ensure server starts
                time.sleep(2)
                
                logger.info("API server started successfully in background")
                return {'status': 'success', 'mode': 'background'}
            else:
                # Start API server in foreground
                logger.info("Starting Flask API server in foreground...")
                app.run(
                    host=self.config['api_host'],
                    port=self.config['api_port'],
                    debug=True
                )
                return {'status': 'success', 'mode': 'foreground'}
                
        except Exception as e:
            logger.error(f"Failed to start API server: {str(e)}")
            return {'status': 'error', 'error': str(e)}
    
    def run_complete_pipeline(self, force_update: bool = False) -> Dict:
        """Run the complete data pipeline"""
        logger.info("🚀 STARTING COMPLETE TB DATA PIPELINE")
        logger.info("🎯 Target: 10 Southeast Asian Countries TB Data Processing")
        pipeline_start = datetime.now()
        
        results = {
            'pipeline_start': pipeline_start.isoformat(),
            'phases': {}
        }
        
        try:
            # Phase 1: Data Collection
            collection_result = self.run_data_collection(force_update)
            results['phases']['data_collection'] = collection_result
            
            if collection_result['status'] == 'error':
                logger.error("Pipeline stopped due to data collection failure")
                return results
            elif collection_result['status'] == 'skipped':
                # Load existing data for processing
                logger.info("Loading existing raw data...")
                # Load from most recent files
                raw_data_files = list(Path('data/raw').glob('*_data_*.csv'))
                if raw_data_files:
                    import pandas as pd
                    latest_tb = max([f for f in raw_data_files if 'who_tb' in f.name])
                    latest_pop = max([f for f in raw_data_files if 'worldbank_population' in f.name])
                    
                    raw_data = {
                        'tb_data': pd.read_csv(latest_tb),
                        'population_data': pd.read_csv(latest_pop)
                    }
                    logger.info("Existing raw data loaded successfully")
                else:
                    logger.warning("No existing data found, generating sample data")
                    raw_data = self.collector.collect_all_data(
                        self.config['data_years']['start_year'],
                        self.config['data_years']['end_year']
                    )
            else:
                raw_data = collection_result['data']
            
            # Phase 2: Data Processing
            processing_result = self.run_data_processing(raw_data)
            results['phases']['data_processing'] = processing_result
            
            if processing_result['status'] == 'error':
                logger.error("Pipeline stopped due to data processing failure")
                return results
            
            # Phase 3: Start API Server
            api_result = self.start_api_server(background=True)
            results['phases']['api_server'] = api_result
            
            # Calculate total pipeline time
            pipeline_end = datetime.now()
            total_time = (pipeline_end - pipeline_start).total_seconds()
            
            results.update({
                'pipeline_end': pipeline_end.isoformat(),
                'total_duration_seconds': total_time,
                'status': 'success'
            })
            
            logger.info("="*60)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"⏱️  Total execution time: {total_time:.2f} seconds")
            logger.info(f"🌐 API server available at: http://{self.config['api_host']}:{self.config['api_port']}")
            logger.info("📊 Available endpoints:")
            logger.info("   - GET /api/map-data - Interactive map data")
            logger.info("   - GET /api/trends/{iso3} - Country trends")
            logger.info("   - GET /api/comparison - Regional comparison")
            logger.info("   - GET /api/yearly-trends - Regional yearly trends")
            logger.info("   - GET /api/health - Health check")
            logger.info("="*60)
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            results.update({
                'status': 'error',
                'error': str(e),
                'pipeline_end': datetime.now().isoformat()
            })
            return results
    
    def run_scheduled_updates(self):
        """Run scheduled data updates"""
        logger.info("Starting scheduled update service...")
        
        while True:
            try:
                if self.scheduler.should_update():
                    logger.info("Running scheduled data update...")
                    self.run_complete_pipeline(force_update=True)
                
                # Sleep for 1 day before checking again
                time.sleep(86400)
                
            except KeyboardInterrupt:
                logger.info("Scheduled updates stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in scheduled update: {str(e)}")
                time.sleep(3600)  # Wait 1 hour before retry
    
    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        
        if hasattr(self, 'processor'):
            self.processor.cleanup()
        
        if self.api_server_process:
            self.api_server_process.terminate()
        
        logger.info("Cleanup completed")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='TB Data Processing Pipeline')
    parser.add_argument('--mode', choices=['full', 'collect', 'process', 'api', 'schedule'], 
                       default='full', help='Pipeline execution mode')
    parser.add_argument('--force-update', action='store_true', 
                       help='Force data update even if not needed')
    parser.add_argument('--config', type=str, 
                       help='Path to configuration file')
    parser.add_argument('--years', type=str, 
                       help='Year range (e.g., 2018-2023)')
    
    args = parser.parse_args()
    
    # Initialize orchestrator
    config = {}
    if args.years:
        start_year, end_year = map(int, args.years.split('-'))
        config['data_years'] = {'start_year': start_year, 'end_year': end_year}
    
    orchestrator = TBPipelineOrchestrator(config)
    
    def signal_handler(signum, frame):
        logger.info("Received interrupt signal, cleaning up...")
        orchestrator.cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.mode == 'full':
            # Run complete pipeline
            result = orchestrator.run_complete_pipeline(args.force_update)
            
            if result['status'] == 'success':
                # Keep API server running
                logger.info("Pipeline completed. API server is running...")
                logger.info("Press Ctrl+C to stop the server")
                
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Shutting down...")
            else:
                logger.error("Pipeline failed")
                sys.exit(1)
                
        elif args.mode == 'collect':
            # Run only data collection
            result = orchestrator.run_data_collection(args.force_update)
            print(f"Data collection result: {result}")
            
        elif args.mode == 'process':
            # Run only data processing (requires existing raw data)
            logger.info("Loading existing raw data for processing...")
            raw_data = orchestrator.collector.collect_all_data(
                config.get('data_years', {}).get('start_year', 2018),
                config.get('data_years', {}).get('end_year', 2023)
            )
            result = orchestrator.run_data_processing(raw_data)
            print(f"Data processing result: {result}")
            
        elif args.mode == 'api':
            # Run only API server
            result = orchestrator.start_api_server(background=False)
            
        elif args.mode == 'schedule':
            # Run scheduled updates
            orchestrator.run_scheduled_updates()
            
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        orchestrator.cleanup()
        sys.exit(1)
    finally:
        orchestrator.cleanup()

if __name__ == "__main__":
    main()