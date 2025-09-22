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
import requests

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our modules
from tb_data_collection import ImprovedTBDataCollector, DataUpdateScheduler
from spark_data_processor import FixedTBDataProcessor, DataWarehouse  # Updated import
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

class DockerServiceManager:
    """
    Manager for Docker services (Hadoop, PostgreSQL)
    """
    
    def __init__(self):
        self.compose_file = Path('docker-compose.yml')
    
    def check_docker_installed(self) -> bool:
        """Check if Docker is installed and running"""
        try:
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Docker found: {result.stdout.strip()}")
                return True
        except Exception as e:
            logger.error(f"Docker not found or not running: {e}")
        return False
    
    def check_docker_compose_installed(self) -> bool:
        """Check if Docker Compose is installed"""
        try:
            # Try docker compose (new format)
            result = subprocess.run(['docker', 'compose', 'version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Docker Compose found: {result.stdout.strip()}")
                return True
            
            # Try docker-compose (old format)
            result = subprocess.run(['docker-compose', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"Docker Compose found: {result.stdout.strip()}")
                return True
        except Exception as e:
            logger.error(f"Docker Compose not found: {e}")
        return False
    
    def start_services(self) -> bool:
        """Start Docker services"""
        if not self.compose_file.exists():
            logger.error(f"Docker compose file not found: {self.compose_file}")
            return False
        
        try:
            logger.info("Starting Docker services (Hadoop + PostgreSQL)...")
            
            # Try new docker compose format first
            try:
                result = subprocess.run(['docker', 'compose', 'up', '-d'], 
                                      capture_output=True, text=True, timeout=120)
            except:
                # Fallback to old format
                result = subprocess.run(['docker-compose', 'up', '-d'], 
                                      capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                logger.info("Docker services started successfully")
                return True
            else:
                logger.error(f"Failed to start Docker services: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error starting Docker services: {e}")
            return False
    
    def stop_services(self) -> bool:
        """Stop Docker services"""
        try:
            logger.info("Stopping Docker services...")
            
            try:
                result = subprocess.run(['docker', 'compose', 'down'], 
                                      capture_output=True, text=True, timeout=60)
            except:
                result = subprocess.run(['docker-compose', 'down'], 
                                      capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info("Docker services stopped successfully")
                return True
            else:
                logger.error(f"Failed to stop Docker services: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error stopping Docker services: {e}")
            return False
    
    def wait_for_services(self, timeout: int = 120) -> bool:
        """Wait for Docker services to be ready"""
        logger.info("Waiting for Docker services to be ready...")
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            hadoop_ready = False
            postgres_ready = False
            
            # Check Hadoop NameNode
            try:
                response = requests.get('http://localhost:9870/jmx', timeout=5)
                if response.status_code == 200:
                    hadoop_ready = True
                    logger.info("✓ Hadoop NameNode is ready")
            except:
                pass
            
            # Check PostgreSQL
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host='localhost',
                    port=5433,
                    database='tb_data_warehouse',
                    user='postgres',
                    password='gilbert123'
                )
                conn.close()
                postgres_ready = True
                logger.info("✓ PostgreSQL is ready")
            except:
                pass
            
            if hadoop_ready and postgres_ready:
                logger.info("All Docker services are ready!")
                return True
            
            if not hadoop_ready or not postgres_ready:
                logger.info("Services not ready yet, waiting...")
            time.sleep(5)
        
        logger.error("Services did not become ready within timeout")
        return False
    
    def get_service_status(self) -> Dict:
        """Get status of Docker services"""
        status = {}
        
        # Check Hadoop
        try:
            response = requests.get('http://localhost:9870/jmx', timeout=5)
            status['hadoop'] = 'running' if response.status_code == 200 else 'error'
        except:
            status['hadoop'] = 'stopped'
        
        # Check PostgreSQL
        try:
            import psycopg2
            conn = psycopg2.connect(
                host='localhost', port=5433, database='tb_data_warehouse',
                user='postgres', password='gilbert123'
            )
            conn.close()
            status['postgresql'] = 'running'
        except:
            status['postgresql'] = 'stopped'
        
        return status


class TBPipelineOrchestrator:
    """
    Main orchestrator for the TB data processing pipeline with Docker integration
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or self._load_default_config()
        self.setup_directories()
        
        # Initialize Docker manager
        self.docker_manager = DockerServiceManager()
        
        # Initialize components
        self.collector = ImprovedTBDataCollector()
        # Use the fixed processor
        self.processor = FixedTBDataProcessor(app_name="TB_Southeast_Asia_Fixed_Pipeline")
        self.scheduler = DataUpdateScheduler(self.collector)
        
        # Database connection string (PostgreSQL)
        self.db_connection = self.config.get('database_url', 
            'postgresql://postgres:gilbert123@localhost:5433/tb_data_warehouse')
        self.warehouse = DataWarehouse(self.db_connection)
        
        # Pipeline state
        self.pipeline_running = False
        self.api_server_process = None
        
    def _load_default_config(self) -> Dict:
        return {
            'data_years': {'start_year': 2018, 'end_year': 2023},
            'database_url': 'postgresql://postgres:gilbert123@localhost:5433/tb_data_warehouse',
            'api_host': '0.0.0.0',
            'api_port': 5000,
            'auto_update': True,
            'update_interval_days': 30,
            'cache_timeout': 3600,
            'use_docker': True
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
    
    def setup_docker_environment(self) -> bool:
        """Setup and start Docker services"""
        if not self.config.get('use_docker', True):
            logger.info("Docker integration disabled in configuration")
            return True
        
        logger.info("=" * 60)
        logger.info("DOCKER ENVIRONMENT SETUP")
        logger.info("=" * 60)
        
        # Check Docker installation
        if not self.docker_manager.check_docker_installed():
            logger.error("Docker is not installed or not running")
            return False
        
        if not self.docker_manager.check_docker_compose_installed():
            logger.error("Docker Compose is not installed")
            return False
        
        # Start Docker services
        if not self.docker_manager.start_services():
            logger.error("Failed to start Docker services")
            return False
        
        # Wait for services to be ready
        if not self.docker_manager.wait_for_services():
            logger.error("Docker services failed to become ready")
            return False
        
        logger.info("Docker environment setup completed successfully!")
        return True
    
    def run_complete_pipeline_with_docker(self, force_update: bool = False) -> Dict:
        """Run the complete data pipeline with Docker integration"""
        logger.info("🚀 STARTING COMPLETE TB DATA PIPELINE WITH DOCKER (FIXED)")
        logger.info("🎯 Target: 10 Southeast Asian Countries TB Data Processing")
        logger.info("🔧 Using Fixed Data Processor with Docker-only HDFS")
        pipeline_start = datetime.now()
        
        results = {
            'pipeline_start': pipeline_start.isoformat(),
            'phases': {}
        }
        
        try:
            # Phase 0: Setup Docker Environment
            docker_setup_result = self.setup_docker_environment()
            results['phases']['docker_setup'] = {
                'status': 'success' if docker_setup_result else 'error'
            }
            
            if not docker_setup_result:
                logger.error("Pipeline stopped due to Docker setup failure")
                results.update({
                    'status': 'error',
                    'error': 'docker_setup_failed',
                    'pipeline_end': datetime.now().isoformat()
                })
                return results
            
            # Phase 1: Data Collection
            collection_result = self.run_data_collection(force_update)
            results['phases']['data_collection'] = collection_result
            
            if collection_result['status'] == 'error':
                logger.error("Pipeline stopped due to data collection failure")
                results.update({
                    'status': 'error',
                    'error': collection_result.get('error', 'data_collection_failed'),
                    'pipeline_end': datetime.now().isoformat()
                })
                return results
            elif collection_result['status'] == 'skipped':
                # Load existing data for processing
                logger.info("Loading existing raw data...")
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
            
            # Phase 2: Data Processing with Fixed Processor + Docker HDFS
            processing_result = self.run_fixed_data_processing_with_docker(raw_data)
            results['phases']['data_processing'] = processing_result
            
            if processing_result['status'] == 'error':
                logger.error("Pipeline stopped due to data processing failure")
                results.update({
                    'status': 'error',
                    'error': processing_result.get('error', 'data_processing_failed'),
                    'pipeline_end': datetime.now().isoformat()
                })
                return results
            
            self.ensure_api_data_availability()
            
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
            
            logger.info("=" * 60)
            logger.info("🎉 FIXED PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"⏱️  Total execution time: {total_time:.2f} seconds")
            logger.info(f"🌐 API server available at: http://{self.config['api_host']}:{self.config['api_port']}")
            logger.info("Available endpoints:")
            logger.info("   - GET /api/map-data - Interactive map data")
            logger.info("   - GET /api/trends/{iso3} - Country trends")
            logger.info("   - GET /api/comparison - Regional comparison")
            logger.info("   - GET /api/yearly-trends - Regional yearly trends")
            logger.info("   - GET /api/health - Health check")
            logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            results.update({
                'status': 'error',
                'error': str(e),
                'pipeline_end': datetime.now().isoformat()
            })
            return results
    
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
    
    def run_fixed_data_processing_with_docker(self, raw_data: Dict) -> Dict:
        """Run fixed data processing phase with Apache Spark and Docker HDFS"""
        logger.info("="*60)
        logger.info("PHASE 2: FIXED DATA PROCESSING WITH SPARK & DOCKER HDFS")
        logger.info("="*60)
        
        try:
            start_time = datetime.now()
            logger.info("Starting Fixed Spark-based data processing with Docker HDFS backup...")
            
            # Process data using fixed processor
            result = self.processor.process_complete_pipeline(
                raw_data['tb_data'],
                raw_data['population_data']
            )
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if result['status'] == 'success':
                logger.info(f"Fixed data processing completed in {processing_time:.2f} seconds")
                
                # Get processed DataFrames
                processed_dfs = self.processor.get_processed_dataframes()
                
                # Save to distributed storage systems (Docker HDFS + PostgreSQL)
                self.save_to_distributed_storage(processed_dfs)
                
                return {
                    'status': 'success',
                    'processing_time': processing_time,
                    'quality_report': result['quality_report'],
                    'processed_data': processed_dfs
                }
            else:
                logger.error(f"Fixed data processing failed: {result['error_message']}")
                return {'status': 'error', 'error': result['error_message']}
                
        except Exception as e:
            logger.error(f"Fixed data processing failed: {str(e)}")
            return {'status': 'error', 'error': str(e)}
        finally:
            # Always cleanup Spark session
            self.processor.cleanup()
    
    def save_to_distributed_storage(self, processed_data: Dict):
        """Save processed data to Docker HDFS and PostgreSQL"""
        logger.info("Saving data to distributed storage systems...")
        
        try:
            # Save to PostgreSQL
            if hasattr(tb_service, 'postgres_manager') and tb_service.postgres_manager.engine:
                success = tb_service.postgres_manager.save_data(processed_data)
                if success:
                    logger.info("Data saved to Docker PostgreSQL successfully")
            
            # Save to Docker HDFS (using fixed method)
            if hasattr(self.processor, 'hdfs_manager'):
                self.processor.save_with_docker_hdfs_backup()
                logger.info("Data backed up to Docker HDFS successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving to distributed storage: {str(e)}")
            return False
    
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
    
    def ensure_api_data_availability(self):
        """Ensure API has access to processed data"""
        logger.info("Ensuring API data availability...")

        processed_dir = Path('data/processed')
        csv_map = {
            'tb_country_summary':  processed_dir / 'country_summary.csv',
            'tb_yearly_trends':    processed_dir / 'yearly_trends.csv',
            'tb_country_trends':   processed_dir / 'country_trends.csv',
        }

        missing = [p.name for p in csv_map.values() if not p.exists()]
        if missing:
            logger.error(f"Expected processed CSVs not found: {missing}")
            logger.info("Attempting to regenerate processed data...")
            try:
                raw_data = self.collector.collect_all_data(
                    self.config['data_years']['start_year'],
                    self.config['data_years']['end_year']
                )
                self.run_fixed_data_processing_with_docker(raw_data)
            except Exception as e:
                logger.error(f"Failed to regenerate CSVs: {e}")

            # Re-check after regeneration
            missing = [p.name for p in csv_map.values() if not p.exists()]
            if missing:
                logger.error(f"CSVs still missing: {missing}. Cannot reload API DB from CSV.")
                return

        # Always reload CSVs into the API DB (SQLite) so the API has data even before PG is queried.
        try:
            tb_service._load_csv_to_sqlite()
            logger.info("CSV to API DB reload complete.")
        except Exception as e:
            logger.error(f"CSV to API DB reload failed: {e}")
    
    def cleanup(self):
        """Cleanup resources and optionally stop Docker services"""
        logger.info("Cleaning up resources...")
        
        if hasattr(self, 'processor'):
            self.processor.cleanup()
        
        if self.api_server_process:
            self.api_server_process.terminate()
        
        # Optionally stop Docker services
        if self.config.get('stop_docker_on_exit', False):
            self.docker_manager.stop_services()
        
        logger.info("Cleanup completed")


def main():
    """Main entry point with Docker integration and fixed processing"""
    parser = argparse.ArgumentParser(description='Fixed TB Data Processing Pipeline with Docker')
    parser.add_argument('--mode', choices=['full', 'collect', 'process', 'api', 'schedule', 'docker-status'], 
                       default='full', help='Pipeline execution mode')
    parser.add_argument('--force-update', action='store_true', 
                       help='Force data update even if not needed')
    parser.add_argument('--config', type=str, 
                       help='Path to configuration file')
    parser.add_argument('--years', type=str, 
                       help='Year range (e.g., 2018-2023)')
    parser.add_argument('--no-docker', action='store_true',
                       help='Disable Docker integration')
    parser.add_argument('--stop-docker-on-exit', action='store_true',
                       help='Stop Docker services when pipeline exits')
    
    args = parser.parse_args()
    
    # Initialize orchestrator with fixed configuration
    config = {}
    if args.years:
        start_year, end_year = map(int, args.years.split('-'))
        config['data_years'] = {'start_year': start_year, 'end_year': end_year}
    
    if args.no_docker:
        config['use_docker'] = False
    
    if args.stop_docker_on_exit:
        config['stop_docker_on_exit'] = True
    
    orchestrator = TBPipelineOrchestrator(config)
    
    def signal_handler(signum, frame):
        logger.info("Received interrupt signal, cleaning up...")
        orchestrator.cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.mode == 'docker-status':
            # Check Docker service status
            status = orchestrator.docker_manager.get_service_status()
            print("Docker Service Status:")
            for service, state in status.items():
                print(f"  {service}: {state}")
            return
            
        elif args.mode == 'full':
            # Run complete pipeline with fixed processor
            result = orchestrator.run_complete_pipeline_with_docker(args.force_update)
            
            if result['status'] == 'success':
                # Keep API server running
                logger.info("Fixed pipeline completed. API server is running...")
                logger.info("Press Ctrl+C to stop the server")
                
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Shutting down...")
            else:
                logger.error("Fixed pipeline failed")
                sys.exit(1)
                
        elif args.mode == 'collect':
            # Run only data collection
            result = orchestrator.run_data_collection(args.force_update)
            print(f"Data collection result: {result}")
            
        elif args.mode == 'process':
            # Run only fixed data processing (requires existing raw data)
            logger.info("Loading existing raw data for fixed processing...")
            raw_data = orchestrator.collector.collect_all_data(
                config.get('data_years', {}).get('start_year', 2018),
                config.get('data_years', {}).get('end_year', 2023)
            )
            result = orchestrator.run_fixed_data_processing_with_docker(raw_data)
            print(f"Fixed data processing result: {result}")
            
        elif args.mode == 'api':
            # Run only API server
            result = orchestrator.start_api_server(background=False)
            
        elif args.mode == 'schedule':
            # Run scheduled updates with fixed processor
            logger.info("Running scheduled updates with fixed processor...")
            try:
                while True:
                    if orchestrator.scheduler.should_update():
                        logger.info("Running scheduled data update...")
                        result = orchestrator.run_complete_pipeline_with_docker(force_update=True)
                        if result['status'] == 'success':
                            logger.info("Scheduled update completed successfully")
                        else:
                            logger.error(f"Scheduled update failed: {result.get('error', 'Unknown error')}")
                    
                    # Wait for next check (check every hour)
                    time.sleep(3600)
            except KeyboardInterrupt:
                logger.info("Scheduled updates stopped by user")
            
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        orchestrator.cleanup()
        sys.exit(1)
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    main()