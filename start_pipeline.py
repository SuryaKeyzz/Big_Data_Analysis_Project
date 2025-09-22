#!/usr/bin/env python3
"""
TB Data Pipeline Startup Script with Docker Integration
Easy-to-use script for starting the complete TB data processing pipeline
"""

import os
import sys
import time
import subprocess
from pathlib import Path

def print_banner():
    print("=" * 70)
    print("🏥 TB DATA PROCESSING PIPELINE WITH DOCKER INTEGRATION")
    print("📊 Southeast Asia TB Epidemic Analysis System")
    print("=" * 70)

def check_requirements():
    """Check if all requirements are met"""
    print("🔍 Checking requirements...")
    
    # Check Python
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return False
    print("✅ Python version OK")
    
    # Check if virtual environment is activated
    if not hasattr(sys, 'real_prefix') and not (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("⚠️  Virtual environment not detected - recommended to use venv")
    else:
        print("✅ Virtual environment active")
    
    # Check Docker
    try:
        result = subprocess.run(['docker', '--version'], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print("✅ Docker available")
        else:
            print("❌ Docker not available")
            return False
    except:
        print("❌ Docker not found")
        return False
    
    # Check Docker Compose
    try:
        result = subprocess.run(['docker', 'compose', 'version'], capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            result = subprocess.run(['docker-compose', '--version'], capture_output=True, text=True, timeout=5)
        
        if result.returncode == 0:
            print("✅ Docker Compose available")
        else:
            print("❌ Docker Compose not available")
            return False
    except:
        print("❌ Docker Compose not found")
        return False
    
    # Check docker-compose.yml exists
    if not Path('docker-compose.yml').exists():
        print("❌ docker-compose.yml not found")
        return False
    print("✅ docker-compose.yml found")
    
    return True

def install_python_requirements():
    """Install required Python packages"""
    print("📦 Installing Python requirements...")
    
    requirements = [
        'pyspark>=3.3.0',
        'pandas>=1.5.0',
        'flask>=2.0.0',
        'flask-cors',
        'flask-caching', 
        'requests>=2.25.0',
        'sqlalchemy>=1.4.0',
        'psycopg2-binary>=2.9.0',
        'hdfs3',
        'numpy>=1.21.0'
    ]
    
    for package in requirements:
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install', package], 
                         check=True, capture_output=True)
            print(f"✅ {package}")
        except subprocess.CalledProcessError:
            print(f"❌ Failed to install {package}")
            return False
    
    return True

def start_docker_services():
    """Start Docker services"""
    print("🐳 Starting Docker services...")
    
    try:
        # Try new docker compose format first
        try:
            result = subprocess.run(['docker', 'compose', 'up', '-d'], 
                                  capture_output=True, text=True, timeout=120)
        except:
            # Fallback to old format
            result = subprocess.run(['docker-compose', 'up', '-d'], 
                                  capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ Docker services started")
            return True
        else:
            print(f"❌ Failed to start Docker services: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error starting Docker services: {e}")
        return False

def wait_for_services():
    """Wait for services to be ready"""
    print("⏳ Waiting for services to be ready...")
    
    import requests
    
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Check Hadoop NameNode
            hadoop_ready = False
            try:
                response = requests.get('http://localhost:9870/jmx', timeout=5)
                if response.status_code == 200:
                    hadoop_ready = True
            except:
                pass
            
            # Check PostgreSQL
            postgres_ready = False
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host='localhost', port=5433, database='tb_data_warehouse',
                    user='postgres', password='gilbert123'
                )
                conn.close()
                postgres_ready = True
            except:
                pass
            
            if hadoop_ready and postgres_ready:
                print("✅ All services ready!")
                return True
            
            print(f"⏳ Services not ready yet... (attempt {attempt + 1}/{max_attempts})")
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n❌ Interrupted by user")
            return False
    
    print("❌ Services did not become ready in time")
    return False

def start_pipeline():
    """Start the main pipeline"""
    print("🚀 Starting TB Data Pipeline...")
    
    try:
        # Run the main orchestrator
        result = subprocess.run([
            sys.executable, 'main_orchestrator.py', 
            '--mode', 'full',
            '--force-update'
        ])
        
        return result.returncode == 0
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        return False
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return False

def main():
    """Main startup sequence"""
    print_banner()
    
    # Step 1: Check requirements
    if not check_requirements():
        print("\n❌ Requirements check failed. Please install missing components.")
        return 1
    
    # Step 2: Install Python requirements
    try:
        if not install_python_requirements():
            print("\n❌ Failed to install Python requirements")
            return 1
    except KeyboardInterrupt:
        print("\n❌ Installation interrupted")
        return 1
    
    # Step 3: Start Docker services
    if not start_docker_services():
        print("\n❌ Failed to start Docker services")
        return 1
    
    # Step 4: Wait for services
    if not wait_for_services():
        print("\n❌ Services did not start properly")
        return 1
    
    # Step 5: Start pipeline
    print("\n" + "=" * 70)
    print("🎯 STARTING TB DATA PROCESSING PIPELINE")
    print("=" * 70)
    
    if start_pipeline():
        print("\n🎉 Pipeline completed successfully!")
        print("📊 API server should now be running at: http://localhost:5000")
        print("\nAvailable endpoints:")
        print("  • GET /api/map-data - Interactive map data")
        print("  • GET /api/trends/{iso3} - Country trends") 
        print("  • GET /api/comparison - Regional comparison")
        print("  • GET /api/yearly-trends - Regional yearly trends")
        print("  • GET /api/health - Health check")
        return 0
    else:
        print("\n❌ Pipeline failed")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(1)