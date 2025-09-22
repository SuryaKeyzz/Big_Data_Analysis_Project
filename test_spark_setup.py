#!/usr/bin/env python3
"""
Test script to verify Spark and Hadoop setup on Windows
Run this before executing the main pipeline
"""

import os
import sys
from pathlib import Path

def test_environment_setup():
    """Test if all required environment variables and files are present"""
    print("Testing Spark/Hadoop setup for Windows...")
    print("=" * 50)
    
    # Check Java
    java_home = os.environ.get('JAVA_HOME')
    print(f"JAVA_HOME: {java_home}")
    if java_home and Path(java_home, 'bin', 'java.exe').exists():
        print("✓ Java found")
    else:
        print("✗ Java not found or JAVA_HOME not set correctly")
        return False
    
    # Check Hadoop
    hadoop_home = os.environ.get('HADOOP_HOME')
    print(f"HADOOP_HOME: {hadoop_home}")
    if hadoop_home and Path(hadoop_home).exists():
        print("✓ Hadoop directory found")
    else:
        print("✗ Hadoop not found or HADOOP_HOME not set correctly")
        return False
    
    # Check critical files
    winutils_path = Path(hadoop_home, 'bin', 'winutils.exe')
    hadoop_dll_path = Path(hadoop_home, 'bin', 'hadoop.dll')
    
    print(f"Checking winutils.exe: {winutils_path}")
    if winutils_path.exists():
        print("✓ winutils.exe found")
    else:
        print("✗ winutils.exe NOT found - download from https://github.com/steveloughran/winutils")
        print(f"   Should be placed at: {winutils_path}")
        return False
    
    print(f"Checking hadoop.dll: {hadoop_dll_path}")
    if hadoop_dll_path.exists():
        print("✓ hadoop.dll found")
    else:
        print("✗ hadoop.dll NOT found - download from https://github.com/steveloughran/winutils")
        print(f"   Should be placed at: {hadoop_dll_path}")
        return False
    
    # Check PATH
    path_env = os.environ.get('PATH', '')
    hadoop_bin = str(Path(hadoop_home, 'bin'))
    if hadoop_bin in path_env:
        print("✓ Hadoop bin directory is in PATH")
    else:
        print("✗ Hadoop bin directory not in PATH")
        print(f"   Add this to PATH: {hadoop_bin}")
        return False
    
    # Test directory creation
    test_dirs = [
        'F:/Big_Data_Project/tmp',
        'F:/Big_Data_Project/spark-warehouse',
        'data/processed'
    ]
    
    for test_dir in test_dirs:
        try:
            Path(test_dir).mkdir(parents=True, exist_ok=True)
            print(f"✓ Directory accessible: {test_dir}")
        except Exception as e:
            print(f"✗ Cannot create directory {test_dir}: {e}")
            return False
    
    print("=" * 50)
    print("✓ All checks passed! Environment should work with Spark.")
    return True

def test_spark_initialization():
    """Test if Spark can be initialized"""
    try:
        from pyspark.sql import SparkSession
        
        # Set environment variables
        os.environ['HADOOP_HOME'] = r'F:\hadoop'
        os.environ['JAVA_HOME'] = r'F:\Eclipse Adoptium\jdk-17'
        os.environ['PYSPARK_PYTHON'] = r'F:\Big_Data_Project\env\Scripts\python.exe'
        os.environ['PYSPARK_DRIVER_PYTHON'] = r'F:\Big_Data_Project\env\Scripts\python.exe'
        
        hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
        if hadoop_bin not in os.environ.get('PATH', ''):
            os.environ['PATH'] = hadoop_bin + os.pathsep + os.environ.get('PATH', '')
        
        print("Attempting to create Spark session...")
        
        spark = SparkSession.builder \
            .appName("Test_Spark_Setup") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.sql.warehouse.dir", "file:///F:/Big_Data_Project/spark-warehouse") \
            .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=F:/Big_Data_Project/tmp") \
            .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=F:/Big_Data_Project/tmp") \
            .config("spark.local.dir", "F:/Big_Data_Project/tmp") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        print(f"✓ Spark session created successfully! Version: {spark.version}")
        
        # Test basic DataFrame operations
        from pyspark.sql import Row
        test_data = [Row(id=1, name="test"), Row(id=2, name="test2")]
        df = spark.createDataFrame(test_data)
        
        print(f"✓ DataFrame creation successful. Rows: {df.count()}")
        
        # Test file write (the problematic operation)
        test_output_dir = Path("data/processed/test_output")
        test_output_dir.mkdir(parents=True, exist_ok=True)
        
        df.write.mode("overwrite").option("header", "true").csv(str(test_output_dir))
        print("✓ CSV write test successful")
        
        # Clean up
        spark.stop()
        print("✓ Spark session stopped cleanly")
        
        return True
        
    except Exception as e:
        print(f"✗ Spark test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("Running Spark/Hadoop setup verification...")
    
    if test_environment_setup():
        print("\nTesting Spark initialization...")
        if test_spark_initialization():
            print("\n🎉 All tests passed! Your setup should work.")
        else:
            print("\n❌ Spark initialization failed. Check the error above.")
    else:
        print("\n❌ Environment setup incomplete. Fix the issues above first.")