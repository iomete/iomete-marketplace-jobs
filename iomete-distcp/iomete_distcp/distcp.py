#!/usr/bin/env python3
"""
Distributed file copying tool using PySpark and PyArrow.
Supports copying files between different storage systems (S3, HDFS, GCS, etc.)
"""

import argparse
import logging
import os
from typing import List, Dict, Any
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pyarrow as pa
from pyarrow import fs


class DistCp:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger(__name__)
    
    def list_files(self, source_path: str) -> List[Dict[str, Any]]:
        """List all files in source path recursively"""
        filesystem, _ = fs.FileSystem.from_uri(source_path)
        parsed_uri = urlparse(source_path)
        path = parsed_uri.path or '/'
        
        files = []
        try:
            file_info = filesystem.get_file_info(fs.FileSelector(path, recursive=True))
            for info in file_info:
                if info.type == fs.FileType.File:
                    files.append({
                        'source_path': f"{parsed_uri.scheme}://{parsed_uri.netloc}{info.path}",
                        'size': info.size,
                        'path': info.path
                    })
        except Exception as e:
            self.logger.error(f"Failed to list files from {source_path}: {e}")
            raise
        
        self.logger.info(f"Found {len(files)} files to copy")
        return files
    
    @staticmethod
    def copy_file(source_uri: str, dest_uri: str, source_path: str) -> Dict[str, Any]:
        """Copy a single file from source to destination"""
        try:
            source_fs, source_base_path = fs.FileSystem.from_uri(source_uri)
            dest_fs, dest_base_path = fs.FileSystem.from_uri(dest_uri)

            # Read from source
            with source_fs.open_input_file(source_path) as source_file:
                data = source_file.read()

            # Calculate relative path from source base to actual file
            relative_path = os.path.relpath(source_path, source_base_path)
            
            # Create destination path preserving directory structure
            dest_path = os.path.join(dest_base_path, relative_path)

            # Ensure parent directories exist
            parent_dir = os.path.dirname(dest_path)
            if parent_dir:
                dest_fs.create_dir(parent_dir, recursive=True)

            with dest_fs.open_output_stream(dest_path) as dest_file:
                dest_file.write(data)

            return {
                'source_path': source_path,
                'dest_path': dest_path,
                'size': len(data),
                'status': 'success'
            }
        except Exception as e:
            return {
                'source_path': source_path,
                'dest_path': '',
                'size': 0,
                'status': 'failed',
                'error': str(e)
            }
    
    def run(self, source_path: str, dest_path: str) -> None:
        """Main method to run distributed file copy"""
        self.logger.info(f"Starting distcp from {source_path} to {dest_path}")
        
        # Driver: List and plan files
        files_to_copy = self.list_files(source_path)
        
        if not files_to_copy:
            self.logger.warning("No files found to copy")
            return
        
        # Create DataFrame for distributed processing
        files_df = self.spark.createDataFrame(files_to_copy)
        files_df = files_df.withColumn("dest_uri", lit(dest_path))
        files_df = files_df.withColumn("source_uri", lit(source_path))
        
        # Distributed copy using map partitions
        def copy_partition(iterator):
            results = []
            for row in iterator:
                result = DistCp.copy_file(
                    row['source_uri'], 
                    row['dest_uri'], 
                    row['path']
                )
                results.append(result)
            return results
        
        # Execute distributed copy
        copy_results = files_df.rdd.mapPartitions(copy_partition).collect()
        
        # Summary
        successful = sum(1 for r in copy_results if r['status'] == 'success')
        failed = sum(1 for r in copy_results if r['status'] == 'failed')
        total_size = sum(r['size'] for r in copy_results if r['status'] == 'success')
        
        self.logger.info(f"Copy completed: {successful} successful, {failed} failed, {total_size} bytes copied")
        
        if failed > 0:
            self.logger.error("Failed copies:")
            for result in copy_results:
                if result['status'] == 'failed':
                    self.logger.error(f"  {result['source_path']}: {result.get('error', 'Unknown error')}")


def main():
    parser = argparse.ArgumentParser(description='Distributed file copy tool')
    parser.add_argument('source', help='Source path (s3://bucket/path, hdfs://host/path, etc.)')
    parser.add_argument('dest', help='Destination path')
    parser.add_argument('--app-name', default='DistCp', help='Spark application name')
    parser.add_argument('--log-level', default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(args.app_name) \
        .getOrCreate()
    
    try:
        distcp = DistCp(spark)
        distcp.run(args.source, args.dest)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()