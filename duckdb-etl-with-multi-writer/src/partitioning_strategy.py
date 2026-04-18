#!/usr/bin/env python3
"""
Partition Strategy Analyzer

Analyzes NYC Yellow Taxi data to recommend optimal partitioning strategies
for parallel loading with Registry Locking.

Usage:
    analyzer = PartitionAnalyzer(data_dir='NYC Yellow Taxi Record 23-24-25')
    recommendations = analyzer.analyze()
    print(recommendations)
"""

from pathlib import Path
from typing import Dict, List, Any
import json
from collections import defaultdict


class PartitionAnalyzer:
    """
    Analyzes parquet files to recommend partitioning strategies
    """
    
    def __init__(self, data_dir: str = '../NYC Yellow Taxi Record 23-24-25'):
        """
        Initialize partition analyzer
        
        Args:
            data_dir: Root directory of NYC taxi data
        """
        self.data_dir = Path(data_dir)
    
    def discover_partitions(self) -> Dict[str, List[Path]]:
        """
        Discover available parquet files organized by year and month
        
        Returns:
            Dict mapping 'year/month' to list of parquet file paths
        """
        partitions = defaultdict(list)
        
        if not self.data_dir.exists():
            print(f"⚠️  Data directory not found: {self.data_dir}")
            return partitions
        
        # Walk through year subdirectories
        for year_dir in sorted(self.data_dir.glob('*/'), key=lambda x: x.name):
            if year_dir.is_dir() and year_dir.name.isdigit():
                year = year_dir.name
                
                # Find parquet files
                parquet_files = list(year_dir.glob('*.parquet'))
                
                if parquet_files:
                    # Group by month if possible
                    # Expected format: yellow_tripdata_YYYY-MM.parquet
                    for pf in parquet_files:
                        month_key = year
                        if '-' in pf.stem:
                            parts = pf.stem.split('_')
                            if len(parts) >= 3:
                                month_key = f"{year}/{parts[-1]}"  # YYYY/MM
                        
                        partitions[month_key].append(pf)
        
        return partitions
    
    def analyze(self) -> Dict[str, Any]:
        """
        Analyze data structure and recommend partitioning
        
        Returns:
            Analysis dict with recommendations
        """
        partitions = self.discover_partitions()
        
        analysis = {
            'status': 'success' if partitions else 'no_data_found',
            'total_partitions': len(partitions),
            'partitions': {},
            'recommendations': {}
        }
        
        if not partitions:
            analysis['status'] = 'no_data_found'
            analysis['message'] = f"No parquet files found in {self.data_dir}"
            return analysis
        
        # Analyze each partition
        total_files = 0
        partition_sizes = []
        
        for partition_key in sorted(partitions.keys()):
            files = partitions[partition_key]
            total_files += len(files)
            
            # Try to get file sizes (in bytes)
            file_sizes = []
            for f in files:
                try:
                    file_sizes.append(f.stat().st_size)
                except:
                    pass
            
            partition_info = {
                'file_count': len(files),
                'file_names': [f.name for f in files],
                'total_size_bytes': sum(file_sizes) if file_sizes else 0,
                'total_size_gb': round(sum(file_sizes) / (1024**3), 2) if file_sizes else 0
            }
            
            analysis['partitions'][partition_key] = partition_info
            partition_sizes.append(partition_info['total_size_gb'])
        
        # Generate recommendations
        avg_partition_size = sum(partition_sizes) / len(partition_sizes) if partition_sizes else 0
        
        analysis['recommendations'] = {
            'total_files': total_files,
            'total_size_gb': round(sum(partition_sizes), 2),
            'average_partition_gb': round(avg_partition_size, 2),
            'partition_strategy': 'yearly' if len(partitions) <= 3 else 'monthly',
            'recommended_workers': min(len(partitions), 4),  # Cap at 4 workers
            'loading_order': sorted(partitions.keys()),
            'notes': [
                f"Found {total_files} parquet files across {len(partitions)} partition(s)",
                f"Total dataset size: ~{round(sum(partition_sizes), 1)} GB",
                f"Average partition: ~{round(avg_partition_size, 1)} GB",
                f"Recommended strategy: Load partitions {len(partitions)} at a time with registry locks"
            ]
        }
        
        return analysis
    
    def get_partition_globs(self) -> List[str]:
        """
        Get glob patterns for each partition (for use with Registry Locking)
        
        Returns:
            List of glob patterns ready for ETL loading
        """
        partitions = self.discover_partitions()
        globs = []
        
        for partition_key in sorted(partitions.keys()):
            if '/' in partition_key:
                # Monthly partition: NYC Yellow Taxi Record 23-24-25/2023/yellow_tripdata_2023-01.parquet
                year, month = partition_key.split('/')
                glob_pattern = f"NYC Yellow Taxi Record 23-24-25/{year}/*.parquet"
            else:
                # Yearly partition
                year = partition_key
                glob_pattern = f"NYC Yellow Taxi Record 23-24-25/{year}/*.parquet"
            
            if glob_pattern not in globs:
                globs.append(glob_pattern)
        
        return globs
    
    def estimate_load_time(
        self,
        throughput_rows_per_sec: int = 2800000
    ) -> Dict[str, Any]:
        """
        Estimate load time based on row count estimates
        
        Args:
            throughput_rows_per_sec: DuckDB's estimated throughput
        
        Returns:
            Timing estimates
        """
        analysis = self.analyze()
        
        # NYC Yellow Taxi 2023-2025 typically:
        # 2023: ~45M rows
        # 2024: ~50M rows
        # 2025: ~30M rows (partial)
        # Total: ~125M rows
        
        total_gb = analysis['recommendations']['total_size_gb']
        
        # Rough estimate: ~1M rows per GB
        estimated_rows = total_gb * 1_000_000
        
        estimate_sec = estimated_rows / throughput_rows_per_sec
        estimate_min = estimate_sec / 60
        
        return {
            'estimated_total_rows': int(estimated_rows),
            'throughput_rows_per_sec': throughput_rows_per_sec,
            'estimated_duration_sec': round(estimate_sec, 1),
            'estimated_duration_min': round(estimate_min, 1),
            'notes': 'Estimate assumes DuckDB at 2.8M rows/sec; actual may vary based on system'
        }


if __name__ == '__main__':
    # Example usage
    print("🔍 Partition Analysis for NYC Yellow Taxi Data\n")
    
    analyzer = PartitionAnalyzer()
    analysis = analyzer.analyze()
    
    print("📊 Partition Analysis Results:")
    print(json.dumps(analysis, indent=2))
    
    print("\n⏱️  Load Time Estimates:")
    timing = analyzer.estimate_load_time()
    print(json.dumps(timing, indent=2))
    
    print("\n🎯 Recommended Loading Globs:")
    globs = analyzer.get_partition_globs()
    for i, glob_pattern in enumerate(globs, 1):
        print(f"  {i}. {glob_pattern}")
