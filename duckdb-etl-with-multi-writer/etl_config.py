#!/usr/bin/env python3
"""
ETL Configuration Management

Provides preset configurations for different usage patterns:
- development: For local testing (2 workers, snappy compression)
- production: For high throughput (8 workers, snappy compression)
- fast: Maximum speed (8 workers, no compression)
- compact: Maximum compression (4 workers, gzip compression)

Usage:
    python etl_config.py development
    python etl_config.py fast
    python etl_config.py production
    python etl_config.py compact
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class ETLConfig:
    """ETL Configuration Manager"""
    
    PRESETS = {
        'development': {
            'raw_dir': 'NYC Yellow Taxi Record 23-24-25',
            'processed_dir': 'data/processed',
            'max_workers': 2,
            'compression': 'snappy',
            'batch_size': 1000,
            'enable_dedup': False,
            'registry_path': 'data_registry.json',
            'description': 'Local development (2 workers, snappy compression)'
        },
        'production': {
            'raw_dir': 'NYC Yellow Taxi Record 23-24-25',
            'processed_dir': 'data/processed',
            'max_workers': 8,
            'compression': 'snappy',
            'batch_size': 5000,
            'enable_dedup': True,
            'registry_path': 'data_registry.json',
            'description': 'Production (8 workers, snappy compression, dedup enabled)'
        },
        'fast': {
            'raw_dir': 'NYC Yellow Taxi Record 23-24-25',
            'processed_dir': 'data/processed',
            'max_workers': 8,
            'compression': 'uncompressed',
            'batch_size': 10000,
            'enable_dedup': False,
            'registry_path': 'data_registry.json',
            'description': 'Maximum speed (8 workers, no compression)'
        },
        'compact': {
            'raw_dir': 'NYC Yellow Taxi Record 23-24-25',
            'processed_dir': 'data/processed',
            'max_workers': 4,
            'compression': 'gzip',
            'batch_size': 2000,
            'enable_dedup': True,
            'registry_path': 'data_registry.json',
            'description': 'Maximum compression (4 workers, gzip compression)'
        }
    }
    
    def __init__(self, config_file: str = 'etl_config.json'):
        """Initialize configuration manager"""
        self.config_file = Path(config_file)
        self.config = {}
    
    def load_preset(self, preset_name: str) -> Dict[str, Any]:
        """
        Load a configuration preset
        
        Args:
            preset_name: Name of preset (development, production, fast, compact)
        
        Returns:
            Configuration dictionary
        """
        if preset_name not in self.PRESETS:
            raise ValueError(f"Unknown preset: {preset_name}. Available: {list(self.PRESETS.keys())}")
        
        preset = self.PRESETS[preset_name].copy()
        self.config = preset
        return preset
    
    def save_to_file(self) -> None:
        """Save current configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
        logger.info(f"✅ Config saved to {self.config_file}")
    
    def load_from_file(self) -> Dict[str, Any]:
        """Load configuration from file"""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                self.config = json.load(f)
            return self.config
        return {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set configuration value"""
        self.config[key] = value
    
    @staticmethod
    def show_presets() -> None:
        """Display all available presets"""
        logger.info("")
        logger.info("Available presets:")
        for name, preset in ETLConfig.PRESETS.items():
            logger.info(f"  {name:<12} - {preset['description']}")
    
    def show_current(self) -> None:
        """Display current configuration"""
        logger.info("")
        logger.info("Configuration:")
        for key, value in self.config.items():
            if key != 'description':
                logger.info(f"  {key:<20} = {value}")
    
    @staticmethod
    def get_benchmark_info(preset_name: str) -> Dict[str, Any]:
        """Get performance benchmarks for a preset"""
        benchmarks = {
            'development': {
                'est_throughput_mb_sec': 50,
                'est_total_time_min': 45,
                'est_cost': 'Low (local testing)'
            },
            'production': {
                'est_throughput_mb_sec': 200,
                'est_total_time_min': 12,
                'est_cost': 'Medium (production)'
            },
            'fast': {
                'est_throughput_mb_sec': 300,
                'est_total_time_min': 8,
                'est_cost': 'High (no compression)'
            },
            'compact': {
                'est_throughput_mb_sec': 80,
                'est_total_time_min': 60,
                'est_cost': 'Low (heavy compression)'
            }
        }
        return benchmarks.get(preset_name, {})


# Export PRESETS at module level for easier access
PRESETS = ETLConfig.PRESETS


def main():
    """Main entry point"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("NYC Taxi ETL Pipeline - Configuration Runner")
    logger.info("=" * 80)
    
    # Show available presets
    ETLConfig.show_presets()
    
    # Get preset from command line
    if len(sys.argv) > 1:
        preset_name = sys.argv[1]
    else:
        logger.info("\nUsage: python etl_config.py <preset>")
        logger.info("       python etl_config.py development|production|fast|compact")
        return
    
    # Load and save configuration
    try:
        config = ETLConfig()
        preset = config.load_preset(preset_name)
        
        logger.info(f"\n✅ Using '{preset_name}' preset")
        logger.info("")
        logger.info("-" * 80)
        config.show_current()
        logger.info("-" * 80)
        
        # Save to file
        config.save_to_file()
        
        # Show benchmarks
        bench = ETLConfig.get_benchmark_info(preset_name)
        if bench:
            logger.info("")
            logger.info("📊 Performance Estimates:")
            logger.info(f"   Throughput:    ~{bench['est_throughput_mb_sec']} MB/sec")
            logger.info(f"   Total time:    ~{bench['est_total_time_min']} minutes")
            logger.info(f"   Cost profile:  {bench['est_cost']}")
        
        logger.info("\n🚀 Starting ETL pipeline...")
        logger.info("")
        
        # Import and run the ETL
        try:
            from src.unified_etl_pipeline import UnifiedETLPipeline
            import asyncio
            
            pipeline = UnifiedETLPipeline(
                mode='etl',
                db_path=config.get('db_path', 'nyc_yellow_taxi.duckdb'),
                data_dir=config.get('raw_dir'),
                pipeline_id=f'taxi_etl_{preset_name}'
            )
            
            # Load data
            result = pipeline.run(years=[2023, 2024, 2025])
            
            logger.info("=" * 80)
            logger.info("✅ ETL completed successfully")
            logger.info("=" * 80)
        
        except Exception as e:
            logger.error(f"Error running ETL: {e}")
    
    except ValueError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
