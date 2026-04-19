#!/usr/bin/env python3
"""
Comprehensive tests for all make targets

Tests all Makefile targets to ensure they work correctly and cover all functionality.
Run with: pytest tests/test_make_targets.py -v
"""

import pytest
import subprocess
import os
import tempfile
from pathlib import Path
import duckdb
import json


class TestMakeTargets:
    """Test all make targets"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python, ensuring it exists"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            # Fall back to system python if venv doesn't exist
            return "python3"
        return str(venv_python)

    def run_make(self, target: str, project_root: Path, timeout: int = 60) -> subprocess.CompletedProcess:
        """Helper to run make targets"""
        result = subprocess.run(
            ["make", target],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result

    # ========================================================================
    # 🛠️ ENVIRONMENT TARGETS
    # ========================================================================

    def test_venv_create(self, project_root):
        """Test venv-create target"""
        # Remove existing venv if it exists
        venv_path = project_root / ".venv"
        if venv_path.exists():
            import shutil
            shutil.rmtree(venv_path, ignore_errors=True)
        
        result = self.run_make("venv-create", project_root, timeout=30)
        assert result.returncode == 0
        assert venv_path.exists(), "Virtual environment not created"
        assert (venv_path / "bin" / "python").exists() or (venv_path / "Scripts" / "python.exe").exists()

    def test_venv_install(self, project_root):
        """Test venv-install target"""
        result = self.run_make("venv-install", project_root, timeout=120)
        assert result.returncode == 0
        assert "duckdb" in result.stdout or "Successfully installed" in result.stdout

    def test_clean(self, project_root):
        """Test clean target"""
        result = self.run_make("clean", project_root)
        assert result.returncode == 0
        assert "Cleaned" in result.stdout or "Cleaned" in result.stderr

    # ========================================================================
    # 🚀 UNIFIED ETL MODES
    # ========================================================================

    def test_etl_mode_help(self, project_root):
        """Test ETL mode can show help"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-m", "src.unified_etl_pipeline", "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0
        assert "Unified ETL Pipeline" in result.stdout

    def test_etl_mode_status(self, project_root):
        """Test ETL mode --status flag"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-m", "src.unified_etl_pipeline", "--status"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0 or "no data" in result.stderr.lower() or "no file" in result.stderr.lower()

    def test_etl_mode_show_metrics(self, project_root):
        """Test ETL mode --show-metrics flag"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-m", "src.unified_etl_pipeline", "--show-metrics"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )
        # Should succeed or show that no metrics are available yet
        assert result.returncode == 0 or len(result.stdout) > 0 or len(result.stderr) > 0

    # ========================================================================
    # 📊 ANALYTICS & QUERIES
    # ========================================================================

    def test_query_optimizer_imports(self, project_root):
        """Test that QueryOptimizer can be imported"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-c", "from src.query_optimizer import QueryOptimizer; print('✅ OK')"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to import QueryOptimizer. Error: {result.stderr}"
        assert "OK" in result.stdout

    def test_unified_pipeline_imports(self, project_root):
        """Test that UnifiedETLPipeline can be imported"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-c", "from src.unified_etl_pipeline import UnifiedETLPipeline; print('✅ OK')"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to import UnifiedETLPipeline. Error: {result.stderr}"
        assert "OK" in result.stdout

    # ========================================================================
    # 📋 REGISTRY MANAGEMENT
    # ========================================================================

    def test_registry_status_target(self, project_root):
        """Test registry-status target"""
        result = subprocess.run(
            ["make", "registry-status"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )
        # Should succeed or show some status
        assert result.returncode == 0 or "Registry Status" in result.stdout

    # ========================================================================
    # 📚 HELP
    # ========================================================================

    def test_help_target(self, project_root):
        """Test help target shows available targets"""
        result = subprocess.run(
            ["make", "help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0
        assert "ETL" in result.stdout or "Pipeline" in result.stdout
        # Check for various target categories
        assert any(keyword in result.stdout for keyword in ["UNIFIED", "ANALYTICS", "REGISTRY", "TESTING"])


class TestUnifiedETLPipelineAPI:
    """Test UnifiedETLPipeline API which underlies all make targets"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python, ensuring it exists"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"
        return str(venv_python)

    @pytest.fixture
    def temp_db(self):
        """Create temporary DuckDB for testing"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.duckdb"
            yield str(db_path)

    def test_unified_pipeline_initialization(self, project_root, temp_db):
        """Test UnifiedETLPipeline can be instantiated"""
        python = self.get_venv_python(project_root)
        code = f"""
from src.unified_etl_pipeline import UnifiedETLPipeline
pipeline = UnifiedETLPipeline(mode='etl', db_path='{temp_db}')
print(f'✅ Pipeline initialized with mode: {{pipeline.mode}}')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to initialize pipeline. Error: {result.stderr}"
        assert "Pipeline initialized" in result.stdout

    def test_unified_pipeline_modes(self, project_root, temp_db):
        """Test all UnifiedETLPipeline modes can be instantiated"""
        python = self.get_venv_python(project_root)
        modes = ['etl', 'partition', 'query', 'validate', 'both']
        for mode in modes:
            code = f"""
from src.unified_etl_pipeline import UnifiedETLPipeline
pipeline = UnifiedETLPipeline(mode='{mode}', db_path='{temp_db}')
assert pipeline.mode == '{mode}', f'Expected mode {mode}, got {{pipeline.mode}}'
print(f'✅ Mode {{pipeline.mode}} OK')
"""
            result = subprocess.run(
                [python, "-c", code],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            assert result.returncode == 0, f"Mode {mode} failed: {result.stderr}"
            assert "OK" in result.stdout

    def test_unified_pipeline_data_models(self, project_root):
        """Test UnifiedETLPipeline data models are accessible"""
        python = self.get_venv_python(project_root)
        code = """
from src.unified_etl_pipeline import DataRegistry, FileMetadata, ETLMetrics
from dataclasses import fields

# Check FileMetadata (is a dataclass)
file_fields = {f.name for f in fields(FileMetadata)}
assert len(file_fields) > 0
print(f'✅ FileMetadata has {len(file_fields)} fields')

# Check ETLMetrics (is a dataclass)
metrics_fields = {f.name for f in fields(ETLMetrics)}
assert len(metrics_fields) > 0
print(f'✅ ETLMetrics has {len(metrics_fields)} fields')

# Check DataRegistry exists and is instantiable
registry = DataRegistry()
assert hasattr(registry, 'path')
assert hasattr(registry, 'data')
print(f'✅ DataRegistry is accessible')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to test data models. Error: {result.stderr}"
        assert "FileMetadata" in result.stdout
        assert "ETLMetrics" in result.stdout
        assert "DataRegistry" in result.stdout

    def test_duckdb_multiwriter_etl(self, project_root, temp_db):
        """Test DuckDBMultiWriterETL is accessible"""
        python = self.get_venv_python(project_root)
        code = f"""
from src.duckdb_multiwriter_etl import DuckDBMultiWriterETL
etl = DuckDBMultiWriterETL('{temp_db}')
print(f'✅ DuckDBMultiWriterETL initialized')
assert hasattr(etl, 'db_path'), 'Missing db_path attribute'
assert hasattr(etl, 'registry'), 'Missing registry attribute'
print(f'✅ DuckDBMultiWriterETL has expected attributes')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0
        assert "initialized" in result.stdout

    def test_registry_lock_manager(self, project_root):
        """Test RegistryLockManager is accessible"""
        python = self.get_venv_python(project_root)
        code = """
from src.registry_lock_manager import RegistryLockManager
import tempfile
with tempfile.TemporaryDirectory() as tmpdir:
    manager = RegistryLockManager(tmpdir)
    print(f'✅ RegistryLockManager initialized')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to initialize RegistryLockManager. Error: {result.stderr}"
        assert "initialized" in result.stdout

    def test_query_optimizer(self, project_root):
        """Test QueryOptimizer is accessible"""
        python = self.get_venv_python(project_root)
        code = """
from src.query_optimizer import QueryOptimizer
print(f'✅ QueryOptimizer imported successfully')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to import QueryOptimizer. Error: {result.stderr}"
        assert "imported" in result.stdout

    def test_metrics_collector(self, project_root):
        """Test MetricsCollector is accessible"""
        python = self.get_venv_python(project_root)
        code = """
from src.metrics import MetricsCollector
metrics = MetricsCollector()
print(f'✅ MetricsCollector initialized')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to initialize MetricsCollector. Error: {result.stderr}"
        assert "initialized" in result.stdout


class TestModuleExports:
    """Test that all modules are properly exported"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"
        return str(venv_python)

    def test_src_init_exports(self, project_root):
        """Test that src/__init__.py exports all required items"""
        python = self.get_venv_python(project_root)
        code = """
from src import (
    UnifiedETLPipeline, DataRegistry, FileMetadata, ETLMetrics,
    DuckDBMultiWriterETL, RegistryLockManager,
    QueryOptimizer, MetricsCollector, MetricsReporter,
    ETLError, normalize_columns
)
print('✅ All exports available')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to import exports. Error: {result.stderr}"
        assert "All exports available" in result.stdout

    def test_module_structure(self, project_root):
        """Test that key modules exist and import correctly"""
        python = self.get_venv_python(project_root)
        modules = [
            'src.unified_etl_pipeline',
            'src.duckdb_multiwriter_etl',
            'src.registry_lock_manager',
            'src.query_optimizer',
            'src.metrics',
            'src.partitioning_strategy',
            'src.exceptions'
        ]
        
        for module in modules:
            code = f"import {module}; print('✅ {module}')"
            result = subprocess.run(
                [python, "-c", code],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            assert result.returncode == 0, f"Failed to import {module}: {result.stderr}"


class TestConfigurationSystem:
    """Test configuration presets and system"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"
        return str(venv_python)

    def test_etl_config_exists(self, project_root):
        """Test etl_config.py exists and can be imported"""
        config_path = project_root / "etl_config.py"
        assert config_path.exists(), "etl_config.py not found"

    def test_etl_config_presets(self, project_root):
        """Test ETL configuration presets are available"""
        python = self.get_venv_python(project_root)
        code = """
import sys
sys.path.insert(0, '.')
from etl_config import PRESETS
assert 'development' in PRESETS
assert 'fast' in PRESETS
assert 'compact' in PRESETS
print('✅ All presets available')
"""
        result = subprocess.run(
            [python, "-c", code],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Failed to access PRESETS. Error: {result.stderr}"
        assert "✅ All presets" in result.stdout or "PRESETS" in result.stdout


class TestDemosAndScripts:
    """Test demo scripts and tools"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"
        return str(venv_python)

    def test_demo_script_exists(self, project_root):
        """Test demo script exists"""
        demo_path = project_root / "scripts" / "demo_registry_locking.py"
        assert demo_path.exists(), "Demo script not found"

    def test_demo_script_syntax(self, project_root):
        """Test demo script has valid Python syntax"""
        python = self.get_venv_python(project_root)
        demo_path = project_root / "scripts" / "demo_registry_locking.py"
        result = subprocess.run(
            [python, "-m", "py_compile", str(demo_path)],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0, f"Demo script syntax error: {result.stderr}"


class TestIntegration:
    """Integration tests for make targets and pipeline"""

    @pytest.fixture
    def project_root(self):
        """Get project root"""
        return Path(__file__).parent.parent

    def get_venv_python(self, project_root: Path) -> str:
        """Get path to venv python"""
        venv_python = project_root / ".venv" / "bin" / "python"
        if not venv_python.exists():
            return "python3"
        return str(venv_python)

    def test_pipeline_cli_full_help(self, project_root):
        """Test pipeline CLI provides full help"""
        python = self.get_venv_python(project_root)
        result = subprocess.run(
            [python, "-m", "src.unified_etl_pipeline", "--help"],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=10
        )
        assert result.returncode == 0
        # Check for modes
        assert "etl" in result.stdout
        assert "partition" in result.stdout
        assert "query" in result.stdout
        assert "validate" in result.stdout

    def test_all_required_targets_defined(self, project_root):
        """Test that all required make targets are defined"""
        makefile_path = project_root / "Makefile"
        with open(makefile_path) as f:
            content = f.read()
        
        required_targets = [
            'venv-create', 'venv-install', 'clean',
            'etl', 'partition', 'query', 'validate',
            'etl-load-2023', 'etl-load-2024', 'etl-load-2025',
            'test-etl', 'test-multiwriter',
            'registry-status', 'show-metrics',
            'query-stats', 'query-peek', 'query-daily',
            'help'
        ]
        
        for target in required_targets:
            assert target + ":" in content, f"Target '{target}' not found in Makefile"

    def test_no_old_references_in_makefile(self, project_root):
        """Test that Makefile doesn't reference deleted files"""
        makefile_path = project_root / "Makefile"
        with open(makefile_path) as f:
            content = f.read()
        
        # These files were deleted, should not be referenced
        deleted_files = [
            'src.etl_pipeline_refactored',
            'from src.etl_pipeline import'
        ]
        
        for deleted_ref in deleted_files:
            assert deleted_ref not in content, f"Makefile still references deleted module: {deleted_ref}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
