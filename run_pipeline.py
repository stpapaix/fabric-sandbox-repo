#!/usr/bin/env python3
"""
MS Fabric Sandbox Data Pipeline Orchestrator

This script orchestrates the execution of the data pipeline notebooks
in the correct order with proper error handling and logging.
"""

import json
import logging
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """Orchestrates the execution of the MS Fabric data pipeline notebooks."""
    
    def __init__(self, config_path: str = "config/pipeline_config.json"):
        """
        Initialize the orchestrator.
        
        Args:
            config_path (str): Path to the pipeline configuration file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.execution_results = {}
        
    def _load_config(self) -> Dict:
        """Load pipeline configuration from JSON file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file: {e}")
            raise
    
    def _execute_notebook(self, notebook_path: str, notebook_name: str) -> bool:
        """
        Execute a Jupyter notebook.
        
        Args:
            notebook_path (str): Path to the notebook file
            notebook_name (str): Name of the notebook for logging
            
        Returns:
            bool: True if execution successful, False otherwise
        """
        start_time = datetime.now()
        logger.info(f"Starting execution of {notebook_name}")
        
        try:
            # Use nbconvert to execute the notebook
            cmd = [
                "jupyter", "nbconvert", 
                "--execute", 
                "--to", "notebook",
                "--inplace",
                notebook_path
            ]
            
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=self._get_timeout(notebook_name)
            )
            
            execution_time = datetime.now() - start_time
            
            if result.returncode == 0:
                logger.info(f"✅ {notebook_name} completed successfully in {execution_time}")
                self.execution_results[notebook_name] = {
                    "status": "success",
                    "execution_time": str(execution_time),
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.now().isoformat()
                }
                return True
            else:
                logger.error(f"❌ {notebook_name} failed with return code {result.returncode}")
                logger.error(f"Error output: {result.stderr}")
                self.execution_results[notebook_name] = {
                    "status": "failed",
                    "execution_time": str(execution_time),
                    "error": result.stderr,
                    "start_time": start_time.isoformat(),
                    "end_time": datetime.now().isoformat()
                }
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {notebook_name} timed out")
            self.execution_results[notebook_name] = {
                "status": "timeout",
                "execution_time": str(datetime.now() - start_time),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }
            return False
        except Exception as e:
            logger.error(f"❌ {notebook_name} failed with exception: {e}")
            self.execution_results[notebook_name] = {
                "status": "error",
                "execution_time": str(datetime.now() - start_time),
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat()
            }
            return False
    
    def _get_timeout(self, notebook_name: str) -> int:
        """Get timeout for a specific notebook from configuration."""
        notebook_configs = self.config.get("notebooks", {})
        
        # Map notebook names to config keys
        notebook_mapping = {
            "01_data_ingestion": "01_data_ingestion",
            "02_data_transformation": "02_data_transformation", 
            "03_data_aggregation": "03_data_aggregation"
        }
        
        config_key = None
        for key in notebook_mapping:
            if key in notebook_name:
                config_key = notebook_mapping[key]
                break
        
        if config_key and config_key in notebook_configs:
            return notebook_configs[config_key].get("timeout_minutes", 120) * 60
        
        return 120 * 60  # Default 2 hours
    
    def _retry_notebook(self, notebook_path: str, notebook_name: str, max_retries: int = 3) -> bool:
        """
        Execute notebook with retry logic.
        
        Args:
            notebook_path (str): Path to the notebook
            notebook_name (str): Name of the notebook
            max_retries (int): Maximum number of retries
            
        Returns:
            bool: True if successful, False if all retries failed
        """
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"Retry attempt {attempt} for {notebook_name}")
            
            if self._execute_notebook(notebook_path, notebook_name):
                return True
            
            if attempt < max_retries:
                logger.info(f"Waiting before retry {attempt + 1}...")
                # Add exponential backoff if needed
                import time
                time.sleep(30 * (attempt + 1))
        
        logger.error(f"All retry attempts failed for {notebook_name}")
        return False
    
    def run_pipeline(self, notebooks: Optional[List[str]] = None) -> bool:
        """
        Run the complete data pipeline.
        
        Args:
            notebooks (List[str], optional): Specific notebooks to run. If None, runs all.
            
        Returns:
            bool: True if all notebooks executed successfully
        """
        pipeline_start = datetime.now()
        logger.info(f"🚀 Starting MS Fabric Sandbox Data Pipeline at {pipeline_start}")
        
        # Default notebook execution order
        default_notebooks = [
            ("notebooks/01_data_ingestion.ipynb", "01_data_ingestion"),
            ("notebooks/02_data_transformation.ipynb", "02_data_transformation"),
            ("notebooks/03_data_aggregation.ipynb", "03_data_aggregation")
        ]
        
        notebooks_to_run = default_notebooks
        if notebooks:
            notebooks_to_run = [(nb, Path(nb).stem) for nb in notebooks]
        
        all_successful = True
        
        for notebook_path, notebook_name in notebooks_to_run:
            if not Path(notebook_path).exists():
                logger.error(f"Notebook not found: {notebook_path}")
                all_successful = False
                continue
            
            # Get retry count from config
            notebook_config = self.config.get("notebooks", {}).get(notebook_name, {})
            max_retries = notebook_config.get("retry_count", 3)
            
            success = self._retry_notebook(notebook_path, notebook_name, max_retries)
            
            if not success:
                logger.error(f"Pipeline stopped due to failure in {notebook_name}")
                all_successful = False
                break
        
        pipeline_end = datetime.now()
        total_time = pipeline_end - pipeline_start
        
        if all_successful:
            logger.info(f"🎉 Pipeline completed successfully in {total_time}")
        else:
            logger.error(f"💥 Pipeline failed after {total_time}")
        
        # Save execution results
        self._save_execution_results(pipeline_start, pipeline_end, all_successful)
        
        return all_successful
    
    def _save_execution_results(self, start_time: datetime, end_time: datetime, success: bool):
        """Save pipeline execution results to a JSON file."""
        results = {
            "pipeline_run": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "total_duration": str(end_time - start_time),
                "success": success,
                "pipeline_version": self.config.get("pipeline", {}).get("version", "unknown")
            },
            "notebook_results": self.execution_results
        }
        
        results_file = f"pipeline_results_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            logger.info(f"Execution results saved to {results_file}")
        except Exception as e:
            logger.error(f"Failed to save execution results: {e}")

def main():
    """Main entry point for the pipeline orchestrator."""
    orchestrator = PipelineOrchestrator()
    
    # Parse command line arguments for specific notebooks if needed
    import argparse
    parser = argparse.ArgumentParser(description="MS Fabric Sandbox Data Pipeline Orchestrator")
    parser.add_argument(
        "--notebooks", 
        nargs="+", 
        help="Specific notebooks to run (default: all)"
    )
    parser.add_argument(
        "--config",
        default="config/pipeline_config.json",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    
    if args.config != "config/pipeline_config.json":
        orchestrator = PipelineOrchestrator(args.config)
    
    success = orchestrator.run_pipeline(args.notebooks)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()