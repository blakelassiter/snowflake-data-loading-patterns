"""
dbt Trigger Utilities

Utilities for triggering dbt runs from various sources.
Used by webhook handlers and orchestration tools.
"""

import os
import time
from typing import Optional
from dataclasses import dataclass
from enum import Enum
import requests


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@dataclass
class DbtCloudConfig:
    """dbt Cloud configuration."""
    api_key: str
    account_id: str
    base_url: str = "https://cloud.getdbt.com/api/v2"
    
    @classmethod
    def from_env(cls) -> "DbtCloudConfig":
        """Load configuration from environment variables."""
        return cls(
            api_key=os.environ.get("DBT_CLOUD_API_KEY", ""),
            account_id=os.environ.get("DBT_CLOUD_ACCOUNT_ID", ""),
        )


class RunStatus(Enum):
    """dbt Cloud run statuses."""
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


# -----------------------------------------------------------------------------
# dbt Cloud Client
# -----------------------------------------------------------------------------

class DbtCloudClient:
    """Client for dbt Cloud API."""
    
    def __init__(self, config: Optional[DbtCloudConfig] = None):
        self.config = config or DbtCloudConfig.from_env()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {self.config.api_key}",
            "Content-Type": "application/json"
        })
    
    def trigger_job(
        self,
        job_id: str,
        cause: str = "API trigger",
        git_sha: Optional[str] = None,
        schema_override: Optional[str] = None,
        steps_override: Optional[list] = None,
    ) -> dict:
        """
        Trigger a dbt Cloud job.
        
        Args:
            job_id: dbt Cloud job ID
            cause: Description of why run was triggered
            git_sha: Optional specific git commit
            schema_override: Optional schema override
            steps_override: Optional custom steps
        
        Returns:
            dict with run_id and status
        """
        url = f"{self.config.base_url}/accounts/{self.config.account_id}/jobs/{job_id}/run/"
        
        payload = {
            "cause": cause,
        }
        
        if git_sha:
            payload["git_sha"] = git_sha
        if schema_override:
            payload["schema_override"] = schema_override
        if steps_override:
            payload["steps_override"] = steps_override
        
        response = self.session.post(url, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        run_data = data.get("data", {})
        
        return {
            "run_id": run_data.get("id"),
            "status": run_data.get("status"),
            "status_humanized": run_data.get("status_humanized"),
            "href": run_data.get("href"),
        }
    
    def get_run_status(self, run_id: int) -> dict:
        """Get status of a specific run."""
        url = f"{self.config.base_url}/accounts/{self.config.account_id}/runs/{run_id}/"
        
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        run_data = data.get("data", {})
        
        return {
            "run_id": run_data.get("id"),
            "status": run_data.get("status"),
            "status_humanized": run_data.get("status_humanized"),
            "finished_at": run_data.get("finished_at"),
            "duration": run_data.get("duration"),
        }
    
    def wait_for_run(
        self,
        run_id: int,
        timeout_seconds: int = 3600,
        poll_interval: int = 30,
    ) -> dict:
        """
        Wait for a run to complete.
        
        Args:
            run_id: dbt Cloud run ID
            timeout_seconds: Maximum wait time
            poll_interval: Seconds between status checks
        
        Returns:
            Final run status
        """
        start_time = time.time()
        
        while True:
            status = self.get_run_status(run_id)
            status_code = status.get("status")
            
            # Check if run is complete
            if status_code in [RunStatus.SUCCESS.value, RunStatus.ERROR.value, RunStatus.CANCELLED.value]:
                return status
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(f"Run {run_id} did not complete within {timeout_seconds} seconds")
            
            time.sleep(poll_interval)
    
    def trigger_and_wait(
        self,
        job_id: str,
        cause: str = "API trigger",
        timeout_seconds: int = 3600,
    ) -> dict:
        """
        Trigger a job and wait for completion.
        
        Returns:
            Final run status including success/failure
        """
        # Trigger
        trigger_result = self.trigger_job(job_id, cause)
        run_id = trigger_result.get("run_id")
        
        if not run_id:
            raise ValueError("Failed to get run_id from trigger response")
        
        # Wait
        final_status = self.wait_for_run(run_id, timeout_seconds)
        
        return {
            **final_status,
            "success": final_status.get("status") == RunStatus.SUCCESS.value,
        }
    
    def get_job(self, job_id: str) -> dict:
        """Get job details."""
        url = f"{self.config.base_url}/accounts/{self.config.account_id}/jobs/{job_id}/"
        
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        
        return response.json().get("data", {})
    
    def list_jobs(self, project_id: Optional[str] = None) -> list:
        """List all jobs, optionally filtered by project."""
        url = f"{self.config.base_url}/accounts/{self.config.account_id}/jobs/"
        
        params = {}
        if project_id:
            params["project_id"] = project_id
        
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response.json().get("data", [])


# -----------------------------------------------------------------------------
# Convenience functions
# -----------------------------------------------------------------------------

def trigger_dbt_job(
    job_id: str,
    cause: str = "API trigger",
    wait: bool = False,
    timeout_seconds: int = 3600,
) -> dict:
    """
    Trigger a dbt Cloud job.
    
    Convenience function that handles client setup.
    
    Args:
        job_id: dbt Cloud job ID
        cause: Description of trigger reason
        wait: If True, wait for job to complete
        timeout_seconds: Max wait time if waiting
    
    Returns:
        Run details including run_id
    """
    client = DbtCloudClient()
    
    if wait:
        return client.trigger_and_wait(job_id, cause, timeout_seconds)
    else:
        return client.trigger_job(job_id, cause)


def trigger_dbt_on_data_load(
    source_system: str,
    tables_loaded: list,
    job_mapping: Optional[dict] = None,
) -> list:
    """
    Trigger appropriate dbt job(s) based on what data was loaded.
    
    Args:
        source_system: Name of source system (e.g., "postgres_prod")
        tables_loaded: List of tables that were loaded
        job_mapping: Map of source systems to dbt job IDs
    
    Returns:
        List of triggered job results
    """
    # Default mapping - customize for your setup
    default_mapping = {
        "postgres_prod": os.environ.get("DBT_JOB_POSTGRES", ""),
        "salesforce": os.environ.get("DBT_JOB_SALESFORCE", ""),
        "stripe": os.environ.get("DBT_JOB_STRIPE", ""),
        "default": os.environ.get("DBT_JOB_DEFAULT", ""),
    }
    
    mapping = job_mapping or default_mapping
    job_id = mapping.get(source_system) or mapping.get("default")
    
    if not job_id:
        return [{"error": f"No dbt job mapped for {source_system}"}]
    
    result = trigger_dbt_job(
        job_id=job_id,
        cause=f"Data load: {source_system} - {len(tables_loaded)} tables"
    )
    
    return [result]


# -----------------------------------------------------------------------------
# CLI interface
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Trigger dbt Cloud jobs")
    parser.add_argument("job_id", help="dbt Cloud job ID")
    parser.add_argument("--cause", default="CLI trigger", help="Trigger cause")
    parser.add_argument("--wait", action="store_true", help="Wait for completion")
    parser.add_argument("--timeout", type=int, default=3600, help="Timeout in seconds")
    
    args = parser.parse_args()
    
    result = trigger_dbt_job(
        job_id=args.job_id,
        cause=args.cause,
        wait=args.wait,
        timeout_seconds=args.timeout,
    )
    
    print(f"Result: {result}")
