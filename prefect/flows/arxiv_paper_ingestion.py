"""
Prefect 3 flow — ArxivLens daily ingestion pipeline.
Mirrors the Airflow DAG exactly: same 5 steps, same Mon-Fri 06:00 UTC schedule.
"""

import os
import sys
from datetime import datetime, timedelta

# Ensure python paths are correct if executing directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from prefect import flow, task
from prefect.logging import get_run_logger
from src.pipelines.ingestion.shared import (
    run_cleanup_temp_files,
    run_fetch_daily_papers,
    run_generate_daily_report,
    run_index_papers_hybrid,
    run_setup_environment,
)


@task(name="setup-environment", retries=2, retry_delay_seconds=60)
def setup_environment_task():
    logger = get_run_logger()
    logger.info("Setting up environment for arXiv paper ingestion")
    return run_setup_environment()


@task(name="fetch-daily-papers", retries=2, retry_delay_seconds=1800)
def fetch_daily_papers_task():
    logger = get_run_logger()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"Fetching papers for date: {yesterday}")
    return run_fetch_daily_papers(target_date=yesterday)


@task(name="index-papers-hybrid", retries=1, retry_delay_seconds=300)
def index_papers_hybrid_task(fetch_results: dict):
    logger = get_run_logger()
    logger.info(f"Indexing papers: {fetch_results.get('papers_stored', 0)} stored")
    return run_index_papers_hybrid(fetch_results)


@task(name="generate-daily-report")
def generate_daily_report_task(fetch_stats: dict, hybrid_stats: dict):
    logger = get_run_logger()
    logger.info("Generating daily report")
    return run_generate_daily_report(fetch_stats, hybrid_stats)


@task(name="cleanup-temp-files")
def cleanup_temp_files_task():
    logger = get_run_logger()
    logger.info("Cleaning up temp files")
    run_cleanup_temp_files()


@flow(
    name="arxiv-paper-ingestion",
    description="Daily arXiv CS.AI pipeline: fetch → store to PostgreSQL → chunk & embed → hybrid OpenSearch indexing",
    log_prints=True,
)
def arxiv_ingestion_flow():
    setup_environment_task()
    fetch_results = fetch_daily_papers_task()
    hybrid_stats = index_papers_hybrid_task(fetch_results)
    generate_daily_report_task(fetch_results, hybrid_stats)
    cleanup_temp_files_task()


if __name__ == "__main__":
    # .serve() registers a deployment on the Prefect server and blocks,
    # listening for scheduled + manual runs.
    schedule_cron = os.getenv("PREFECT__SCHEDULE", "0 6 * * 1-5")
    logger = get_run_logger() if sys.modules.get("prefect") else logging.getLogger(__name__)

    # Simple direct running if serving or run directly
    print(f"Starting Prefect deployment serve with cron schedule: {schedule_cron}")
    arxiv_ingestion_flow.serve(
        name="arxiv-ingestion-daily",
        cron=schedule_cron,
    )
