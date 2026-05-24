import logging
from datetime import datetime, timedelta

from src.pipelines.ingestion.shared import run_fetch_daily_papers

logger = logging.getLogger(__name__)


def fetch_daily_papers(**context):
    """Fetch daily papers from arXiv and store in PostgreSQL.

    This task:
    1. Determines the target date (defaults to yesterday)
    2. Fetches papers from arXiv API
    3. Downloads and processes PDFs using Docling
    4. Stores metadata and parsed content in PostgreSQL

    Note: OpenSearch indexing is handled by a separate dedicated task
    """
    logger.info("Starting daily paper fetching task")

    execution_date = context.get("execution_date")
    if execution_date:
        target_dt = execution_date - timedelta(days=1)
        target_date = target_dt.strftime("%Y%m%d")
    else:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    results = run_fetch_daily_papers(target_date=target_date)

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="fetch_results", value=results)

    return results
