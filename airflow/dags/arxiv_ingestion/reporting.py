import logging
from datetime import datetime

from src.pipelines.ingestion.shared import run_generate_daily_report

logger = logging.getLogger(__name__)


def generate_daily_report(**context):
    """Generate a daily report of the ingestion pipeline results.

    Collects statistics from all previous tasks and generates a summary report.
    """
    logger.info("Generating daily ingestion report")

    ti = context.get("ti")
    if not ti:
        logger.warning("No task instance available, generating basic report")
        return {"status": "basic_report", "message": "No task instance for XCom data"}

    fetch_stats = ti.xcom_pull(task_ids="fetch_daily_papers", key="fetch_results") or {}
    hybrid_stats = ti.xcom_pull(task_ids="index_papers_hybrid", key="hybrid_index_stats") or {}

    # Robust execution date parsing (Airflow 2.x logical_date compatibility)
    execution_date_val = context.get("execution_date") or context.get("logical_date") or datetime.now()

    report = run_generate_daily_report(fetch_stats=fetch_stats, hybrid_stats=hybrid_stats, execution_date_val=execution_date_val)

    ti.xcom_push(key="daily_report", value=report)

    return report
