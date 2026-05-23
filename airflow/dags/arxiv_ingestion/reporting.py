import json
import logging
from datetime import datetime

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
    if hasattr(execution_date_val, "isoformat"):
        execution_date_str = execution_date_val.isoformat()
    else:
        execution_date_str = str(execution_date_val)

    report = {
        "execution_date": execution_date_str,
        "fetch_statistics": {
            "papers_fetched": fetch_stats.get("papers_fetched", 0),
            "papers_stored": fetch_stats.get("papers_stored", 0),
            "target_date": fetch_stats.get("date", "unknown"),
        },
        "indexing_statistics": {
            "papers_processed": hybrid_stats.get("papers_processed", 0),
            "chunks_created": hybrid_stats.get("total_chunks_created", 0),
            "chunks_indexed": hybrid_stats.get("total_chunks_indexed", 0),
            "embeddings_generated": hybrid_stats.get("total_embeddings_generated", 0),
        },
        "pipeline_status": "success" if fetch_stats and hybrid_stats else "partial",
    }

    try:
        # Import factories inside task to avoid importing heavy dependencies (docling, torch)
        # during DAG parsing by the Airflow scheduler.
        from src.db.factory import make_database
        from src.services.opensearch.factory import make_opensearch_client

        database = make_database()
        opensearch_client = make_opensearch_client()

        with database.get_session() as session:
            from sqlalchemy import func
            from src.models.paper import Paper

            total_papers = session.query(func.count(Paper.id)).scalar()
            report["database_statistics"] = {"total_papers": total_papers}

        if opensearch_client.health_check():
            try:
                stats_response = opensearch_client.client.indices.stats(index=opensearch_client.index_name)

                count_response = opensearch_client.client.count(index=opensearch_client.index_name)

                index_stats = stats_response["indices"][opensearch_client.index_name]["total"]

                report["opensearch_statistics"] = {
                    "index_name": opensearch_client.index_name,
                    "document_count": count_response["count"],
                    "index_size_mb": round(index_stats["store"]["size_in_bytes"] / (1024 * 1024), 2),
                }
            except Exception as stats_error:
                logger.error(f"Failed to get OpenSearch statistics: {stats_error}")
                report["opensearch_statistics"] = {"index_name": opensearch_client.index_name, "error": str(stats_error)}
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        report["error"] = str(e)

    # Safe json serialization helper
    def default_serializer(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    logger.info("Daily Ingestion Report:")
    logger.info(json.dumps(report, indent=2, default=default_serializer))

    ti.xcom_push(key="daily_report", value=report)

    return report
