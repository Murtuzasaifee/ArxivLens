import logging

from src.pipelines.ingestion.shared import run_index_papers_hybrid, run_verify_hybrid_index

logger = logging.getLogger(__name__)


def index_papers_hybrid(**context):
    """Index papers with chunking and vector embeddings for hybrid search.

    This task:
    1. Fetches recently processed papers from PostgreSQL
    2. Chunks them into overlapping segments (600 words, 100 overlap)
    3. Generates embeddings using Jina AI
    4. Indexes chunks with embeddings into OpenSearch
    """
    try:
        ti = context.get("ti")
        fetch_results = None
        if ti:
            fetch_results = ti.xcom_pull(task_ids="fetch_daily_papers", key="fetch_results")

        stats = run_index_papers_hybrid(fetch_results)

        if ti:
            ti.xcom_push(key="hybrid_index_stats", value=stats)

        return stats

    except Exception as e:
        logger.error(f"Failed to index papers for hybrid search: {e}")
        raise


def verify_hybrid_index(**context):
    """Verify hybrid index health and get statistics."""
    try:
        return run_verify_hybrid_index()
    except Exception as e:
        logger.error(f"Failed to verify hybrid index: {e}")
        raise
