"""
Prefect 3 flow — ArxivLens daily ingestion pipeline.
Mirrors the Airflow DAG exactly: same 5 steps, same Mon-Fri 06:00 UTC schedule.
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Tuple

# Ensure python paths are correct if executing directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from prefect import flow, task
from prefect.logging import get_run_logger

logger = logging.getLogger(__name__)

@lru_cache(maxsize=1)
def _get_cached_services() -> Tuple[Any, Any, Any, Any, Any]:
    """Cached service instances — (arxiv_client, pdf_parser, database, metadata_fetcher, opensearch_client)."""
    from src.db.factory import make_database
    from src.services.arxiv.factory import make_arxiv_client
    from src.services.metadata_fetcher import make_metadata_fetcher
    from src.services.opensearch.factory import make_opensearch_client
    from src.services.pdf_parser.factory import make_pdf_parser_service

    arxiv_client = make_arxiv_client()
    pdf_parser = make_pdf_parser_service()
    database = make_database()
    opensearch_client = make_opensearch_client()
    metadata_fetcher = make_metadata_fetcher(arxiv_client, pdf_parser)

    return arxiv_client, pdf_parser, database, metadata_fetcher, opensearch_client


async def _run_paper_ingestion_pipeline(target_date: str, process_pdfs: bool = True) -> dict:
    """Fetch most recent papers from arXiv — sorted by submittedDate descending.

    arXiv export API bracket range queries (submittedDate:[X+TO+Y]) return empty
    results reliably. We fetch the latest max_results papers instead; DB deduplication
    ensures already-stored papers are skipped.
    """
    arxiv_client, _, database, metadata_fetcher, _ = _get_cached_services()

    max_results = arxiv_client.max_results
    logger.info(f"Fetching {max_results} most recent papers from cat:{arxiv_client.search_category} (run_date={target_date})")

    with database.get_session() as session:
        return await metadata_fetcher.fetch_and_process_papers(
            max_results=max_results,
            from_date=None,
            to_date=None,
            process_pdfs=process_pdfs,
            store_to_db=True,
            db_session=session,
        )


async def _index_papers_with_chunks(papers) -> dict:
    """Chunk and embed papers into OpenSearch — mirrors airflow indexing.py."""
    from src.services.indexing.factory import make_hybrid_indexing_service

    indexing_service = make_hybrid_indexing_service()

    papers_data = []
    for paper in papers:
        if hasattr(paper, "__dict__"):
            papers_data.append({
                "id": str(paper.id),
                "arxiv_id": paper.arxiv_id,
                "title": paper.title,
                "authors": paper.authors,
                "abstract": paper.abstract,
                "categories": paper.categories,
                "published_date": paper.published_date,
                "raw_text": paper.raw_text,
                "sections": paper.sections,
            })
        else:
            papers_data.append(paper)

    return await indexing_service.index_papers_batch(papers=papers_data, replace_existing=True)


@task(name="setup-environment", retries=2, retry_delay_seconds=60)
def setup_environment_task():
    logger = get_run_logger()
    logger.debug("[setup-environment] task started")
    logger.info("Setting up environment for arXiv paper ingestion")

    from sqlalchemy import text

    arxiv_client, _pdf_parser, database, _metadata_fetcher, opensearch_client = _get_cached_services()

    with database.get_session() as session:
        session.execute(text("SELECT 1"))
        logger.info("Database connection verified")

    health = opensearch_client.client.cluster.health()
    if health["status"] not in ["green", "yellow", "red"]:
        raise Exception(f"OpenSearch cluster unhealthy: {health['status']}")
    logger.info(f"OpenSearch connected (cluster status: {health['status']})")

    setup_results = opensearch_client.setup_indices(force=False)
    logger.info("Hybrid search index %s" % ("created" if setup_results.get("hybrid_index") else "already exists"))
    logger.info("RRF pipeline %s" % ("created" if setup_results.get("rrf_pipeline") else "already exists"))
    logger.info(f"arXiv client ready: {arxiv_client.base_url}")

    result = {"status": "success", "message": "Environment setup completed"}
    logger.debug(f"[setup-environment] completed with result: {result}")
    return result


@task(name="fetch-daily-papers", retries=2, retry_delay_seconds=1800)
def fetch_daily_papers_task():
    logger = get_run_logger()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    logger.debug(f"[fetch-daily-papers] task started, target_date={yesterday}")
    logger.info(f"Fetching papers for date: {yesterday}")

    results = asyncio.run(_run_paper_ingestion_pipeline(target_date=yesterday, process_pdfs=True))
    target_date = yesterday
    results["date"] = yesterday

    logger.info(f"Daily fetch complete: {results['papers_fetched']} papers fetched")
    logger.debug(f"[fetch-daily-papers] completed: {results}")
    return results


@task(name="index-papers-hybrid", retries=1, retry_delay_seconds=300)
def index_papers_hybrid_task(fetch_results: dict):
    logger = get_run_logger()
    logger.debug(f"[index-papers-hybrid] task started with fetch_results keys: {list(fetch_results.keys()) if isinstance(fetch_results, dict) else fetch_results}")
    logger.info(f"Indexing papers: {fetch_results.get('papers_stored', 0)} stored")

    from src.db.factory import make_database
    from src.models.paper import Paper
    from sqlalchemy import desc

    database = make_database()

    with database.get_session() as session:
        if fetch_results and fetch_results.get("papers_stored", 0) > 0:
            papers = session.query(Paper).order_by(desc(Paper.created_at)).limit(fetch_results["papers_stored"]).all()
        else:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=1)
            papers = session.query(Paper).filter(Paper.created_at >= cutoff_date).all()

        if not papers:
            logger.info("No papers to index for hybrid search")
            return {"papers_indexed": 0, "chunks_created": 0}

        logger.info(f"Indexing {len(papers)} papers for hybrid search")
        stats = asyncio.run(_index_papers_with_chunks(papers))

    logger.info(
        f"Hybrid indexing complete: {stats['papers_processed']} papers, "
        f"{stats['total_chunks_created']} chunks created, "
        f"{stats['total_chunks_indexed']} chunks indexed"
    )
    logger.debug(f"[index-papers-hybrid] completed: {stats}")
    return stats


@task(name="generate-daily-report")
def generate_daily_report_task(fetch_stats: dict, hybrid_stats: dict):
    logger = get_run_logger()
    logger.debug(f"[generate-daily-report] task started — fetch_stats={fetch_stats}, hybrid_stats={hybrid_stats}")
    logger.info("Generating daily report")

    report = {
        "execution_date": datetime.now(timezone.utc).isoformat(),
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
        from sqlalchemy import func
        from src.models.paper import Paper

        _arxiv_client, _pdf_parser, database, _metadata_fetcher, opensearch_client = _get_cached_services()

        with database.get_session() as session:
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

    logger.info("Daily Ingestion Report:")
    logger.info(json.dumps(report, indent=2, default=str))

    result = report
    logger.debug(f"[generate-daily-report] completed: {result}")
    return result


@task(name="cleanup-temp-files")
def cleanup_temp_files_task():
    logger = get_run_logger()
    logger.debug("[cleanup-temp-files] task started")
    logger.info("Cleaning up temp files")
    subprocess.run(
        "find /tmp -name '*.pdf' -type f -mtime +30 -delete 2>/dev/null || true",
        shell=True,
        check=False,
    )
    logger.debug("[cleanup-temp-files] completed")


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
    schedule_cron = os.getenv("PREFECT__SCHEDULE", "0 6 * * 1-5")
    print(f"Starting Prefect deployment serve with cron schedule: {schedule_cron}")
    arxiv_ingestion_flow.serve(
        name="arxiv-ingestion-daily",
        cron=schedule_cron,
    )
