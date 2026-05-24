"""
Shared ingestion pipeline logic — framework agnostic.
Called by both the Airflow task wrappers and the Prefect flow tasks.
"""

import asyncio
import logging
import subprocess
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Tuple

from sqlalchemy import desc, func, text

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_cached_services() -> Tuple[Any, Any, Any, Any, Any]:
    """Get cached service instances using lru_cache for automatic memoization.

    :returns: Tuple of (arxiv_client, pdf_parser, database, metadata_fetcher, opensearch_client)
    """
    logger.info("Initializing services (cached with lru_cache)")

    # Lazy imports to keep startup fast and light
    from src.db.factory import make_database
    from src.services.arxiv.factory import make_arxiv_client
    from src.services.metadata_fetcher import make_metadata_fetcher
    from src.services.opensearch.factory import make_opensearch_client
    from src.services.pdf_parser.factory import make_pdf_parser_service

    # Initialize core services
    arxiv_client = make_arxiv_client()
    pdf_parser = make_pdf_parser_service()
    database = make_database()
    opensearch_client = make_opensearch_client()

    # Create metadata fetcher with dependencies
    metadata_fetcher = make_metadata_fetcher(arxiv_client, pdf_parser)

    logger.info("All services initialized and cached with lru_cache")
    return arxiv_client, pdf_parser, database, metadata_fetcher, opensearch_client


def run_setup_environment() -> dict:
    """Setup environment and verify dependencies.

    Creates hybrid search index with RRF pipeline.
    """
    logger.info("Setting up environment for arXiv paper ingestion")

    try:
        arxiv_client, _pdf_parser, database, _metadata_fetcher, opensearch_client = get_cached_services()

        with database.get_session() as session:
            session.execute(text("SELECT 1"))
            logger.info("Database connection verified")

        try:
            health = opensearch_client.client.cluster.health()
            if health["status"] in ["green", "yellow", "red"]:
                logger.info(f"OpenSearch hybrid client connected (cluster status: {health['status']})")
            else:
                raise Exception(f"OpenSearch cluster unhealthy: {health['status']}")
        except Exception as e:
            raise Exception(f"OpenSearch hybrid client connection failed: {e}")

        setup_results = opensearch_client.setup_indices(force=False)
        if setup_results.get("hybrid_index"):
            logger.info("Hybrid search index created with vector support")
        else:
            logger.info("Hybrid search index already exists")

        if setup_results.get("rrf_pipeline"):
            logger.info("RRF pipeline created successfully")
        else:
            logger.info("RRF pipeline already exists")

        logger.info("Hybrid search setup completed")
        logger.info(f"arXiv client ready: {arxiv_client.base_url}")
        logger.info("PDF parser service ready (Docling models cached)")

        return {"status": "success", "message": "Environment setup completed"}

    except Exception as e:
        error_msg = f"Environment setup failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


async def _run_paper_ingestion_pipeline(
    target_date: str,
    process_pdfs: bool = True,
) -> dict:
    """Async wrapper for the paper ingestion pipeline.

    :param target_date: Date to fetch papers for (YYYYMMDD format)
    :param process_pdfs: Whether to download and process PDFs
    :returns: Dictionary with ingestion statistics
    """
    arxiv_client, _, database, metadata_fetcher, _ = get_cached_services()

    max_results = arxiv_client.max_results
    logger.info(f"Using default max_results from config: {max_results}")

    with database.get_session() as session:
        return await metadata_fetcher.fetch_and_process_papers(
            max_results=max_results,
            from_date=target_date,
            to_date=target_date,
            process_pdfs=process_pdfs,
            store_to_db=True,
            db_session=session,
        )


def run_fetch_daily_papers(target_date: str) -> dict:
    """Fetch daily papers from arXiv and store in PostgreSQL.

    :param target_date: Target date string (YYYYMMDD)
    """
    logger.info(f"Fetching papers for date: {target_date}")

    results = asyncio.run(
        _run_paper_ingestion_pipeline(
            target_date=target_date,
            process_pdfs=True,
        )
    )

    logger.info(f"Daily fetch complete: {results['papers_fetched']} papers for {target_date}")
    results["date"] = target_date
    return results


async def _index_papers_with_chunks(papers) -> dict:
    """Async helper to index papers with chunking and embeddings."""
    from src.services.indexing.factory import make_hybrid_indexing_service

    indexing_service = make_hybrid_indexing_service()

    papers_data = []
    for paper in papers:
        if hasattr(paper, "__dict__"):
            paper_dict = {
                "id": str(paper.id),
                "arxiv_id": paper.arxiv_id,
                "title": paper.title,
                "authors": paper.authors,
                "abstract": paper.abstract,
                "categories": paper.categories,
                "published_date": paper.published_date,
                "raw_text": paper.raw_text,
                "sections": paper.sections,
            }
        else:
            paper_dict = paper
        papers_data.append(paper_dict)

    stats = await indexing_service.index_papers_batch(papers=papers_data, replace_existing=True)
    return stats


def run_index_papers_hybrid(fetch_results: dict | None) -> dict:
    """Index papers with chunking and vector embeddings for hybrid search.

    :param fetch_results: Dictionary of fetch results (optional)
    """
    try:
        from src.db.factory import make_database

        database = make_database()

        with database.get_session() as session:
            from src.models.paper import Paper

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

            return stats

    except Exception as e:
        logger.error(f"Failed to index papers for hybrid search: {e}")
        raise


def run_generate_daily_report(fetch_stats: dict, hybrid_stats: dict, execution_date_val: Any = None) -> dict:
    """Generate a daily report of the ingestion pipeline results.

    :param fetch_stats: Stats from fetching step
    :param hybrid_stats: Stats from indexing step
    :param execution_date_val: Optional datetime object representing execution time
    """
    logger.info("Generating daily ingestion report")

    if not execution_date_val:
        execution_date_val = datetime.now(timezone.utc)

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
        from src.db.factory import make_database
        from src.services.opensearch.factory import make_opensearch_client

        database = make_database()
        opensearch_client = make_opensearch_client()

        with database.get_session() as session:
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
    import json

    def default_serializer(obj):
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    logger.info("Daily Ingestion Report:")
    logger.info(json.dumps(report, indent=2, default=default_serializer))

    return report


def run_cleanup_temp_files() -> None:
    """Remove PDFs older than 30 days to manage disk space."""
    logger.info("Cleaning up temporary files...")
    subprocess.run(
        "find /tmp -name '*.pdf' -type f -mtime +30 -delete 2>/dev/null || true",
        shell=True,
        check=False,
    )
    logger.info("Cleanup completed")


def run_verify_hybrid_index() -> dict:
    """Verify hybrid index health and get statistics."""
    try:
        from src.services.opensearch.factory import make_opensearch_client_fresh

        opensearch_client = make_opensearch_client_fresh()

        stats = opensearch_client.client.indices.stats(index=opensearch_client.index_name)
        count = opensearch_client.client.count(index=opensearch_client.index_name)

        paper_count_query = {"aggs": {"unique_papers": {"cardinality": {"field": "arxiv_id"}}}, "size": 0}
        paper_count_response = opensearch_client.client.search(index=opensearch_client.index_name, body=paper_count_query)
        unique_papers = paper_count_response["aggregations"]["unique_papers"]["value"]

        result = {
            "index_name": opensearch_client.index_name,
            "total_chunks": count["count"],
            "unique_papers": unique_papers,
            "avg_chunks_per_paper": (count["count"] / unique_papers if unique_papers > 0 else 0),
            "index_size_mb": stats["indices"][opensearch_client.index_name]["total"]["store"]["size_in_bytes"] / (1024 * 1024),
        }

        logger.info(
            f"Hybrid index stats: {result['total_chunks']} chunks, "
            f"{result['unique_papers']} papers, "
            f"{result['avg_chunks_per_paper']:.1f} chunks/paper"
        )

        return result

    except Exception as e:
        logger.error(f"Failed to verify hybrid index: {e}")
        raise
