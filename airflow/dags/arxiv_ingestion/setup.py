import logging

from src.pipelines.ingestion.shared import run_setup_environment

logger = logging.getLogger(__name__)


def setup_environment():
    """Setup environment and verify dependencies.

    Creates hybrid search index with RRF pipeline.
    """
    return run_setup_environment()
