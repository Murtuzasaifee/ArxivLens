# ArxivLens Project Architecture & Agent Guide

This document is a compressed reference map of the ArxivLens repository, designed to give an incoming AI agent a complete understanding of the system's database schemas, ingestion pipelines, search mechanisms, APIs, and the LangGraph-based state machine.

---

## 1. System & Tech Stack Overview
* **Ingestion (Batch):** Apache Airflow orchestrates the ingestion of CS/AI research papers from arXiv.
* **PDF Parsing:** Docling parses PDFs into clean structured text and section JSONs.
* **Database (Relational):** Neon Serverless PostgreSQL stores raw paper metadata and parsed content.
* **Vector & Text Search:** OpenSearch (local Docker container) hosts text chunks and Jina embeddings.
* **Embeddings Model:** Jina AI `jina-embeddings-v2-base-de` (1024 dimensions, cosine similarity).
* **LLM Model:** OpenAI `gpt-4o-mini` (or standard model overridden via env).
* **State-Based Agentic RAG:** Built with LangGraph, injecting dependencies via runtime context.
* **Observability:** Langfuse Cloud traces execution spans and token metrics.
* **Cache:** Upstash Redis caches endpoint queries (SHA-256 payload keys).
* **UIs:** Gradio Chat interface (`localhost:7861`) and Telegram bot wrapper.

---

## 2. Database Schema (PostgreSQL)
Defined in [src/models/paper.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/models/paper.py). Represents table **`papers`**:

* **`id`** (`UUID`, PK): Unique row identifier.
* **`arxiv_id`** (`String`, Unique, Index): Unique ID from arXiv (e.g. `"2310.00001"`).
* **`title`** (`String`): Title of the academic paper.
* **`authors`** (`JSON`): List of authors.
* **`abstract`** (`Text`): Short summary of the paper.
* **`categories`** (`JSON`): Subject classification tags (e.g., `["cs.AI", "cs.CL"]`).
* **`published_date`** (`DateTime`): Publication timestamp.
* **`pdf_url`** (`String`): Direct link to pdf download.
* **`raw_text`** (`Text`): Extracted full text from the PDF.
* **`sections`** (`JSON`): Hierarchical headings/text segments mapping.
* **`references`** (`JSON`): List of bibliographic references.
* **`parser_used`** (`String`): Name/version of parser (e.g. `"docling"`).
* **`parser_metadata`** (`JSON`): Parsing stats and logs.
* **`pdf_processed`** (`Boolean`): Set to `True` when parsing completes.
* **`pdf_processing_date`** (`DateTime`): Timestamp of PDF extraction.
* **`created_at`** / **`updated_at`** (`DateTime`): Audit timestamps.

---

## 3. Search Index Configuration (OpenSearch)
Defined in [src/services/opensearch/index_config_hybrid.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/opensearch/index_config_hybrid.py). Index **`arxiv-papers-chunks`**:

* **Settings:** Single shard, dynamic strict mapping, `index.knn` enabled, `knn.space_type` set to `"cosinesimil"`.
* **Analyzers:** `text_analyzer` (standard tokenizer with lowercase, stop words, and snowball filters).
* **Properties:**
  * **`chunk_id`** (`keyword`): Unique ID per segment.
  * **`arxiv_id`** (`keyword`): Link to Postgres record.
  * **`paper_id`** (`keyword`): DB record link.
  * **`chunk_index`** (`integer`): Index order of chunk.
  * **`chunk_text`** (`text` with keyword field): Segment text.
  * **`chunk_word_count`** (`integer`): Number of words.
  * **`start_char` / `end_char`** (`integer`): Character offsets.
  * **`embedding`** (`knn_vector`): 1024 dimensions, HNSW method, cosinesimil space, `nmslib` engine, `ef_construction` = 512, `m` = 16.
  * **`title` / `authors` / `abstract`** (`text` fields): Paper metadata for keyword lookups.
  * **`categories`** (`keyword`): Classification.
  * **`published_date`** (`date`): Publication date.
  * **`section_title`** (`keyword`): Chapter/Section name.
  * **`embedding_model`** (`keyword`): Model used.
* **Hybrid Search Post-Processor:** Custom search pipeline (`hybrid-rrf-pipeline`) using **Reciprocal Rank Fusion (RRF)** with default rank constant `k` = 60 to merge BM25 and vector results.

---

## 4. Ingestion Pipeline Workflow (Airflow)
Defined in [airflow/dags/arxiv_paper_ingestion.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/airflow/dags/arxiv_paper_ingestion.py):

```
[Fetch Metadata]
       ↓ (CS/AI category; metadata stored in Postgres)
[Download PDFs]
       ↓ (Docling parser extracts text, sections, and references)
[Store In Postgres]
       ↓ (Chunk text using overlapping sliding window: 600 words / 100-word overlap)
[Compute Embeddings & Index]
       ↓ (Jina API embedding generation -> push to OpenSearch index)
[Generate Daily Report]
```
* Task source definitions are located in [airflow/dags/arxiv_ingestion/](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/airflow/dags/arxiv_ingestion/).

---

## 5. LangGraph Agentic RAG State Machine
Orchestrated in [src/services/agents/agentic_rag.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/agentic_rag.py).

### State Schema (`AgentState`)
Defined in [src/services/agents/state.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/state.py). Keys:
* `messages`: Appended history (Human/AI/Tool messages).
* `original_query`: Saved original query.
* `rewritten_query`: Output query rewrite.
* `retrieval_attempts`: Total search loop count.
* `guardrail_result`: Guardrail score/reason.
* `routing_decision`: State router key.
* `relevant_sources`: Validated source items list.
* `grading_results`: Array of evaluation records.

### Graph Nodes (Lightweight Pure Functions)
1. **`guardrail`** ([guardrail_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/guardrail_node.py)): Uses LLM structured output to grade if a query is within academic bounds (0-100 score). Routes to `out_of_scope` if score < 60, else `retrieve`.
2. **`out_of_scope`** ([out_of_scope_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/out_of_scope_node.py)): Generates a fallback refusal message explaining system capabilities.
3. **`retrieve`** ([retrieve_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/retrieve_node.py)): Checks `retrieval_attempts`. If `>= max_retrieval_attempts` (default: 2), sets fallback error and halts. Otherwise, increments attempts and generates a tool call for `retrieve_papers`.
4. **`tool_retrieve`** (`ToolNode`): System node running the `retrieve_papers` tool ([src/services/agents/tools.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/tools.py)). Computes embedding, queries OpenSearch hybrid search, and outputs a `ToolMessage` with matching chunk contents.
5. **`grade_documents`** ([grade_documents_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/grade_documents_node.py)): Inspects retrieval output using LLM. Returns binary grading relevance score. Routes to `generate_answer` if relevant, else `rewrite_query`.
6. **`rewrite_query`** ([rewrite_query_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/rewrite_query_node.py)): Uses LLM to translate vague terms into specific technical academic concepts. Appends a new `HumanMessage` with the refined query and loops back to `retrieve`.
7. **`generate_answer`** ([generate_answer_node.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/nodes/generate_answer_node.py)): Prompts LLM to write a cited answer using only relevant retrieved paper contexts.

### Dependency Injection (`Context`)
Defined in [src/services/agents/context.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/services/agents/context.py). Bundles active clients (`llm_client`, `opensearch_client`, `embeddings_client`, `langfuse_tracer`) and parameters (`model_name`, `temperature`, `top_k`, `max_retrieval_attempts`, `guardrail_threshold`) as immutable context available in every graph node execution.

---

## 6. API Routing & Endpoints
Implemented in [src/routers/](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/routers/):

* **`POST /api/v1/hybrid-search`** ([hybrid_search.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/routers/hybrid_search.py)): Exposes pure document retrieval. Supports lexical BM25, dense vector, or hybrid search using parameters `use_hybrid: bool`, `size: int`, `categories: list[str]`.
* **`POST /api/v1/ask`** ([ask.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/routers/ask.py) / `/stream`): Basic sequential RAG chain (Retrieval -> Generation). Answers queries using retrieved papers.
* **`POST /api/v1/ask-agentic`** ([agentic_ask.py](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/src/routers/agentic_ask.py)): Runs the LangGraph agent state machine, returning `answer`, `sources` metadata, `reasoning_steps`, and `retrieval_attempts`.

---

## 7. Caching & Observability Details
* **Redis Caching:** Integrated as a decorator on endpoints. The cache key is calculated as: `arxivlens:cache:<endpoint>:<sha256(payload_params)>`. If a hit is registered, the cached JSON is returned directly in `< 50ms`.
* **Langfuse Tracing:** Uses the v3 Python SDK context manager. When executing queries, `CallbackHandler` propagates spans down to embedding queries, database retrieval, prompt builders, and LLM calls, recording precise latency and token costs.

---

## 8. Essential Development Commands
Located in [Makefile](file:///Users/murtuzasaifee/Documents/Workstation/Codes/AI-Workspace/ArxivLens/Makefile):

* `make start`: Bootstraps 4 local containers (API, Airflow, OpenSearch, OpenSearch Dashboards).
* `make stop`: Shuts down containers.
* `make status`: Checks container health statuses.
* `make health`: Executes local connectivity tests.
* `make logs`: Streams stdout/stderr from containers.
* `make lint` / `make format`: Runs ruff formatting and mypy type checks.
* `uv run pytest tests/unit/services/agents/`: Runs Unit tests targeting graph execution nodes.
