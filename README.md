# ArxivLens: Agentic RAG for Academic Research

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12+-blue.svg" alt="Python Version">
  <img src="https://img.shields.io/badge/FastAPI-0.115+-green.svg" alt="FastAPI">
  <img src="https://img.shields.io/badge/OpenSearch-2.19.5-orange.svg" alt="OpenSearch">
  <img src="https://img.shields.io/badge/LLM%20Provider-MeshAPI-blueviolet.svg" alt="LLM Provider">
  <img src="https://img.shields.io/badge/Model-openai%2Fgpt--5.4-blue.svg" alt="LLM Model">
  <img src="https://img.shields.io/badge/Orchestration-Prefect%203-blue.svg" alt="Prefect 3">
  <img src="https://img.shields.io/badge/Cache-Upstash%20Redis-red.svg" alt="Upstash Redis">
  <img src="https://img.shields.io/badge/Telemetry-Langfuse-purple.svg" alt="Langfuse Telemetry">
  <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License">
</p>

<br>

ArxivLens is a production-grade, agentic RAG (Retrieval-Augmented Generation) system designed to automate the ingestion, parsing, indexing, and conversational analysis of academic papers from arXiv.

Built with **FastAPI**, **OpenSearch**, **LangGraph**, and a dual orchestration layer (**Apache Airflow** + **Prefect 3**), ArxivLens transitions traditional search into an intelligent research assistant. It features hybrid (BM25 + vector) search, relevance grading, automatic query rewriting, caching, observability tracing, and dual client interfaces (Gradio Web UI and Telegram Bot).

> [!IMPORTANT]
> **🚀 Featured: Powered by MeshAPI**  
> ArxivLens is integrated with **MeshAPI** as its primary LLM backend. By using MeshAPI's OpenAI-compatible router at `https://api.meshapi.ai/v1`, the system routes LLM requests to advanced next-generation models (e.g. `openai/gpt-5.4`) using the standard `openai` library—without changing a single line of orchestration code.

---

## 🚀 Key Capabilities

- **State-Based Agentic Retrieval** — Orchestrated via LangGraph with input guardrails, document relevance grading, and adaptive multi-attempt query rewriting.
- **MeshAPI Core Integration** — Uses MeshAPI's OpenAI-compatible router to dynamically execute logic against advanced next-gen LLMs (e.g. `openai/gpt-5.4`) without client code modifications.
- **Hybrid Search Engine** — Combines BM25 keyword matching with Jina AI semantic vector embeddings (`jina-embeddings-v3`, 1024-dim) combined using Reciprocal Rank Fusion (RRF).
- **Dual Orchestration** — Choose between **Apache Airflow** (profile `airflow`) or **Prefect 3** (profile `prefect`) to run the same ingestion pipeline; both are docker-compose profile-gated so only one runs at a time.
- **Automated Processing Pipeline** — Scheduled DAGs/flows harvest papers, parse PDFs into markdown structure using Docling, and populate the databases.
- **Production Observability & Tracing** — Complete execution path tracking (latency, token usage, prompts, cost) using Langfuse Cloud.
- **High-Performance Caching** — SHA-256 keyed exact-match caching via Upstash Redis to deliver sub-millisecond response times for cached queries.
- **Multi-Interface Access** — Desktop conversational interface via Gradio and mobile accessibility via a Telegram Bot.

---

## 🏗️ System Architecture

ArxivLens is separated into a batch ingestion data pipeline and a real-time conversational agent pipeline. The following diagram illustrates the flow of data through the entire system:

```mermaid
graph TD
    %% Styling
    classDef infra fill:#1c1c1c,stroke:#444,stroke-width:1px,color:#ddd;
    classDef service fill:#1b365d,stroke:#2b5c8f,stroke-width:2px,color:#fff;
    classDef db fill:#1e4620,stroke:#3b7a3e,stroke-width:2px,color:#fff;
    classDef client fill:#5c3e1e,stroke:#a66c3f,stroke-width:2px,color:#fff;
    classDef external fill:#4a1e4a,stroke:#8a3a8a,stroke-width:1px,color:#fff;

    %% Ingestion Flow
    subgraph Ingestion ["Ingestion Pipeline (Batch)"]
        Airflow["Apache Airflow<br>(DAG Orchestration)"]:::service
        Prefect["Prefect 3<br>(Flow Orchestration)"]:::service
        ArxivAPI["arXiv API<br>(Paper Ingestion)"]:::external
        Docling["Docling Parser<br>(Scientific PDF Extraction)"]:::service
        Postgres["Neon PostgreSQL<br>(Paper Metadata Db)"]:::db
    end

    %% Storage & Indexing
    subgraph Storage ["Storage & Search Foundation"]
        OpenSearch["OpenSearch Cluster<br>(Hybrid Index)"]:::db
        JinaAI["Jina AI Embeddings<br>(1024-dim Vectors)"]:::external
    end

    %% Query Pipeline
    subgraph API_Services ["API & Agentic Query Layer"]
        FastAPI["FastAPI App<br>(REST API & Endpoints)"]:::service
        LangGraph["LangGraph Engine<br>(Agentic State Machine)"]:::service
        MeshAPI["MeshAPI Endpoint<br>(OpenAI-Compatible LLM)"]:::external
        Redis["Upstash Redis<br>(Exact-Match Cache)"]:::db
        Langfuse["Langfuse Cloud<br>(Tracing & Observability)"]:::external
    end

    %% Clients
    subgraph Clients ["User Interfaces"]
        Gradio["Gradio Web UI<br>(Model Selector Chat)"]:::client
        Telegram["Telegram Bot<br>(Mobile Chat Client)"]:::client
    end

    %% Ingestion Connections
    Airflow -->|1. Trigger fetch| ArxivAPI
    Prefect -->|1. Trigger fetch| ArxivAPI
    ArxivAPI -->|2. Download PDF| Docling
    Docling -->|3. Store metadata| Postgres
    Docling -->|4. Chunk & Embed| JinaAI
    JinaAI -->|5. Store vector indices| OpenSearch

    %% Query Connections
    Gradio & Telegram <-->|Interact| FastAPI
    FastAPI -->|Check Cache| Redis
    FastAPI -->|Record Traces| Langfuse
    FastAPI -->|Execute Agent| LangGraph
    LangGraph -->|Retrieve Context| OpenSearch
    LangGraph -->|Generate Responses| MeshAPI
```

---

## 🛠️ Deep Dive: Core Subsystems

### 1. Data Ingestion & Parsing Pipeline
The data ingestion pipeline runs automatically — scheduled via **Apache Airflow** (default) or **Prefect 3** (alternative) — to fetch papers, parse their complex layouts, extract metadata, and index them. Both orchestrators execute identical shared pipeline logic from `src/pipelines/ingestion/shared.py`.

```mermaid
sequenceDiagram
    autonumber
    actor Orchestrator as Airflow DAG / Prefect Flow
    participant ACM as ArxivClient (Metadata)
    participant PGS as Postgres DB
    participant DL as Docling (PDF Parser)
    participant Jina as Jina AI API
    participant OS as OpenSearch (Index)

    Orchestrator->>ACM: Fetch latest academic papers metadata
    ACM-->>Orchestrator: Return paper metadata (ID, title, authors, abstract)
    Orchestrator->>PGS: Check if paper already exists / Save metadata
    alt Paper is new
        Orchestrator->>ACM: Download PDF file to local storage
        ACM-->>Orchestrator: Return PDF path
        Orchestrator->>DL: Parse PDF layout & equations
        DL-->>Orchestrator: Extract Markdown structured text & sections
        Orchestrator->>Jina: Generate embeddings for section chunks (1024-dim)
        Jina-->>Orchestrator: Return dense vectors
        Orchestrator->>OS: Index chunks (text + embeddings + metadata)
        OS-->>Orchestrator: Confirm indexing complete
        Orchestrator->>PGS: Mark paper status as "processed"
    end
```

- **Docling Parser**: Extracts layout-aware structures, headers, and text from academic PDFs, preserving document context far better than basic PDF readers.
- **Section-Aware Chunking**: Chunks text into 600-word segments with 100-word overlaps, honoring header structure to keep sections cohesive.

---

### 2. Prefect 3 Orchestration

Prefect 3 is available as a drop-in alternative to Airflow. It runs the same five ingestion steps as a native Prefect flow with built-in retries, a real-time UI, and structured logging at `DEBUG` level.

#### Flow Architecture

```mermaid
flowchart TD
    classDef task fill:#1b365d,stroke:#2b5c8f,stroke-width:2px,color:#fff;
    classDef server fill:#4a1e4a,stroke:#8a3a8a,stroke-width:2px,color:#fff;
    classDef schedule fill:#1e4620,stroke:#3b7a3e,stroke-width:1px,color:#fff;

    Schedule(["Cron Schedule<br>Mon–Fri 06:00 UTC"]):::schedule

    subgraph PrefectServer ["Prefect Server  (localhost:4200)"]
        UI["Prefect UI<br>(Flow Runs · Logs · Schedules)"]:::server
    end

    subgraph PrefectWorker ["Prefect Worker (rag-prefect-worker)"]
        T1["setup-environment<br>retries: 2 · delay: 60s"]:::task
        T2["fetch-daily-papers<br>retries: 2 · delay: 1800s"]:::task
        T3["index-papers-hybrid<br>retries: 1 · delay: 300s"]:::task
        T4["generate-daily-report"]:::task
        T5["cleanup-temp-files"]:::task
    end

    Schedule -->|triggers| T1
    T1 --> T2
    T2 --> T3
    T3 --> T4
    T4 --> T5
    T1 & T2 & T3 & T4 & T5 -->|stream logs| UI
```

#### Key Properties

| Property | Value |
|---|---|
| **Flow name** | `arxiv-paper-ingestion` |
| **Deployment name** | `arxiv-ingestion-daily` |
| **Default schedule** | Mon–Fri 06:00 UTC (`0 6 * * 1-5`) |
| **Schedule override** | `PREFECT__SCHEDULE` env var |
| **Log level** | `DEBUG` (all levels captured) |
| **Extra loggers** | `docling`, `opensearch`, `httpx`, `sqlalchemy`, `urllib3` |
| **UI** | [http://localhost:4200](http://localhost:4200) |

#### Task Retry Policy

| Task | Retries | Retry Delay |
|---|---|---|
| `setup-environment` | 2 | 60 s |
| `fetch-daily-papers` | 2 | 30 min |
| `index-papers-hybrid` | 1 | 5 min |
| `generate-daily-report` | — | — |
| `cleanup-temp-files` | — | — |

#### Shared Pipeline Logic
Both Airflow and Prefect call the same underlying functions in `src/pipelines/ingestion/shared.py`:
- `run_setup_environment()` — Initialises OpenSearch index and Postgres schema.
- `run_fetch_daily_papers(target_date)` — Fetches metadata + PDFs from arXiv, runs Docling, stores to Postgres.
- `run_index_papers_hybrid(fetch_results)` — Chunks text, generates Jina embeddings, upserts to OpenSearch.
- `run_generate_daily_report(fetch_stats, hybrid_stats)` — Compiles and logs a summary report.
- `run_cleanup_temp_files()` — Removes temporary PDF files from disk.

---

### 3. Hybrid Search & Reciprocal Rank Fusion (RRF)
To ensure high retrieval recall and precision, the system processes queries in parallel, combining keyword matching and vector search.

```mermaid
flowchart TD
    %% Styling
    classDef step fill:#2b2b2b,stroke:#555,stroke-width:1px,color:#fff;
    classDef process fill:#1b365d,stroke:#2b5c8f,stroke-width:2px,color:#fff;
    classDef fusion fill:#4a1e4a,stroke:#8a3a8a,stroke-width:2px,color:#fff;
    classDef input fill:#1e4620,stroke:#3b7a3e,stroke-width:1px,color:#fff;

    Query(["User Query: 'What is RAG?'"]):::input
    
    subgraph ParallelSearch ["Parallel Retrieval Paths"]
        %% BM25 Keyword Search
        BM25["BM25 Keyword Matching<br>(Lexical Scoring)"]:::process
        KeywordQuery["Query DSL<br>(Title/Abstract/Text Boosting)"]:::step
        KeywordDocs["Top K Keyword Matches<br>(BM25 Scores)"]:::step
        
        %% Vector Semantic Search
        Vector["Dense Vector Matching<br>(Semantic Similarity)"]:::process
        JinaClient["Jina AI Embeddings v3<br>(Generate Query Vector)"]:::step
        VectorDocs["Top K Vector Matches<br>(Cosine Similarity)"]:::step
    end
    
    subgraph Fusion ["Rank Fusion Engine"]
        RRF["Reciprocal Rank Fusion (RRF)<br>Formula: Score = Σ 1 / (60 + Rank)"]:::fusion
        RankedDocs["Fused & Sorted Document Chunks"]:::step
    end
    
    %% Connections
    Query --> BM25
    Query --> Vector
    
    BM25 --> KeywordQuery
    KeywordQuery --> KeywordDocs
    
    Vector --> JinaClient
    JinaClient --> VectorDocs
    
    KeywordDocs --> RRF
    VectorDocs --> RRF
    
    RRF --> RankedDocs
    RankedDocs --> Output(["Top Chunks for RAG Context"]):::input
```

- **BM25 Search**: Matches exact keywords, acronyms, and terminology across text fields, with search boosting configured on paper titles and abstracts.
- **Semantic Vector Search**: Generates dense embeddings via Jina AI API to capture conceptual similarity, even if matching keywords are absent.
- **RRF (Reciprocal Rank Fusion)**: Dynamically merges results from both paths. Rank scores are calculated by summing the reciprocal ranks across the keyword and vector result lists, avoiding score scale differences.

---

### 4. Agentic RAG (LangGraph Workflow)
When executing `/api/v1/ask-agentic`, the query runs through a LangGraph state machine. This flow provides input guardrails, relevancy grading on retrieved chunks, and query rewriting if the initial results are insufficient.

```mermaid
stateDiagram-v2
    direction TB
    [*] --> Guardrail : User Query
    
    state Guardrail {
        [*] --> CheckScope
        CheckScope --> EvaluateScore : Threshold = 60
    }
    
    Guardrail --> OutOfScope : score < 60
    Guardrail --> Retrieve : score >= 60
    
    state OutOfScope {
        [*] --> FormatOOSMessage
        FormatOOSMessage --> [*]
    }
    OutOfScope --> [*] : Return out-of-scope response
    
    state Retrieve {
        [*] --> CallSearchTool
        CallSearchTool --> FetchOpenSearch
    }
    
    Retrieve --> GradeDocuments : Document chunks retrieved
    
    state GradeDocuments {
        [*] --> LLMGradeChunks
        LLMGradeChunks --> FilterIrrelevant
        FilterIrrelevant --> CheckRelevanceCount
    }
    
    GradeDocuments --> GenerateAnswer : Count > 0 OR Max Attempts Reached
    GradeDocuments --> RewriteQuery : Count == 0 AND Attempts < Max
    
    state RewriteQuery {
        [*] --> LLMRewriteQuery
        LLMRewriteQuery --> IncrementAttempts
    }
    RewriteQuery --> Retrieve : Retry with refined query
    
    state GenerateAnswer {
        [*] --> ConstructPrompt
        ConstructPrompt --> CallMeshAPI
        CallMeshAPI --> FormatResponse
    }
    
    GenerateAnswer --> [*] : Return final response with citations
```

- **Guardrail Node**: Validates that queries relate to science, engineering, or academic papers. Out-of-scope queries (e.g., "What is the best pizza recipe?") are blocked before calling downstream tools.
- **Retrieval Node**: Invokes the hybrid OpenSearch tool.
- **Document Grading Node**: Grades each retrieved document chunk. Irrelevant chunks are discarded.
- **Query Rewrite Node**: If no documents pass grading, the LLM rewrites the query to improve retrieval matching (up to 3 attempts).
- **Generation Node**: Compiles the final answer using the filtered document context and returns it with paper citations.

---

### 5. Caching & Tracing Performance Layer
Queries are monitored and optimized for performance:

- **Upstash Redis Caching**: Incoming queries are hashed. Cache hits bypass the LangGraph state machine and the MeshAPI call completely, returning the cached response in <10ms. A 6-hour Time-to-Live (TTL) is applied to all keys.
- **Langfuse Tracing**: Traces every pipeline node, monitoring LLM latency, token counts, and input/output payloads. This provides complete cost and error visibility in production.

---

## 🚀 Quick Start

### Prerequisites
- **Docker & Docker Compose** (6GB+ allocated RAM, 5GB+ disk space)
- **Python 3.12**
- **UV Package Manager** ([Installation Guide](https://docs.astral.sh/uv/getting-started/installation/))

### Cloud Accounts Needed (Free Tiers)
1. **MeshAPI** (or OpenAI) — For OpenAI-compatible LLM generation and evaluation (using `https://api.meshapi.ai/v1`)
2. **Jina AI** — For 1024-dim dense embeddings ([console](https://jina.ai))
3. **Neon PostgreSQL** — Serverless PostgreSQL database ([console](https://console.neon.tech))
4. **Upstash** — Serverless Redis cache ([console](https://console.upstash.com))
5. **Langfuse Cloud** — Trace telemetry dashboard ([console](https://cloud.langfuse.com))

---

### Installation & Run

1. **Clone the Repository & Setup Dependencies**
   ```bash
   git clone https://github.com/sourangshupal/Agentic-RAG-project.git
   cd Agentic-RAG-project
   uv sync
   ```

2. **Configure Environment Variables**
   Create a `.env` file in the root directory:
   ```env
   # LLM & Embedding API Keys
   OPENAI_API_KEY=your-meshapi-api-key
   OPENAI_BASE_URL="https://api.meshapi.ai/v1"
   OPENAI_MODEL="openai/gpt-5.4"
   JINA_API_KEY=your-jina-api-key

   # Neon Serverless PostgreSQL
   POSTGRES_DATABASE_URL="postgresql://user:password@subdomain.neon.tech/dbname?sslmode=require"

   # Upstash Serverless Redis (Use TCP connection URL)
   REDIS__URL="rediss://default:token@host.upstash.io:6379"

   # Langfuse Cloud Telemetry
   LANGFUSE__PUBLIC_KEY=pk-lf-...
   LANGFUSE__SECRET_KEY=sk-lf-...
   LANGFUSE__HOST="https://us.cloud.langfuse.com"

   # Telegram Bot configuration (Optional)
   TELEGRAM__BOT_TOKEN=your-telegram-bot-token
   ```
   > [!IMPORTANT]
   > Ensure variables for nested configurations contain double underscores (e.g. `REDIS__URL` and `LANGFUSE__PUBLIC_KEY`). Single-underscore configurations are ignored by Pydantic Settings.

3. **Verify API Connections**
   Ensure all cloud services are online and credentials are valid:
   ```bash
   uv run python scripts/test_connections.py
   ```

4. **Start the Local Containers**

   Launch the core stack (API + OpenSearch + OpenSearch Dashboards):
   ```bash
   make start
   ```

   To start with **Airflow** orchestration:
   ```bash
   docker compose --profile airflow up --build -d
   ```

   To start with **Prefect** orchestration:
   ```bash
   docker compose --profile prefect up --build -d
   ```

   > [!NOTE]
   > Only one orchestrator profile should be active at a time. Airflow runs on port `8080`, Prefect Server UI runs on port `4200`.

5. **Verify REST API Health**
   ```bash
   curl http://localhost:8000/api/v1/health
   ```

---

### Access Local Dashboards

| Service | Address | Purpose | Credentials | Profile |
|---------|---------|---------|-------------|--------|
| **API Documentation** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive OpenAPI specs | — | default |
| **Gradio Web Interface** | [http://localhost:7861](http://localhost:7861) | Chat interface with model selection | — | default |
| **OpenSearch Dashboards** | [http://localhost:5601](http://localhost:5601) | OpenSearch cluster management | — | default |
| **Airflow Orchestrator** | [http://localhost:8080](http://localhost:8080) | DAG scheduling & run history | `admin` / `admin` | `airflow` |
| **Prefect Server UI** | [http://localhost:4200](http://localhost:4200) | Flow runs, logs & schedules | — | `prefect` |
| **Langfuse Dashboard** | [https://us.cloud.langfuse.com](https://us.cloud.langfuse.com) | Trace latency, cost, and flows | Cloud Login | — |

---

## ⚙️ Configuration Reference

### Project Directory Structure
```
ArxivLens/
├── src/                          # Core Application Source Code
│   ├── routers/                  # FastAPI router endpoints (ask, search, health)
│   ├── services/                 # Subsystem clients and engines
│   │   ├── agents/               # LangGraph state machine, nodes, and tool definitions
│   │   ├── arxiv/                # arXiv API Client & Downloader
│   │   ├── cache/                # Upstash Redis caching interface
│   │   ├── embeddings/           # Jina AI vector embedding client
│   │   ├── langfuse/             # Langfuse telemetry configuration
│   │   ├── openai_llm/           # OpenAI client wrapper
│   │   ├── opensearch/           # OpenSearch query DSL & mapping client
│   │   ├── pdf_parser/           # PDF layout parsing using Docling
│   │   └── telegram/             # Telegram Bot command handlers
│   ├── pipelines/
│   │   └── ingestion/
│   │       └── shared.py         # Shared ingestion functions (Airflow & Prefect both call these)
│   ├── models/                   # SQLAlchemy ORM models
│   ├── schemas/                  # Pydantic schemas (requests & responses)
│   ├── main.py                   # FastAPI application entrypoint
│   └── config.py                 # Pydantic Settings configuration
├── airflow/                      # Airflow orchestration
│   ├── dags/                     # DAG definitions (arxiv_paper_ingestion)
│   ├── entrypoint.sh             # Container bootstrap (db migrate, user create)
│   └── Dockerfile                # Custom Airflow container using uv
├── prefect/                      # Prefect 3 orchestration
│   ├── flows/
│   │   └── arxiv_paper_ingestion.py  # Prefect flow (mirrors Airflow DAG)
│   └── Dockerfile                # Prefect worker container using uv
├── scripts/                      # Administration & verification scripts
│   └── test_connections.py       # Verification script for cloud services
├── tests/                        # Unit and API integration tests
│   ├── unit/                     # Mocked unit tests
│   └── api/                      # Local API testing suites
├── compose.yml                   # Multi-profile compose (default · airflow · prefect)
├── Makefile                      # Shortcut tasks for developer operations
└── step-by-step.md               # Documentation on system features
```

### Main API Endpoints

| Endpoint | Method | Input / Format | Purpose |
|----------|--------|----------------|---------|
| `/api/v1/health` | GET | None | Checks connectivity to PostgreSQL, OpenSearch, and Redis. |
| `/api/v1/hybrid-search/` | POST | JSON: `{ "query": "str", "top_k": 3, "use_hybrid": true }` | Retrieves matching paper chunks using BM25 or Hybrid Search. |
| `/api/v1/ask` | POST | JSON: `{ "query": "str", "model": "openai/gpt-5.4-mini" }` | Returns a standard, synchronous RAG response. |
| `/api/v1/stream` | POST | JSON: `{ "query": "str" }` | Streams a RAG response in real-time using Server-Sent Events (SSE). |
| `/api/v1/ask-agentic` | POST | JSON: `{ "query": "str" }` | Routes the query through the LangGraph agent workflow. |
| `/api/v1/feedback` | POST | JSON: `{ "trace_id": "str", "score": 1 }` | Logs user feedback directly to Langfuse. |

---

## 🔧 Operations Command Guide

### Container Operations
```bash
make start                                            # Start core stack (API + OpenSearch + Dashboards)
make stop                                             # Shut down all local containers
make health                                           # Print Docker service states
make status                                           # Check container health statuses

# Airflow profile
docker compose --profile airflow up --build -d        # Start core stack + Airflow
docker compose --profile airflow down                 # Stop Airflow stack

# Prefect profile
docker compose --profile prefect up --build -d        # Start core stack + Prefect Server + Worker
docker compose --profile prefect down                 # Stop Prefect stack
```

### Logs & Diagnostics
```bash
docker compose logs -f api                            # Tail FastAPI application logs
docker compose logs -f airflow                        # Tail Airflow scheduler and webserver logs
docker compose logs -f prefect-server                 # Tail Prefect Server logs
docker compose logs -f prefect-worker                 # Tail Prefect Worker / flow run logs
```

### Prefect: Trigger a Manual Run
```bash
# From inside the prefect-worker container
docker exec -it rag-prefect-worker \
  python -c "from flows.arxiv_paper_ingestion import arxiv_ingestion_flow; arxiv_ingestion_flow()"

# Or via the Prefect UI at http://localhost:4200 — click the deployment → "Run" button
```

### Prefect: Override the Schedule
Set `PREFECT__SCHEDULE` in `.env` (standard cron expression):
```env
# Run every day at 08:00 UTC instead of the default Mon–Fri 06:00
PREFECT__SCHEDULE=0 8 * * *
```

### Running Tests
```bash
make test                                             # Run all test suites
uv run pytest tests/unit/ -v                          # Run unit tests
uv run pytest tests/api/ -v                           # Run API integration tests
uv run pytest tests/unit/services/agents/ -v          # Run LangGraph agent node tests
```

### Format & Lint
```bash
make format                          # Format Python code using Ruff
make lint                            # Execute Ruff checks and MyPy type analysis
```

### System Reset
```bash
# Deletes OpenSearch data volumes and rebuilds containers.
# Note: Does not affect Neon PostgreSQL or Upstash Redis cloud instances.
docker compose down --volumes && make start
```

---

## 🛠️ Troubleshooting Matrix

| Symptoms | Root Cause | Remediation |
|----------|------------|-------------|
| **`test_connections.py` fails** | Invalid credentials in `.env` | Recheck API keys. Ensure no whitespaces or quotes are present in values unless required. |
| **No traces appear in Langfuse** | Host or Key not recognized | Confirm nested keys contain double underscores (e.g. `LANGFUSE__PUBLIC_KEY`). Check if the host matches your cloud region. |
| **Redis connection fails** | REST client URL used instead of TCP | Make sure you copy the **TCP Redis URI** (`rediss://...`) from the Upstash console, not the REST API endpoint. |
| **Search queries return 0 matches** | OpenSearch index is empty | Ingestion has not run yet. Trigger the pipeline manually via Airflow UI or Prefect UI, or run the flow directly in the worker container. |
| **OpenSearch container fails** | Insufficient RAM allocated | OpenSearch requires significant memory. Increase Docker Desktop memory allocation to at least 8GB. |
| **FastAPI container stays unhealthy** | Invalid configuration variables | Run `docker compose logs api` to locate configuration errors. Typically caused by a malformed Postgres or Redis connection URI. |
| **`prefect-worker` build fails (exit 2)** | PyTorch CPU wheel index has no ARM64 wheels | The `whl/cpu` index is x86-64 only. The Dockerfile now installs torch from PyPI which ships native ARM64 wheels — rebuild with `docker compose --profile prefect up --build -d`. |
| **Prefect worker shows only INFO logs** | Default Prefect log level is INFO | Ensure `PREFECT_LOGGING_LEVEL=DEBUG` is set in `compose.yml` for both `prefect-server` and `prefect-worker`. |
| **Prefect worker can't reach Prefect server** | API URL not set or wrong hostname | Verify `PREFECT_API_URL=http://prefect-server:4200/api` in the `prefect-worker` environment block. |
| **Prefect flow not appearing in UI** | Worker hasn't registered the deployment | Check `docker compose logs prefect-worker`. The worker must complete the `arxiv_ingestion_flow.serve()` call before the deployment appears. |

---

## 💰 Operational Cost Analysis
- **Local Services**: Free (FastAPI, OpenSearch, Airflow).
- **Neon Database**: Free (512MB storage allocation).
- **Upstash Redis**: Free (10,000 commands/day allocation).
- **Langfuse Cloud**: Free (50,000 trace events/month allocation).
- **Jina AI Embeddings**: Free tier contains generous token limits.
- **LLM API (MeshAPI)**: Pay-as-you-go. A query using the configured model with 3 chunk contexts of 1,800 tokens is highly cost-effective (e.g. less than **$0.0008**). When Redis hits a cache match, the operational cost is **$0**.

---

## 🤝 Contributing
Issues and Pull Requests are welcome. For major architectural modifications, please open an issue first to discuss details.

---

## 📄 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.