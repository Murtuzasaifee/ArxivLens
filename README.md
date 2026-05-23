# ArxivLens: Agentic RAG for Academic Research

ArxivLens is a production-grade, agentic RAG (Retrieval-Augmented Generation) system designed to automate the ingestion, parsing, indexing, and conversational analysis of academic papers from arXiv. 

Built with **FastAPI**, **Apache Airflow**, **OpenSearch**, and **LangGraph**, ArxivLens transitions traditional search into an intelligent research assistant. It features hybrid (BM25 + vector) search, relevance grading, automatic query rewriting, caching, observability tracing, and dual client interfaces (Gradio Web UI and Telegram Bot).

---

## 🚀 Key Capabilities

- **State-Based Agentic Retrieval** — Orchestrated via LangGraph with input guardrails, document relevance grading, and adaptive multi-attempt query rewriting.
- **Hybrid Search Engine** — Combines BM25 keyword matching with Jina AI semantic vector embeddings (`jina-embeddings-v3`, 1024-dim) combined using Reciprocal Rank Fusion (RRF).
- **Automated Processing Pipeline** — Scheduled Airflow DAGs that harvest papers, parse PDFs into markdown structure using Docling, and populate the databases.
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
        OpenAI["OpenAI API<br>(gpt-4o-mini generation)"]:::external
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
    LangGraph -->|Generate Responses| OpenAI
```

---

## 🛠️ Deep Dive: Core Subsystems

### 1. Data Ingestion & Parsing Pipeline
The data ingestion pipeline runs automatically (scheduled via Apache Airflow) to fetch papers, parse their complex layouts, extract metadata, and index them.

```mermaid
sequenceDiagram
    autonumber
    actor Airflow as Airflow DAG
    participant ACM as ArxivClient (Metadata)
    participant PGS as Postgres DB
    participant DL as Docling (PDF Parser)
    participant Jina as Jina AI API
    participant OS as OpenSearch (Index)

    Airflow->>ACM: Fetch latest academic papers metadata
    ACM-->>Airflow: Return paper metadata (ID, title, authors, abstract)
    Airflow->>PGS: Check if paper already exists / Save metadata
    alt Paper is new
        Airflow->>ACM: Download PDF file to local storage
        ACM-->>Airflow: Return PDF path
        Airflow->>DL: Parse PDF layout & equations
        DL-->>Airflow: Extract Markdown structured text & sections
        Airflow->>Jina: Generate embeddings for section chunks (1024-dim)
        Jina-->>Airflow: Return dense vectors
        Airflow->>OS: Index chunks (text + embeddings + metadata)
        OS-->>Airflow: Confirm indexing complete
        Airflow->>PGS: Mark paper status as "processed"
    end
```

- **Docling Parser**: Extracts layout-aware structures, headers, and text from academic PDFs, preserving document context far better than basic PDF readers.
- **Section-Aware Chunking**: Chunks text into 600-word segments with 100-word overlaps, honoring header structure to keep sections cohesive.

---

### 2. Hybrid Search & Reciprocal Rank Fusion (RRF)
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

### 3. Agentic RAG (LangGraph Workflow)
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
        ConstructPrompt --> CallOpenAI
        CallOpenAI --> FormatResponse
    }
    
    GenerateAnswer --> [*] : Return final response with citations
```

- **Guardrail Node**: Validates that queries relate to science, engineering, or academic papers. Out-of-scope queries (e.g., "What is the best pizza recipe?") are blocked before calling downstream tools.
- **Retrieval Node**: Invokes the hybrid OpenSearch tool.
- **Document Grading Node**: Grades each retrieved document chunk. Irrelevant chunks are discarded.
- **Query Rewrite Node**: If no documents pass grading, the LLM rewrites the query to improve retrieval matching (up to 3 attempts).
- **Generation Node**: Compiles the final answer using the filtered document context and returns it with paper citations.

---

### 4. Caching & Tracing Performance Layer
Queries are monitored and optimized for performance:

- **Upstash Redis Caching**: Incoming queries are hashed. Cache hits bypass the LangGraph state machine and the OpenAI API completely, returning the cached response in <10ms. A 6-hour Time-to-Live (TTL) is applied to all keys.
- **Langfuse Tracing**: Traces every pipeline node, monitoring LLM latency, token counts, and input/output payloads. This provides complete cost and error visibility in production.

---

## 🚀 Quick Start

### Prerequisites
- **Docker & Docker Compose** (6GB+ allocated RAM, 5GB+ disk space)
- **Python 3.12**
- **UV Package Manager** ([Installation Guide](https://docs.astral.sh/uv/getting-started/installation/))

### Cloud Accounts Needed (Free Tiers)
1. **OpenAI** — For LLM generation and evaluation ([console](https://platform.openai.com))
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
   Create a `.env` file in the root directory. Use the template below (refer to [step-by-step.md](step-by-step.md) for details):
   ```env
   # LLM & Embedding API Keys
   OPENAI_API_KEY=your-openai-api-key
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
   Launch OpenSearch, OpenSearch Dashboards, Airflow, and the FastAPI application:
   ```bash
   docker compose up --build -d
   ```

5. **Verify REST API Health**
   ```bash
   curl http://localhost:8000/api/v1/health
   ```

---

### Access Local Dashboards

| Service | Address | Purpose | Credentials |
|---------|---------|---------|-------------|
| **API Documentation** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive OpenAPI specs | - |
| **Gradio Web Interface** | [http://localhost:7861](http://localhost:7861) | Chat interface with model selection | - |
| **Airflow Orchestrator** | [http://localhost:8080](http://localhost:8080) | Pipeline scheduling dashboard | `admin` / `admin` |
| **OpenSearch Dashboards** | [http://localhost:5601](http://localhost:5601) | OpenSearch cluster management | - |
| **Langfuse Dashboard** | [https://us.cloud.langfuse.com](https://us.cloud.langfuse.com) | Trace latency, cost, and flows | Cloud Login |

---

## ⚙️ Configuration Reference

### Project Directory Structure
```
Agentic-RAG-project/
├── src/                         # Core Application Source Code
│   ├── routers/                 # FastAPI router endpoints (ask, search, health)
│   ├── services/                # Subsystem clients and engines
│   │   ├── agents/              # LangGraph state machine, nodes, and tool definitions
│   │   ├── arxiv/               # arXiv API Client & Downloader
│   │   ├── cache/               # Upstash Redis caching interface
│   │   ├── embeddings/          # Jina AI vector embedding client
│   │   ├── langfuse/            # Langfuse telemetry configuration
│   │   ├── openai_llm/          # OpenAI client wrapper
│   │   ├── opensearch/          # OpenSearch query DSL & mapping client
│   │   ├── pdf_parser/          # PDF layout parsing using Docling
│   │   └── telegram/            # Telegram Bot command handlers
│   ├── models/                  # SQLAlchemy ORM models
│   ├── schemas/                 # Pydantic schemas (requests & responses)
│   ├── main.py                  # FastAPI application entrypoint
│   └── config.py                # Pydantic Settings configuration
├── airflow/                     # Airflow configuration and DAG schedules
│   ├── dags/                    # Ingestion workflows (arxiv_paper_ingestion)
│   └── Dockerfile               # Custom Airflow container utilizing uv
├── scripts/                     # Administration & verification scripts
│   └── test_connections.py      # Verification script for cloud services
├── tests/                       # Unit and API integration tests
│   ├── unit/                    # Mocked unit tests
│   └── api/                     # Local API testing suites
├── compose.yml                  # 4-container stack compose configuration
├── Makefile                     # Shortcut tasks for developer operations
└── step-by-step.md              # Documentation on system features
```

### Main API Endpoints

| Endpoint | Method | Input / Format | Purpose |
|----------|--------|----------------|---------|
| `/api/v1/health` | GET | None | Checks connectivity to PostgreSQL, OpenSearch, and Redis. |
| `/api/v1/hybrid-search/` | POST | JSON: `{ "query": "str", "top_k": 3, "use_hybrid": true }` | Retrieves matching paper chunks using BM25 or Hybrid Search. |
| `/api/v1/ask` | POST | JSON: `{ "query": "str", "model": "gpt-4o-mini" }` | Returns a standard, synchronous RAG response. |
| `/api/v1/stream` | POST | JSON: `{ "query": "str" }` | Streams a RAG response in real-time using Server-Sent Events (SSE). |
| `/api/v1/ask-agentic` | POST | JSON: `{ "query": "str" }` | Routes the query through the LangGraph agent workflow. |
| `/api/v1/feedback` | POST | JSON: `{ "trace_id": "str", "score": 1 }` | Logs user feedback directly to Langfuse. |

---

## 🔧 Operations Command Guide

### Container Operations
```bash
make start                           # Start all local containers (Docker Compose)
make stop                            # Shut down all local containers
make health                          # Print Docker service states
```

### Logs & Diagnostics
```bash
docker compose logs -f api           # Tail FastAPI application logs
docker compose logs -f airflow       # Tail Airflow scheduler and webserver logs
```

### Running Tests
```bash
make test                            # Run all test suites
uv run pytest tests/unit/ -v         # Run unit tests
uv run pytest tests/api/ -v          # Run API integration tests
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
docker compose down --volumes && docker compose up --build -d
```

---

## 🛠️ Troubleshooting Matrix

| Symptoms | Root Cause | Remediation |
|----------|------------|-------------|
| **`test_connections.py` fails** | Invalid credentials in `.env` | Recheck API keys. Ensure no whitespaces or quotes are present in values unless required. |
| **No traces appear in Langfuse** | Host or Key not recognized | Confirm nested keys contain double underscores (e.g. `LANGFUSE__PUBLIC_KEY`). Check if the host matches your cloud region. |
| **Redis connection fails** | REST client URL used instead of TCP | Make sure you copy the **TCP Redis URI** (`rediss://...`) from the Upstash console, not the REST API endpoint. |
| **Search queries return 0 matches** | OpenSearch index is empty | Ingestion has not run yet. Log in to Airflow ([http://localhost:8080](http://localhost:8080)) and trigger the `arxiv_paper_ingestion` DAG manually. |
| **OpenSearch container fails** | Insufficient RAM allocated | OpenSearch requires significant memory. Increase Docker Desktop memory allocation to at least 8GB. |
| **FastAPI container stays unhealthy**| Invalid configuration variables | Run `docker compose logs api` to locate configuration errors. Typically caused by a malformed Postgres or Redis connection URI. |

---

## 💰 Operational Cost Analysis
- **Local Services**: Free (FastAPI, OpenSearch, Airflow).
- **Neon Database**: Free (512MB storage allocation).
- **Upstash Redis**: Free (10,000 commands/day allocation).
- **Langfuse Cloud**: Free (50,000 trace events/month allocation).
- **Jina AI Embeddings**: Free tier contains generous token limits.
- **OpenAI API**: Pay-as-you-go.

---

## 🤝 Contributing
Issues and Pull Requests are welcome. For major architectural modifications, please open an issue first to discuss details.

---

## 📄 License
This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.