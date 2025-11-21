# BevCo Intelligence

A natural language query interface for business data. Ask questions in plain English and get SQL-powered insights automatically.

## Quick Setup

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) package manager (install: `curl -LsSf https://astral.sh/uv/install.sh | sh`)
- OpenAI API key

### Installation

1. **Set up your OpenAI API key**

Create a `.env` file in the project root:

```bash
OPENAI_API_KEY=your_api_key_here
```

2. **Install dependencies**

```bash
uv sync
```

3. **Load the data**

```bash
uv run python -m main refresh
```

This will load all CSV files from `data/dataframes/` into DuckDB and build the schema catalog.

## Testing the Application

### Interactive Chat Mode

Start a conversation with your data:

```bash
uv run python -m main chat
```

**Try these example questions:**

```
❓ What are the top 5 brands by participation in 2024 Q4?
❓ Show me total volume sold by operation type
❓ Which products had the highest revenue in October 2024?
❓ What is the average order value for DTC orders?
❓ Compare brand power between PopMax and ZestDrink
```

Type `exit` or `quit` to end the session.

### Debug Mode

See the generated SQL queries and execution details:

```bash
uv run python -m main chat --debug
```

This shows:
- The SQL query generated from your question
- Raw query results
- How the AI formulates the natural language answer

### Single Question Mode

Ask a single question:

```bash
uv run python -m main ask "What are the top 3 products by volume?"
```

Add `--debug` to see the SQL:

```bash
uv run python -m main ask "What are the top 3 products by volume?" --debug
```

## How It Works

### High-Level Flow

```
User Question → LLM generates SQL → Execute on DuckDB → LLM formats answer → Response
```

### The `refresh` Command

When you run `uv run python -m main refresh`:

1. **Scans** `data/dataframes/` for CSV files
2. **Validates** CSV structure (checks column counts match headers)
3. **Auto-fixes** any mismatches using AI (creates backups)
4. **Loads** all CSVs into DuckDB tables
5. **Builds catalog** by merging:
   - Schema info from DuckDB (column names, types)
   - Business context from `data/annotation.json` (descriptions)
   - Sample values from actual data
6. **Saves** `catalog.json` for the query engine

### The Query Pipeline

When you ask a question:

1. **Context Building**: Loads relevant table schemas from catalog
2. **SQL Generation**: LLM converts question to SQL using schema context
3. **Execution**: Runs SQL on DuckDB
4. **Auto-Repair**: If query fails, LLM analyzes error and retries (up to 3 attempts)
5. **Answer Formatting**: LLM converts results into natural language

### Key Features

- ✅ **Schema-aware**: Uses table/column descriptions for accurate SQL
- ✅ **Self-healing**: Automatically fixes broken queries
- ✅ **Context-rich**: Includes sample values to help with data type inference
- ✅ **Production-ready**: Structured logging, error handling, OOP architecture

## Project Structure

```
.
├── main.py                 # CLI entry point (Typer commands)
├── src/
│   ├── catalog.py          # CatalogBuilder: schema introspection
│   ├── data_loader.py      # DataLoader: CSV validation & loading
│   ├── llm_client.py       # LLMClient: OpenAI API wrapper
│   ├── qa_engine.py        # QAEngine: main query pipeline
│   ├── prompts.py          # LLM prompt templates
│   └── logging_config.py   # Centralized logging
├── data/
│   ├── annotation.json     # Schema descriptions & metadata
│   └── dataframes/         # CSV data files
├── bevco.duckdb           # DuckDB database (generated)
└── catalog.json           # Schema catalog (generated)
```

## Architecture Highlights

### Object-Oriented Design
All core functionality is encapsulated in classes:
- `DataLoader`: Handles CSV validation and DuckDB loading
- `CatalogBuilder`: Introspects schema and builds metadata catalog
- `QAEngine`: Orchestrates the question → SQL → answer pipeline
- `LLMClient`: Simple wrapper around OpenAI API

### Separation of Concerns
- Prompts isolated in `prompts.py`
- Logging configured centrally in `logging_config.py`
- Each class has a single, focused responsibility

### Error Handling
- CSV structure validation with AI-powered auto-fix
- SQL query repair with error message context (up to 3 retries)
- Graceful fallbacks and informative error messages
- Structured logging throughout

## Sample Data

The project includes sample BevCo business data:

- **B2B Sales**: Client data, product data, sales transactions
- **B2B Operations**: Stockout records, weather data
- **DTC (Direct-to-Consumer)**: Orders, coupons, loyalty program
- **Marketing**: Brand power, occasions, participation, servings

## Notes

- The system uses DuckDB (embedded SQL database) for fast analytics
- All data stays local - no data sent to external services except LLM prompts
- CSV files must have headers matching the schema in `annotation.json`
- The AI auto-fix feature can handle common CSV structure issues
