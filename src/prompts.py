"""
Prompts: LLM prompt templates for SQL generation, repair, answer formatting, and CSV fixing.
"""

from typing import Dict, List
from src.catalog import TableMetadata


def catalog_to_text(schema_subset: Dict[str, TableMetadata]) -> str:
    """Convert catalog metadata to readable text with columns and sample values."""
    lines = []
    
    for table_name, table_meta in sorted(schema_subset.items()):
        # Table header
        lines.append(f"Table: {table_name}")
        
        # Table description
        if table_meta.description:
            lines.append(f"  Description: {table_meta.description}")
        
        # Columns with sample values
        lines.append("  Columns:")
        for col in table_meta.columns:
            col_desc = f"    - {col.name} ({col.type})"
            if col.description:
                col_desc += f": {col.description}"
            
            # Include sample values if available
            if col.sample_values:
                # Show up to 3 sample values
                samples = [str(v) for v in col.sample_values[:3]]
                samples_str = ", ".join([f"'{s}'" for s in samples])
                col_desc += f" [Examples: {samples_str}]"
            
            lines.append(col_desc)
        
        lines.append("")  # Empty line between tables
    
    return "\n".join(lines)


def build_sql_generation_messages(
    question: str,
    schema_subset: Dict[str, TableMetadata]
) -> List[Dict]:
    """Build prompt messages for SQL generation from natural language question."""
    system_message = """You are an expert SQL query generator for DuckDB.
Your task is to convert natural language questions into valid DuckDB SQL queries.

Instructions:
- Use ONLY the tables and columns provided in the schema below
- Return ONE valid DuckDB SQL query
- Do NOT include explanations, markdown fences, or any other text
- Return just the raw SQL query that can be executed directly
- Use appropriate JOINs, aggregations, WHERE clauses, and GROUP BY as needed
- Handle date/time columns appropriately for DuckDB

CRITICAL - JOIN Type Matching:
- When joining tables, ensure the columns have COMPATIBLE TYPES
- Check the schema carefully - join INTEGER to INTEGER, STRING to STRING, etc.
- If types don't match, use explicit CAST() to convert them
- Example: If joining client_code (INTEGER) to client_code (STRING), use CAST(client_code AS INTEGER)
- DuckDB will fail if you try to join columns with incompatible types without casting

CRITICAL - Column Name Quoting:
- ALWAYS wrap column names that contain spaces in double quotes
- Example: "2024 Q4 Participation", "Brand Name", "Order Date"
- This is required for DuckDB to properly parse column names with spaces
- Table names do NOT need quotes unless they also contain spaces

IMPORTANT - Temporal Queries:
- When the question asks for "latest", "recent", "last quarter", or "most recent" data:
  * DO NOT hardcode specific quarters or dates (e.g., "2024 Q4")
  * Instead, query the data dynamically to find the actual latest values
  * Use MAX(), ORDER BY DESC with LIMIT, or subqueries to identify the most recent period
  * Example: If columns contain quarters like "2023 Q2", "2024 Q1", etc., use MAX(column_name) to find the latest
  * For date columns, use ORDER BY date_column DESC LIMIT N to get recent records
- Apply this principle to all time-based filters to ensure queries work with fresh data

IMPORTANT - Yearly Aggregations:
- When the question asks for yearly data (e.g., "in 2024", "for the year 2023", "yearly trends"):
  * FIRST, check if a yearly column exists (e.g., "2024", "year", "Year 2024")
  * If a yearly column exists, use it directly - this is preferred and simpler
  * ONLY if no yearly column exists, then COMBINE/SUM/AVG the quarterly or monthly columns
  * Example fallback: For "2024 data", combine "2024 Q1", "2024 Q2", "2024 Q3", "2024 Q4"
  * Use operations like: ("2024 Q1" + "2024 Q2" + "2024 Q3" + "2024 Q4") AS total_2024
  * Or use COALESCE to handle NULLs: COALESCE("2024 Q1", 0) + COALESCE("2024 Q2", 0) + ...
- Always prefer existing yearly columns over combining quarterly data"""

    # Convert schema to text
    schema_text = catalog_to_text(schema_subset)

    user_message = f"""Database Schema:

{schema_text}

Question: {question}

Generate the SQL query:"""

    return [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]


def build_sql_repair_messages(
    question: str,
    schema_subset: Dict[str, TableMetadata],
    broken_sql: str,
    error_message: str
) -> List[Dict]:
    """Build prompt messages for fixing broken SQL based on error message."""
    system_message = """You are an expert SQL debugger for BevCo Industries.
Your task is to fix broken DuckDB SQL queries based on error messages.

Instructions:
- Analyze the error message carefully
- Use ONLY the tables and columns provided in the schema
- Return ONE corrected SQL query
- Do NOT include explanations, markdown fences, or any other text
- Ensure the query is syntactically correct for DuckDB

CRITICAL - JOIN Type Matching:
- Type mismatch errors are common when joining tables
- Check the schema for column types in both tables being joined
- If types don't match, use explicit CAST() to convert them
- Example: CAST(column_name AS INTEGER) or CAST(column_name AS VARCHAR)
- Common conversions: INTEGER ↔ VARCHAR, BIGINT ↔ INTEGER, FLOAT ↔ INTEGER

CRITICAL - Column Name Quoting:
- ALWAYS wrap column names that contain spaces in double quotes
- Example: "2024 Q4 Participation", "Brand Name", "Order Date"
- This is the most common cause of SQL errors - missing quotes around column names with spaces
- If the error mentions a column name, check if it needs quotes

IMPORTANT - Temporal Queries:
- When fixing queries about "latest", "recent", or "last quarter" data:
  * DO NOT hardcode specific quarters or dates
  * Use MAX(), ORDER BY DESC with LIMIT, or subqueries to find the most recent period dynamically
  * Ensure the query will work with fresh data without modification

IMPORTANT - Yearly Aggregations:
- When fixing queries about yearly data (e.g., "in 2024", "for the year 2023"):
  * FIRST, check if a yearly column exists in the schema - if yes, use it (preferred)
  * ONLY if no yearly column exists, then COMBINE/SUM/AVG the quarterly or monthly columns
  * Example: For "2024", combine columns like "2024 Q1", "2024 Q2", "2024 Q3", "2024 Q4"
  * Use: ("2024 Q1" + "2024 Q2" + "2024 Q3" + "2024 Q4") AS total_2024
  * Or use COALESCE to handle NULLs: COALESCE("2024 Q1", 0) + COALESCE("2024 Q2", 0) + ...
- Prefer existing yearly columns over combining quarterly data"""

    # Convert schema to text
    schema_text = catalog_to_text(schema_subset)

    user_message = f"""Database Schema:

{schema_text}

Original Question: {question}

Failed SQL Query:
{broken_sql}

Error Message:
{error_message}

Fix the SQL query to resolve this error. Return only the corrected SQL query."""

    return [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]


def build_answer_formatting_messages(
    question: str,
    sql_query: str,
    query_results: List[Dict]
) -> List[Dict]:
    """Build prompt messages to format SQL results into natural language answer."""
    system_message = """You are a data analyst at BevCo Industries, providing clear answers to business questions.
Your task is to format SQL query results into natural language answers.

Instructions:
- Base your answer ONLY on the provided data - do not make up or hallucinate any numbers or facts
- Be concise and direct
- Include relevant numbers and metrics from the results
- If the results are empty, say so clearly
- Use business-appropriate language"""

    # Format results for readability
    if not query_results:
        results_text = "(No results returned)"
    else:
        # Show first few rows
        results_text = ""
        for i, row in enumerate(query_results[:10], 1):
            results_text += f"Row {i}: {row}\n"
        
        if len(query_results) > 10:
            results_text += f"\n... and {len(query_results) - 10} more rows"

    user_message = f"""Question: {question}

SQL Query Executed:
{sql_query}

Query Results:
{results_text}

Provide a clear, natural language answer based on these results:"""

    return [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]


def build_csv_fix_messages(
    header: List[str],
    data_rows: List[List[str]],
    annotation_data: dict
) -> List[dict]:
    """Build prompt messages for CSV column mismatch analysis and fixing."""
    expected_columns = [col["name"] for col in annotation_data.get("columns", [])]
    
    prompt = f"""You are a data quality expert. A CSV file has a column mismatch issue.

**Expected Schema from annotation.json:**
Table: {annotation_data.get('name')}
Description: {annotation_data.get('description')}
Expected columns ({len(expected_columns)}): {', '.join(expected_columns)}

**Actual CSV Structure:**
Header columns ({len(header)}): {', '.join(header)}
Data row columns: {len(data_rows[0])} columns

**Sample Data (first {len(data_rows)} rows):**
{chr(10).join([','.join(row) for row in data_rows])}

**Problem:**
The header has {len(header)} columns, but data rows have {len(data_rows[0])} columns.

**Task:**
Analyze the data and determine which column index (0-based) in the DATA ROWS should be removed to align with the header. Consider:
1. The expected schema from annotations
2. Empty or duplicate values
3. Data patterns in the last few columns
4. Which column removal would best match the expected schema

Respond with ONLY a JSON object (no markdown, no code blocks):
{{
  "column_index_to_remove": <number>,
  "reason": "<brief explanation>"
}}"""
    
    return [{"role": "user", "content": prompt}]

