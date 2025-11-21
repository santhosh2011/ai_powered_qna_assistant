"""
QA Engine: Complete pipeline from natural language questions to SQL to answers.
"""

import re
import duckdb
import pandas as pd
from typing import Dict, Set, Tuple, Optional
from src.catalog import TableMetadata, CatalogBuilder
from src.llm_client import LLMClient
from src.prompts import (
    build_sql_generation_messages,
    build_sql_repair_messages,
    build_answer_formatting_messages
)
from src.logging_config import get_logger

logger = get_logger(__name__)


class QAEngine:
    """Natural language to SQL QA engine with automatic error repair."""
    
    def __init__(
        self,
        db_path: str = "bevco.duckdb",
        annotation_path: str = "data/annotation.json",
        llm_model: str = "gpt-4o-mini"
    ):
        """Initialize QA engine with DuckDB connection, catalog, and LLM client."""
        self.connection = duckdb.connect(db_path)
        
        catalog_builder = CatalogBuilder(self.connection, annotation_path, include_samples=True)
        self.catalog = catalog_builder.build()
        
        self.llm = LLMClient(model=llm_model)
        self._stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'is', 'are', 'was', 'were', 'what', 'how',
            'when', 'where', 'who', 'which', 'do', 'does', 'did', 'can', 'could',
            'would', 'should', 'will', 'have', 'has', 'had'
        }
    
    def _select_relevant_schema(self, question: str) -> Dict[str, TableMetadata]:
        """Filter catalog to tables relevant to question using keyword matching."""
        if len(self.catalog) <= 5:
            return self.catalog
        
        question_lower = question.lower()
        question_words = set(re.findall(r'\b\w+\b', question_lower)) - self._stop_words
        
        relevant_tables = {}
        for table_name, table_meta in self.catalog.items():
            table_name_words = set(re.findall(r'\b\w+\b', table_name.lower()))
            
            description_words: Set[str] = set()
            if table_meta.description:
                description_words = set(re.findall(r'\b\w+\b', table_meta.description.lower()))
            
            if question_words & (table_name_words | description_words):
                relevant_tables[table_name] = table_meta

        logger.debug(f"Relevant tables: {relevant_tables}")
        return relevant_tables if relevant_tables else self.catalog
    
    def _clean_sql(self, sql: str) -> str:
        """Remove markdown fences and whitespace from SQL string."""
        sql = sql.strip()
        
        if sql.startswith('```'):
            lines = sql.split('\n')
            if lines[0].startswith('```'):
                lines = lines[1:]
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]
            sql = '\n'.join(lines)
        return sql.strip()
    
    def _generate_sql(self, question: str) -> str:
        """Generate SQL from natural language question using LLM."""
        schema_subset = self._select_relevant_schema(question)
        messages = build_sql_generation_messages(question, schema_subset)
        raw_sql = self.llm.chat(messages)
        return self._clean_sql(raw_sql)
    
    def _repair_sql(self, question: str, previous_sql: str, error_message: str) -> str:
        """Fix broken SQL query using LLM based on error message."""
        schema_subset = self._select_relevant_schema(question)
        messages = build_sql_repair_messages(
            question=question,
            schema_subset=schema_subset,
            broken_sql=previous_sql,
            error_message=error_message
        )
        raw_sql = self.llm.chat(messages)
        return self._clean_sql(raw_sql)
    
    def _run_sql(self, sql: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Execute SQL and return (DataFrame, None) on success or (None, error) on failure."""
        try:
            result = self.connection.execute(sql)
            return (result.df(), None)
        except Exception as e:
            return (None, str(e))

    def answer(self, question: str) -> dict:
        """
        Answer a natural language question.
        
        Generates SQL, executes it (with up to 3 repair attempts on errors),
        and returns a dict with question, sql, rows, and natural language answer.
        """
        sql = self._generate_sql(question)
        df, error = self._run_sql(sql)
        
        # Try to repair up to 3 times on error
        max_retries = 3
        retry_count = 0
        
        while error is not None and retry_count < max_retries:
            retry_count += 1
            logger.warning(f"SQL failed (attempt {retry_count}/{max_retries}): {error}")
            logger.info("Attempting to repair SQL...")
            
            sql = self._repair_sql(question, sql, error)
            df, error = self._run_sql(sql)
        
        # Return failure response if all retries exhausted
        if error is not None:
            logger.error(f"All repair attempts failed. Final error: {error}")
            return {
                "question": question,
                "sql": sql,
                "rows": [],
                "answer": f"Failed to construct a valid query after {max_retries} repair attempts. Last error: {error}"
            }
        
        # Process successful results
        logger.info(f"SQL executed successfully, got {len(df)} rows")
        
        preview_df = df.head(50)
        rows = preview_df.to_dict(orient="records")
        
        messages = build_answer_formatting_messages(
            question=question,
            sql_query=sql,
            query_results=rows
        )
        
        answer_text = self.llm.chat(messages)
        
        return {
            "question": question,
            "sql": sql,
            "rows": rows,
            "answer": answer_text
        }

