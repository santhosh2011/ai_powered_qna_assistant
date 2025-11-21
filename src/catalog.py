"""
Catalog module: Build and manage database schema metadata.

Loads annotations, introspects DuckDB tables, and creates structured
catalog with column descriptions and sample values.
"""

import json
import duckdb
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Dict

from src.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TableColumn:
    """Column metadata with name, type, description, and sample values."""
    name: str
    type: str
    description: Optional[str] = None
    sample_values: Optional[List[str]] = None


@dataclass
class TableMetadata:
    """Table metadata with name, description, and columns."""
    name: str
    description: Optional[str]
    columns: List[TableColumn]


class CatalogBuilder:
    """Build database catalog by merging DuckDB schema with annotations."""
    
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        annotation_path: str = "data/annotation.json",
        include_samples: bool = True
    ):
        """Initialize catalog builder with database connection and configuration."""
        self.connection = conn
        self.annotation_path = annotation_path
        self.include_samples = include_samples
        self.annotations = None
        self.annotation_map = {}
    
    def _load_annotations(self) -> Dict:
        """Load annotation.json and return as dictionary."""
        with open(self.annotation_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _extract_table_name_from_path(self, path: str) -> str:
        """Extract table name from CSV path (e.g., 'dataframes/B2B_weather.csv' -> 'B2B_weather')."""
        return Path(path).stem
    
    def _get_duckdb_schema(self, table_name: str) -> List[tuple]:
        """Get column names and types from DuckDB table."""
        result = self.connection.execute(f"DESCRIBE {table_name}").fetchall()
        return [(row[0], row[1]) for row in result]
    
    def _get_sample_values(self, table_name: str, column_name: str, limit: int = 5) -> List[str]:
        """Get sample distinct values from a column (returns empty list on error)."""
        try:
            query = f'SELECT DISTINCT "{column_name}" FROM {table_name} WHERE "{column_name}" IS NOT NULL LIMIT {limit}'
            result = self.connection.execute(query).fetchall()
            return [str(row[0]) for row in result]
        except Exception as e:
            logger.warning(f"Could not get sample values for {table_name}.{column_name}: {e}")
            return []
    
    def _build_annotation_map(self) -> None:
        """Create mapping from table name to annotation data."""
        self.annotation_map = {}
        for key, df_meta in self.annotations.get("dataframes", {}).items():
            table_name = self._extract_table_name_from_path(df_meta["path"])
            self.annotation_map[table_name] = df_meta
    
    def _build_table_metadata(self, table_name: str) -> TableMetadata:
        """Build metadata for a single table."""
        logger.debug(f"Processing table: {table_name}")
        
        db_columns = self._get_duckdb_schema(table_name)
        
        annotation_data = self.annotation_map.get(table_name, {})
        if not annotation_data:
            logger.warning(f"No annotation found for table '{table_name}'")
        
        table_description = annotation_data.get("description")
        
        # Create column annotation lookup
        column_annotations = {}
        for col_meta in annotation_data.get("columns", []):
            column_annotations[col_meta["name"]] = col_meta
        
        # Build column metadata
        columns = []
        for col_name, col_type in db_columns:
            col_annotation = column_annotations.get(col_name, {})
            
            sample_values = None
            if self.include_samples:
                sample_values = self._get_sample_values(table_name, col_name)
            
            column = TableColumn(
                name=col_name,
                type=col_type,
                description=col_annotation.get("description"),
                sample_values=sample_values
            )
            columns.append(column)
        
        return TableMetadata(
            name=table_name,
            description=table_description,
            columns=columns
        )
    
    def build(self) -> Dict[str, TableMetadata]:
        """Build catalog by reading DuckDB schema and merging with annotations."""
        logger.info("Building catalog...")
        
        self.annotations = self._load_annotations()
        self._build_annotation_map()
        
        tables = self.connection.execute("SHOW TABLES").fetchall()
        table_names = [row[0] for row in tables]
        
        catalog = {}
        for table_name in table_names:
            catalog[table_name] = self._build_table_metadata(table_name)
        
        logger.info(f"Catalog built with {len(catalog)} tables")
        return catalog
    
    @staticmethod
    def save_to_json(catalog: Dict[str, TableMetadata], path: str = "catalog.json") -> None:
        """Save catalog to JSON file for debugging."""
        catalog_dict = {}
        
        for table_name, table_meta in catalog.items():
            columns_list = [
                {
                    "name": col.name,
                    "type": col.type,
                    "description": col.description,
                    "sample_values": col.sample_values
                }
                for col in table_meta.columns
            ]
            
            catalog_dict[table_name] = {
                "name": table_meta.name,
                "description": table_meta.description,
                "columns": columns_list
            }
        
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(catalog_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Catalog saved to {path}")

