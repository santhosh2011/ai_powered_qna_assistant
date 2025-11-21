"""
Data loader: Load CSV files into DuckDB with validation and auto-fixing.
"""

import csv
import json
import duckdb
from pathlib import Path
from typing import Tuple, Optional

from src.logging_config import get_logger

logger = get_logger(__name__)


class DataLoader:
    """CSV to DuckDB loader with validation and automatic fixing capabilities."""
    
    def __init__(
        self,
        db_path: str = "bevco.duckdb",
        data_dir: str = "data/dataframes",
        annotation_path: str = "data/annotation.json",
        auto_fix: bool = True
    ):
        """Initialize data loader with paths and configuration."""
        self.db_path = db_path
        self.data_dir = data_dir
        self.annotation_path = annotation_path
        self.auto_fix = auto_fix
        self.connection = None
    
    def _validate_csv_structure(self, csv_path: Path, check_rows: int = 10) -> Tuple[bool, Optional[str]]:
        """Check if CSV header and data rows have consistent column counts."""
        try:
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                
                try:
                    header = next(reader)
                except StopIteration:
                    return False, "CSV file is empty"
                
                header_count = len(header)
                
                for i, row in enumerate(reader, 1):
                    if i > check_rows:
                        break
                    
                    if len(row) != header_count:
                        return False, f"Column mismatch: header has {header_count} columns, but row {i} has {len(row)} columns"
                
                return True, None
                
        except Exception as e:
            return False, f"Error reading CSV: {str(e)}"
    
    def _fix_csv_with_llm(self, csv_path: Path, sample_rows: int = 5) -> bool:
        """Use LLM to analyze and fix CSV column mismatches (creates backup)."""
        try:
            from src.llm_client import LLMClient
            from src.prompts import build_csv_fix_messages
            
            logger.info("Attempting AI-powered CSV fix...")
            
            with open(self.annotation_path, 'r') as f:
                annotations = json.load(f)
            
            csv_filename = f"dataframes/{csv_path.name}"
            annotation_data = None
            for key, df_meta in annotations.get("dataframes", {}).items():
                if df_meta.get("path") == csv_filename:
                    annotation_data = df_meta
                    break
            
            if not annotation_data:
                logger.warning(f"No annotation found for {csv_filename}")
                return False
            
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)
                data_rows = [row for i, row in enumerate(reader) if i < sample_rows]
            
            if not data_rows:
                logger.warning("No data rows to analyze")
                return False
            
            # Use LLM to analyze the issue
            llm = LLMClient()
            messages = build_csv_fix_messages(header, data_rows, annotation_data)
            response = llm.chat(messages, temperature=0.2)
            
            # Parse JSON response
            result = json.loads(response.strip())
            col_to_remove = result.get("column_index_to_remove")
            reason = result.get("reason")
            
            if col_to_remove is None:
                logger.error("LLM response missing column_index_to_remove")
                return False
            
            logger.info(f"LLM Decision: Remove column {col_to_remove} - Reason: {reason}")
            
            # Create backup and fix
            backup_path = csv_path.with_suffix('.csv.backup')
            csv_path.rename(backup_path)
            logger.info(f"Created backup: {backup_path.name}")
            
            with open(backup_path, 'r') as fin, open(csv_path, 'w', newline='') as fout:
                reader = csv.reader(fin)
                writer = csv.writer(fout)
                
                header = next(reader)
                writer.writerow(header)
                
                fixed_count = 0
                for row in reader:
                    if len(row) > len(header):
                        fixed_row = row[:col_to_remove] + row[col_to_remove+1:]
                        writer.writerow(fixed_row)
                        fixed_count += 1
                    else:
                        writer.writerow(row)
                
            logger.info(f"Fixed {fixed_count} rows, saved to {csv_path.name}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return False
        except Exception as e:
            logger.error(f"Error during CSV fix: {str(e)}")
            return False
    
    def load(self) -> duckdb.DuckDBPyConnection:
        """Load all CSV files into DuckDB with validation and auto-fixing."""
        self.connection = duckdb.connect(self.db_path)
        
        data_path = Path(self.data_dir)
        if not data_path.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")
        
        csv_files = sorted(data_path.glob("*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {self.data_dir}")
            return self.connection
        
        logger.info(f"Found {len(csv_files)} CSV file(s) in {self.data_dir}")
        
        loaded_count = 0
        skipped_count = 0
        
        for idx, csv_file in enumerate(csv_files, 1):
            table_name = csv_file.stem
            logger.info(f"[{idx}/{len(csv_files)}] Validating '{table_name}'...")
            
            is_valid, error_msg = self._validate_csv_structure(csv_file)
            
            if not is_valid:
                logger.error(f"CSV validation failed for '{table_name}': {error_msg}")
                
                if self.auto_fix:
                    fix_success = self._fix_csv_with_llm(csv_file)
                    
                    if fix_success:
                        is_valid, error_msg = self._validate_csv_structure(csv_file)
                        if is_valid:
                            logger.info(f"CSV fixed successfully, now loading '{table_name}'")
                        else:
                            logger.error(f"Fix failed validation: {error_msg}")
                            skipped_count += 1
                            continue
                    else:
                        logger.error(f"Could not auto-fix '{table_name}', skipping")
                        skipped_count += 1
                        continue
                else:
                    skipped_count += 1
                    continue
            
            csv_path_str = str(csv_file.absolute())
            
            try:
                self.connection.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto('{csv_path_str}', header=true)
                """)
                
                row_count = self.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                logger.info(f"Loaded '{table_name}' successfully ({row_count} rows)")
                loaded_count += 1
                
            except Exception as e:
                logger.error(f"Failed to load '{table_name}': {str(e)}")
                skipped_count += 1
        
        logger.info(f"Summary: {loaded_count} tables loaded, {skipped_count} skipped")
        return self.connection


# Backward compatibility function
def init_duckdb(
    db_path: str = "bevco.duckdb",
    data_dir: str = "data/dataframes",
    annotation_path: str = "data/annotation.json",
    auto_fix: bool = True
) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB, load all CSVs as tables with validation and auto-fix.
    
    Note: This is a compatibility wrapper. Use DataLoader class for new code.
    """
    loader = DataLoader(db_path, data_dir, annotation_path, auto_fix)
    return loader.load()
