"""
BevCo Intelligence CLI

Command-line interface for querying BevCo data using natural language.
"""

import typer
import os
from rich.console import Console
from rich.table import Table
from src.qa_engine import QAEngine
from src.data_loader import init_duckdb
from src.catalog import CatalogBuilder
from src.logging_config import setup_logging

app = typer.Typer(help="BevCo Intelligence - Natural Language Data Queries")
console = Console()

# Initialize logging on module import
setup_logging(level="INFO")


def _display_result(result: dict, debug: bool = False):
    """Helper function to display query results"""
    # Always print question and answer
    console.print(f"[bold yellow]🤔 Question:[/bold yellow] {result['question']}\n")
    console.print(f"[bold green]💡 Answer:[/bold green]\n{result['answer']}\n")
    
    # If debug mode, show SQL and result preview
    if debug:
        console.print(f"[bold blue]🔍 SQL:[/bold blue]")
        console.print(f"[dim]{result['sql']}[/dim]\n")
        
        # Show result preview as rich table
        if result['rows']:
            console.print(f"[bold magenta]📊 Result Preview:[/bold magenta] (showing {len(result['rows'])} rows)\n")
            # Create rich table
            table = Table(show_header=True, header_style="bold magenta")
            # Add columns from first row
            if result['rows']:
                for col_name in result['rows'][0].keys():
                    table.add_column(col_name)
                # Add rows (limit to 10 for display)
                for row in result['rows'][:10]:
                    table.add_row(*[str(v) for v in row.values()])
                console.print(table)
                if len(result['rows']) > 10:
                    console.print(f"\n[dim]... and {len(result['rows']) - 10} more rows[/dim]\n")
        else:
            console.print("[dim]📊 No results returned[/dim]\n")


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural language question about BevCo data"),
    debug: bool = typer.Option(False, "--debug", help="Show generated SQL and result preview")
):
    """
    Ask a single natural language question about BevCo data.
    
    Examples:
        python main.py ask "What are the top 5 brands by participation?"
        python main.py ask "Show me the latest weather data" --debug
    """
    try:
        # Initialize QA Engine
        console.print("\n[bold cyan]Initializing BevCo Intelligence...[/bold cyan]")
        engine = QAEngine()
        # Get answer
        console.print("[bold cyan]Processing your question...[/bold cyan]\n")
        result = engine.answer(question)
        
        # Display result
        _display_result(result, debug)
    
    except Exception as e:
        console.print(f"[bold red]❌ Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)


@app.command()
def chat(
    debug: bool = typer.Option(False, "--debug", help="Show generated SQL and result preview")
):
    """
    Start an interactive chat session to ask multiple questions.
    The engine loads once and you can ask unlimited questions until you type 'exit' or 'quit'.
    
    Examples:
        python main.py chat
        python main.py chat --debug
    """
    try:
        # Initialize QA Engine once
        console.print("\n[bold cyan]🚀 Initializing BevCo Intelligence...[/bold cyan]")
        engine = QAEngine()
        console.print("[bold green]✓ Ready! You can now ask questions.[/bold green]")
        console.print("[dim]Type 'exit' or 'quit' to end the session.[/dim]\n")
        
        # Interactive loop
        while True:
            # Prompt for question
            question = console.input("[bold cyan]❓ Your question:[/bold cyan] ")
            # Check for exit commands
            if question.lower().strip() in ['exit', 'quit', 'q']:
                console.print("\n[bold cyan]👋 Goodbye![/bold cyan]\n")
                break
            # Skip empty questions
            if not question.strip():
                continue
            try:
                # Get answer
                console.print("[dim]Processing...[/dim]\n")
                result = engine.answer(question)
                # Display result
                _display_result(result, debug)
                console.print("[dim]" + "─" * 80 + "[/dim]\n")
            except Exception as e:
                console.print(f"[bold red]❌ Error:[/bold red] {str(e)}\n")
                console.print("[dim]You can ask another question or type 'exit' to quit.[/dim]\n")
    
    except KeyboardInterrupt:
        console.print("\n\n[bold cyan]👋 Session ended. Goodbye![/bold cyan]\n")
    except Exception as e:
        console.print(f"[bold red]❌ Fatal Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)


@app.command()
def refresh():
    """
    Reload database from CSV files and rebuild catalog with latest annotations.
    
    This command will:
    1. Delete the existing database file (bevco.duckdb)
    2. Load all CSV files fresh from data/dataframes/
    3. Build catalog with data/annotation.json
    4. Save catalog.json
    
    Example:
        python main.py refresh
    """
    # Fixed paths
    db_path = "bevco.duckdb"
    data_dir = "data/dataframes"
    annotation_path = "data/annotation.json"
    catalog_output = "catalog.json"
    
    try:
        console.print("\n[bold cyan]🔄 Refreshing BevCo Intelligence Database...[/bold cyan]\n")
        
        # Step 1: Delete old database file if it exists
        if os.path.exists(db_path):
            console.print(f"[dim]Deleting old database: {db_path}[/dim]")
            os.remove(db_path)
            # Also remove WAL file if exists
            wal_path = f"{db_path}.wal"
            if os.path.exists(wal_path):
                os.remove(wal_path)
        
        # Step 2: Load CSV files into DuckDB with auto-fix enabled
        console.print(f"[bold cyan]📂 Loading CSV files from {data_dir}...[/bold cyan]")
        conn = init_duckdb(db_path=db_path, data_dir=data_dir, annotation_path=annotation_path, auto_fix=True)
        
        # Step 3: Build catalog with annotations
        console.print(f"\n[bold cyan]📋 Building catalog with annotations...[/bold cyan]")
        catalog_builder = CatalogBuilder(conn, annotation_path=annotation_path, include_samples=True)
        catalog = catalog_builder.build()
        
        # Step 4: Save catalog to JSON
        console.print(f"\n[bold cyan]💾 Saving catalog to {catalog_output}...[/bold cyan]")
        CatalogBuilder.save_to_json(catalog, catalog_output)
        
        # Summary
        console.print(f"\n[bold green]✓ Database refresh complete![/bold green]")
        console.print(f"[dim]Database: {db_path}[/dim]")
        console.print(f"[dim]Catalog: {catalog_output}[/dim]")
        console.print(f"[dim]Tables loaded: {len(catalog)}[/dim]\n")
        
        # Close connection
        conn.close()
        
    except Exception as e:
        console.print(f"[bold red]❌ Error during refresh:[/bold red] {str(e)}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
