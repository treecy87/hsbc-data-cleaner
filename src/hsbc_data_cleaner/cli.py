"""Command-line entry point for the HSBC PDF cleaning toolkit."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .config import AppConfig, load_app_config
from .orchestrator import run_cleaning, upload_chunks
from .utils.logging import setup_logging

app = typer.Typer(help="HSBC fund PDF cleaning utilities")


def _load_settings(config_path: Optional[Path]) -> AppConfig:
    return load_app_config(config_path)


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        help="Path to a TOML configuration file.",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose/--no-verbose",
        help="Enable verbose logging.",
    ),
) -> None:
    """Set up shared context before executing commands."""

    settings = _load_settings(config)
    log_level = 10 if verbose else 20
    setup_logging(level=log_level, log_file=settings.log_dir / "hsbc_data_cleaner.log")
    ctx.obj = {
        "settings": settings,
    }


@app.command()
def clean(
    ctx: typer.Context,
    quarter: str = typer.Option(..., help="Quarter to process, e.g. 2025Q2."),
    fund_code: Optional[str] = typer.Option(
        None,
        help="Limit processing to a single fund code.",
    ),
    input_dir: Optional[Path] = typer.Option(
        None,
        help="Override the input directory for raw PDFs.",
    ),
    chunks_dir: Optional[Path] = typer.Option(
        None,
        help="Override the output directory for text chunks.",
    ),
    incremental: bool = typer.Option(
        True,
        "--incremental/--no-incremental",
        help="Process only files that changed since the last run.",
    ),
    upload: bool = typer.Option(
        False,
        help="Upload generated chunks to the configured Drive folder after cleaning.",
    ),
) -> None:
    """Run the cleaning pipeline for a specific quarter."""

    settings: AppConfig = ctx.obj["settings"]
    run_cleaning(
        settings=settings,
        quarter=quarter,
        fund_code=fund_code,
        input_dir=input_dir,
        chunks_dir=chunks_dir,
        incremental=incremental,
        upload=upload,
    )


@app.command()
def upload(
    ctx: typer.Context,
    quarter: str = typer.Option(..., help="Quarter whose chunks should be uploaded."),
    chunks_dir: Optional[Path] = typer.Option(
        None,
        help="Override the chunk directory to upload from.",
    ),
    drive_folder: Optional[str] = typer.Option(
        None,
        help="Override the target Drive folder ID.",
    ),
) -> None:
    """Upload previously generated chunks to Google Drive."""

    settings: AppConfig = ctx.obj["settings"]
    upload_chunks(
        settings=settings,
        quarter=quarter,
        chunks_dir=chunks_dir,
        drive_folder_id=drive_folder,
    )


if __name__ == "__main__":  # pragma: no cover
    app()
