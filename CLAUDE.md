# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project processes biographical data about pioneering women in mathematics from a PDF supplement. The workflow extracts, parses, and structures biographical information into JSON format using a Dagster-based pipeline architecture.

## Technology Stack

- **Build Tool**: `uv` (preferred) or `pip` for dependency management
- **Framework**: Dagster 1.12.2 for data orchestration
- **Python Version**: 3.10-3.13 (specified in pyproject.toml)
- **CLI**: `dg` (dagster-dg-cli) for development commands

## Setup and Development

### Environment Setup

Using uv (recommended):
```bash
uv sync
source .venv/bin/activate  # macOS
```

Using pip:
```bash
python3 -m venv .venv
source .venv/bin/activate  # macOS
pip install -e ".[dev]"
```

### Running the Application

**Using the Dagster UI:**
```bash
dg dev
```
Access the UI at http://localhost:3000

**Using the CLI:**
```bash
# Set OpenAI API key (only needed if outputs don't exist)
export OPENAI_API_KEY="your-key-here"

# List all assets
dagster asset list -m women_in_mathematics -a defs

# Materialize all assets in the pipeline
dagster asset materialize -m women_in_mathematics -a defs --select "*"

# Materialize a specific asset and all its dependencies
dagster asset materialize -m women_in_mathematics -a defs --select join_to_csv

# Materialize a single asset only
dagster asset materialize -m women_in_mathematics -a defs --select split_pdfs
```

## Architecture

### Dagster Assets Pipeline

The project uses Dagster's asset-based pipeline architecture. Assets are organized in nested directories under `src/women_in_mathematics/defs/`:

**Asset Files:**
- `defs/split/src/split_assets.py` - Defines `split_pdfs` asset
- `defs/extract/src/extract_assets.py` - Defines `extract_text` asset (includes `data_exists_check` asset check)
- `defs/parse/src/parse_assets.py` - Defines `parse_biographies` asset (includes `data_exists_check` asset check)
- `defs/join/src/join_assets.py` - Defines `join_to_csv` asset
- `defs/adapter/src/upload_storywrangler.py` - Defines `adapter` asset (not yet implemented)

**Resource Files:**
- `defs/resources.py` - Defines OpenAI resource using `@dg.definitions` pattern

**Definitions Loading:**
- `definitions.py` uses `load_from_defs_folder()` which automatically discovers all `@dg.asset` and `@dg.definitions` decorated functions in the `defs/` directory tree
- This pattern allows modular organization where each stage has its own directory with input/output/src subdirectories

**Pipeline Flow:**

1. **split_pdfs** → Splits the source PDF (`PioneeringWomenSupplement.pdf`) into individual PDFs per woman using bookmarks
   - Input: `defs/split/input/PioneeringWomenSupplement.pdf` (from Internet Archive)
   - Output: Individual PDFs in `defs/split/output/` named as `lastname_firstname.pdf`
   - Uses PyPDF2 and PyMuPDF (fitz) to extract bookmarks and split pages
   - Skips pages before page 6 and single-character titles

2. **extract_text** → Extracts text from individual PDFs using pdf2txt.py
   - Depends on: `split_pdfs`
   - Input: PDFs from `defs/split/output/`
   - Output: Text files in `defs/extract/output/` as `lastname_firstname.pdf.txt`
   - Requires: `pdfminer.six` package for pdf2txt.py command
   - Includes asset check to verify all PDFs were converted to text files

3. **parse_biographies** → Uses OpenAI GPT-4o to extract structured biographical data
   - Depends on: `extract_text`
   - Input: Text files from `defs/extract/output/`
   - Output: JSON files in `defs/parse/output/` with structured biographical data
   - Requires: `OPENAI_API_KEY` environment variable
   - Uses `json_repair` to handle malformed JSON responses
   - Temperature set to 0.3 for deterministic outputs
   - Smart caching: Checks if output JSON files already exist and skips GPT-4 calls to save API costs
   - Includes asset check to verify all text files were converted to JSON

4. **join_to_csv** → Combines all JSON files into normalized CSV tables
   - Depends on: `parse_biographies`
   - Input: JSON files from `defs/parse/output/`
   - Output: 6 CSV files in `defs/join/output/`: personal.csv, degrees.csv, employment.csv, visits.csv, honors.csv, parents.csv
   - Uses dateparser to extract years from date strings

### Dagster Configuration

- **Root Module**: `women_in_mathematics`
- **Definitions**: Located in `src/women_in_mathematics/definitions.py`
- **Pattern**: Uses `load_from_defs_folder()` which automatically discovers all decorated assets and resources in subdirectories
- **Resources**: OpenAI client configured in `defs/resources.py` with `@dg.definitions` pattern
- **Asset Checks**: Some assets include `@dg.asset_check` decorators to validate data integrity between pipeline stages

### Data Schema

The parsed biographical JSON follows this structure:

**Parent Object:**
- name, birthdate, deathdate, profession

**Employment Object:**
- employer, job_title, job_year_begin, job_year_end, reason_end

**Degree Object:**
- degree_institution_name, degree_type (BA, MA, PhD), degree_year, degree_advisor

**Visit Object:**
- visit_location, visit_reason, visit_year

**Honors Object:**
- honor_name, honor_year

## File Organization

- `src/women_in_mathematics/` - Main package directory
- `src/women_in_mathematics/definitions.py` - Dagster definitions entry point using `load_from_defs_folder()`
- `src/women_in_mathematics/defs/` - Directory containing all assets, resources, and data
  - `defs/resources.py` - OpenAI resource definition
  - `defs/split/` - PDF splitting stage
    - `input/` - Source PDF (PioneeringWomenSupplement.pdf)
    - `output/` - Individual PDFs per woman
    - `src/split_assets.py` - Asset definition
  - `defs/extract/` - Text extraction stage
    - `output/` - Text files extracted from PDFs
    - `src/extract_assets.py` - Asset definition with data quality check
  - `defs/parse/` - Biographical parsing stage
    - `output/` - Structured JSON files
    - `src/parse_assets.py` - Asset definition with data quality check
  - `defs/join/` - CSV normalization stage
    - `output/` - Final CSV tables (personal, degrees, employment, visits, honors, parents)
    - `src/join_assets.py` - Asset definition
  - `defs/adapter/` - Data export stage (future)
    - `src/upload_storywrangler.py` - Asset definition (not yet implemented)

## Important Notes

### Pipeline Execution
- Run the entire pipeline by materializing assets in the Dagster UI (http://localhost:3000)
- Assets have clear dependencies: split_pdfs → extract_text → parse_biographies → join_to_csv
- Each asset returns MaterializeResult with metadata about what was processed

### Asset Versioning and Caching
- All assets have `code_version="v1"` which enables automatic caching
- Dagster tracks code versions + upstream data versions to detect if rematerialization is needed
- If nothing changed, Dagster skips recomputation and returns cached results (saves GPT-4 API calls!)
- When you modify asset logic, increment the version (e.g., "v2") to force rematerialization
- The UI will show "Unsynced" status when code version mismatches require updates

### Environment Variables
- `OPENAI_API_KEY` - Required for the parse_biographies asset to call GPT-4o

### Technical Details
- The project uses `pyprojroot.here()` for cross-platform path resolution relative to project root
- The split stage skips pages before page 6 and single-character titles (metadata pages)
- Name formatting normalizes to lowercase `lastname_firstname` pattern
- The parse stage uses `json_repair` to handle malformed JSON responses from GPT-4o
- Temperature is set to 0.3 for more deterministic LLM outputs
- The parse stage implements file-level caching by checking if output JSON already exists before making GPT-4 API calls
- Asset checks validate data consistency between pipeline stages (e.g., ensuring no files are lost during conversion)
- The source PDF comes from Internet Archive: https://web.archive.org/web/20150608005556/http://www.ams.org/publications/authors/books/postpub/hmath-34-PioneeringWomen.pdf
