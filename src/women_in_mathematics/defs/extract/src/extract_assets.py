"""
Dagster assets for extracting text from PDFs.
"""
import subprocess
from pathlib import Path

import dagster as dg
from pyprojroot.here import here


@dg.asset(
    deps=["split_pdfs"],
    code_version="v1",
    description="Extract text from individual PDFs using pdf2txt.py"
)
def extract_text() -> dg.MaterializeResult:
    """
    Extract text from individual PDFs.

    Depends on split_pdfs asset.
    Returns metadata about the number of text files created.
    """
    input_folder = here("src/women_in_mathematics/defs/split/output/")
    output_folder = here("src/women_in_mathematics/defs/extract/output/")

    output_folder.mkdir(parents=True, exist_ok=True)

    pdf_files = list(Path(input_folder).glob("*.pdf"))
    texts_created = 0

    for pdf_file in pdf_files:
        output_file = output_folder / f"{pdf_file.name}.txt"

        try:
            # Run pdf2txt.py command
            subprocess.run(
                ["pdf2txt.py", str(pdf_file)],
                stdout=open(output_file, "w"),
                check=True,
                stderr=subprocess.PIPE
            )
            texts_created += 1
        except subprocess.CalledProcessError as e:
            dg.get_dagster_logger().warning(f"Failed to extract text from {pdf_file.name}: {e}")
        except FileNotFoundError:
            raise RuntimeError(
                "pdf2txt.py not found. Install pdfminer.six: pip install pdfminer.six"
            )

    return dg.MaterializeResult(
        metadata={
            "input_from": "defs/split/output/",
            "output_to": "defs/extract/output/",
            "num_texts_created": texts_created,
            "num_pdfs_processed": len(pdf_files),
        }
    )