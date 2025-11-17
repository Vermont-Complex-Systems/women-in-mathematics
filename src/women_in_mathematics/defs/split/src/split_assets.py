"""
Dagster assets for splitting the PDF by bookmarks.
"""
import os
import re
from pathlib import Path
from typing import List, Tuple

import dagster as dg
import fitz
import PyPDF2
from pyprojroot.here import here


def format_name(name: str) -> str:
    """Format bookmark name to filename."""
    # Remove content in parentheses
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r"'", '', name)

    # Extract last and first name, assuming format "LAST, First"
    parts = name.split(',')
    if len(parts) < 2:
        return ""

    last_name = parts[0].strip(' .').lower()
    first_name = parts[1].strip(' .').split()[0].lower().strip(' .')

    return f"{last_name}_{first_name}"


def extract_bookmarks(pdf_path: str) -> List[Tuple[str, int]]:
    """Extract bookmarks from PDF."""
    doc = fitz.open(pdf_path)
    bookmarks = []

    for toc in doc.get_toc():
        level, title, page = toc
        bookmarks.append((title, page))

    return bookmarks


@dg.asset(
    code_version="v1",
    description="Split the source PDF into individual PDFs per woman based on bookmarks"
)
def split_pdfs() -> dg.MaterializeResult:
    """
    Split the main PDF into individual PDFs for each woman.

    Returns metadata about the number of PDFs created.
    """
    pdf_path = here("src/women_in_mathematics/defs/split/input/PioneeringWomenSupplement.pdf")
    output_folder = here("src/women_in_mathematics/defs/split/output/")

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Extract bookmarks
    bookmarks = extract_bookmarks(str(pdf_path))

    if not bookmarks:
        raise ValueError("No bookmarks found in PDF!")

    # Load the PDF using PyPDF2
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        total_pages = len(reader.pages)
        pdfs_created = 0

        for i, (title, start_page) in enumerate(bookmarks):
            if start_page < 6:
                continue

            if len(title) == 1:
                continue

            # Normalize filename
            title = format_name(title)
            if len(title) == 0:
                continue

            # Determine the end page
            end_page = bookmarks[i + 1][1] if i + 1 < len(bookmarks) else total_pages

            # Create a new PDF writer
            writer = PyPDF2.PdfWriter()
            for page_num in range(start_page - 1, end_page - 1):
                writer.add_page(reader.pages[page_num])

            output_filename = os.path.join(output_folder, f"{title}.pdf")
            with open(output_filename, "wb") as output_pdf:
                writer.write(output_pdf)

            pdfs_created += 1

    return dg.MaterializeResult(
        metadata={
            "input_from": "defs/split/input/PioneeringWomenSupplement.pdf",
            "output_to": "defs/split/output/",
            "num_pdfs_created": pdfs_created,
        }
    )