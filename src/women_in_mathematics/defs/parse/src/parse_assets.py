"""
Dagster assets for parsing biographical text using OpenAI.
"""
import json
from pathlib import Path

import dagster as dg
import json_repair
from pyprojroot.here import here

from women_in_mathematics.defs.resources import OpenAIResource


PARSE_PROMPT_TEMPLATE = """Given the following biographical summary, return the following data in JSON format:
- full_name
- birthdate
- deathdate
- birthplace
- parents: [List of Parents]
- employment: [List of Employment]
- degrees: [List of Degrees]
- visits: [List of Visits]
- honors: [List of Honors]

where each Parent is a JSON object with keys
- name
- birthdate
- deathdate
- profession

Employment is a JSON object with keys
- employer
- job_title
- job_year_begin
- job_year_end
- reason_end

Degree is a JSON object with keys
- degree_institution_name
- degree_type (eg: BA, MA, PhD)
- degree_year
- degree_advisor

Visit is a JSON object with keys:
- visit_location
- visit_reason
- visit_year

Honors is a JSON object with keys:
- honor_name
- honor_year

---
{bio_text}"""


@dg.asset(
    deps=["extract_text"],
    code_version="v1",
    description="Parse biographical text using GPT-4o to extract structured data"
)
def parse_biographies(openai_resource: OpenAIResource) -> dg.MaterializeResult:
    """
    Parse biographical text files using OpenAI GPT-4o.

    Depends on extract_text asset.
    Returns metadata about the number of JSON files created.
    """
    input_folder = here("src/women_in_mathematics/defs/extract/output/")
    output_folder = here("src/women_in_mathematics/defs/parse/output/")

    output_folder.mkdir(parents=True, exist_ok=True)

    txt_files = list(Path(input_folder).glob("*.txt"))

    # Check if all outputs already exist
    files_to_process = []
    existing_files = 0
    for txt_file in txt_files:
        output_filename = txt_file.stem.replace('.pdf', '') + '.json'
        output_path = output_folder / output_filename
        if output_path.exists():
            existing_files += 1
        else:
            files_to_process.append(txt_file)

    # If all files exist, skip GPT-4 calls
    if not files_to_process:
        dg.get_dagster_logger().info(f"All {existing_files} JSON files already exist. Skipping GPT-4 calls.")
        return dg.MaterializeResult(
            metadata={
                "input_from": "defs/extract/output/",
                "output_to": "defs/parse/output/",
                "num_jsons_created": 0,
                "num_texts_processed": len(txt_files),
                "num_errors": 0,
                "skipped": True,
                "existing_files": existing_files,
            }
        )

    # Process only missing files
    client = openai_resource.get_client()
    jsons_created = 0
    errors = []

    for txt_file in files_to_process:
        output_filename = txt_file.stem.replace('.pdf', '') + '.json'
        output_path = output_folder / output_filename

        with open(txt_file, 'r', encoding='utf-8') as f:
            bio_text = f.read()

        prompt = PARSE_PROMPT_TEMPLATE.format(bio_text=bio_text)

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are an AI that extracts structured data from text, returning a JSON object and no other text."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )

            json_response = response.choices[0].message.content
            parsed_response = json_repair.loads(json_response)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(parsed_response, f, indent=4)

            jsons_created += 1

        except Exception as e:
            error_msg = f"Error processing {txt_file.name}: {e}"
            dg.get_dagster_logger().error(error_msg)
            errors.append(error_msg)

            # Write raw response if available
            try:
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(json_response if 'json_response' in locals() else str(e))
            except:
                pass

    return dg.MaterializeResult(
        metadata={
            "input_from": "defs/extract/output/",
            "output_to": "defs/parse/output/",
            "num_jsons_created": jsons_created,
            "num_texts_processed": len(txt_files),
            "num_errors": len(errors),
        }
    )