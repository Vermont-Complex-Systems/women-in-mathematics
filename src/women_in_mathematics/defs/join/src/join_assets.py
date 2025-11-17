"""
Dagster assets for joining parsed JSON data into CSV files.
"""
import json
from pathlib import Path

import dagster as dg
import dateparser
import pandas as pd
from pyprojroot.here import here


def year_or_none(date_str):
    """Extract year from date string, return None if parsing fails."""
    try:
        return dateparser.parse(date_str).year
    except (TypeError, AttributeError):
        return None


@dg.asset(
    deps=["parse_biographies"],
    code_version="v1",
    description="Join parsed JSON files into normalized CSV tables"
)
def join_to_csv() -> dg.MaterializeResult:
    """
    Join all parsed JSON files into normalized CSV tables.

    Depends on parse_biographies asset.
    Creates separate CSV files for people, degrees, employment, visits, honors, and parents.
    """
    input_folder = here("src/women_in_mathematics/defs/parse/output/")
    output_folder = here("src/women_in_mathematics/defs/join/output/")

    output_folder.mkdir(parents=True, exist_ok=True)

    # Load all JSON files
    parsed_fn_paths = Path(input_folder).glob("*.json")
    jsons = []
    for path in parsed_fn_paths:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                jsons.append(json.load(f))
        except Exception as e:
            dg.get_dagster_logger().warning(f"Failed to load {path.name}: {e}")

    # Initialize lists for each table
    degrees = []
    employment = []
    visits = []
    honors = []
    parents = []
    people = []

    person_keys = ['full_name', 'birthdate', 'deathdate', 'birthplace']

    # Process each person's data
    for js in jsons:
        birthyear = year_or_none(js.get('birthdate'))
        deathyear = year_or_none(js.get('deathdate'))

        personal = {k: js.get(k) for k in person_keys}
        personal['birthyear'] = birthyear
        personal['deathyear'] = deathyear
        people.append(personal)

        # Join biographical data with personal info
        degrees.extend([{**deg, **personal} for deg in js.get('degrees', [])])
        employment.extend([{**emp, **personal} for emp in js.get('employment', [])])
        visits.extend([{**vis, **personal} for vis in js.get('visits', [])])
        honors.extend([{**hon, **personal} for hon in js.get('honors', [])])

        # Handle parents separately
        for p in js.get('parents', []):
            parent_record = {
                'parent_name': p.get('name'),
                'parent_birthdate': p.get('birthdate'),
                'parent_deathdate': p.get('deathdate'),
                'parent_birthyear': year_or_none(p.get('birthdate') or ''),
                'parent_deathyear': year_or_none(p.get('deathdate') or ''),
                'parent_profession': p.get('profession'),
                **personal
            }
            parents.append(parent_record)

    # Create DataFrames and save to CSV
    df_people = pd.DataFrame(people)
    df_degrees = pd.DataFrame(degrees)
    df_employment = pd.DataFrame(employment)
    df_visits = pd.DataFrame(visits)
    df_honors = pd.DataFrame(honors)
    df_parents = pd.DataFrame(parents)

    df_people.to_csv(output_folder / "personal.csv", index=False)
    df_degrees.to_csv(output_folder / "degrees.csv", index=False)
    df_employment.to_csv(output_folder / "employment.csv", index=False)
    df_visits.to_csv(output_folder / "visits.csv", index=False)
    df_honors.to_csv(output_folder / "honors.csv", index=False)
    df_parents.to_csv(output_folder / "parents.csv", index=False)

    return dg.MaterializeResult(
        metadata={
            "input_from": "defs/parse/output/",
            "output_to": "defs/join/output/",
            "num_people": len(people),
            "num_degrees": len(degrees),
            "num_employment_records": len(employment),
            "num_visits": len(visits),
            "num_honors": len(honors),
            "num_parents": len(parents),
        }
    )